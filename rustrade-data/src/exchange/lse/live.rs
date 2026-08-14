//! Live WebSocket subscription: authentication, the pre-subscribe guards, and the subscribe flow.

use super::market::LseDataset;
use crate::{
    Identifier,
    exchange::Connector,
    instrument::InstrumentData,
    subscriber::{
        Subscribed, Subscriber,
        mapper::{SubscriptionMapper, WebSocketSubMapper},
        validator::SubscriptionValidator,
    },
    subscription::{Subscription, SubscriptionKind, SubscriptionMeta},
};
use futures::{SinkExt, StreamExt};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    error::SocketError,
    protocol::websocket::{WebSocket, WsMessage, connect},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use smol_str::SmolStr;
use std::{collections::HashMap, env, fmt, time::Duration};
use tracing::{debug, warn};

/// The environment variable [`LseCredentials::from_env`] reads.
const API_KEY_ENV: &str = "LSE_API_KEY";

/// How long to wait for the `authenticated` frame.
///
/// Generous because that frame is not small — it enumerates every symbol the key may subscribe to,
/// which was over eight thousand entries when measured.
const AUTH_TIMEOUT: Duration = Duration::from_secs(15);

/// The API key the WebSocket authenticates with.
///
/// `Debug` is implemented manually to redact the key: subscribers are routinely logged with `?`,
/// and a credential that reaches a log line has effectively been disclosed.
#[derive(Clone)]
pub struct LseCredentials {
    api_key: String,
}

impl fmt::Debug for LseCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LseCredentials")
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

impl LseCredentials {
    /// Construct credentials from an explicit key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
        }
    }

    /// Construct credentials from the `LSE_API_KEY` environment variable.
    ///
    /// # Errors
    /// Returns [`SocketError::Subscribe`] if the variable is unset or does not hold valid UTF-8.
    /// The message names the variable and never its value — `VarError`'s own `Display` embeds the
    /// raw `OsString` on the non-UTF-8 arm, which would put essentially the whole key into a string
    /// callers log.
    pub fn from_env() -> Result<Self, SocketError> {
        let api_key = env::var(API_KEY_ENV).map_err(|error| {
            SocketError::Subscribe(match error {
                env::VarError::NotPresent => format!("{API_KEY_ENV} is not set"),
                env::VarError::NotUnicode(_) => {
                    format!("{API_KEY_ENV} is set but is not valid UTF-8")
                }
            })
        })?;

        Ok(Self::new(api_key))
    }
}

/// The London Strategic Edge WebSocket subscriber.
///
/// # Why this provider needs a subscriber of its own
/// Authentication is a **message**, not a header, and subscriptions are only accepted after the
/// server answers it — so [`Connector::requests`] alone cannot express the flow.
///
/// The handshake also produces something the guards below need: the `authenticated` frame
/// enumerates every symbol the key may subscribe to. That list is the only defence against this
/// surface's quietest failure — see [`Self::subscribe`].
///
/// # Example
/// ```ignore
/// use rustrade_data::exchange::lse::{LseCrypto, live::LseSubscriber};
/// use rustrade_data::streams::Streams;
/// use rustrade_data::subscription::trade::PublicTrades;
/// use rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
///
/// let subscriber = LseSubscriber::from_env()?;
///
/// let streams = Streams::<PublicTrades>::builder()
///     .subscribe(subscriber, [(
///         LseCrypto::default(), "btc", "usd", MarketDataInstrumentKind::Spot, PublicTrades,
///     )])
///     .init()
///     .await?;
/// ```
#[derive(Clone, Debug)]
pub struct LseSubscriber {
    credentials: LseCredentials,
}

impl LseSubscriber {
    /// Construct a subscriber with the provided credentials.
    pub fn new(credentials: LseCredentials) -> Self {
        Self { credentials }
    }

    /// Construct a subscriber from the `LSE_API_KEY` environment variable.
    ///
    /// # Errors
    /// See [`LseCredentials::from_env`].
    pub fn from_env() -> Result<Self, SocketError> {
        Ok(Self::new(LseCredentials::from_env()?))
    }
}

impl Subscriber for LseSubscriber {
    type SubMapper = WebSocketSubMapper;

    /// Connect, authenticate, check the batch, then subscribe.
    ///
    /// # ⚠️ Every guard runs before the first subscribe leaves the client, deliberately
    /// This surface confirms a subscription to a symbol it has never heard of: it answers
    /// `subscribed`, never errors, never ticks, **and permanently consumes one of the connection's
    /// slots**. A slot spent that way cannot be reclaimed without reconnecting, and the standard
    /// subscription validator counts the confirmation as a success. So the requested symbols are
    /// checked against the list the `authenticated` frame supplies *first*, and a batch that fails
    /// costs nothing.
    ///
    /// Two checks run over that list:
    /// - **Membership is enforced.** A symbol the provider does not offer fails the batch.
    /// - **The category is only cross-checked when the provider supplies one.** Roughly half the
    ///   published entries carry no `category` key at all — and some carry neither `category` nor
    ///   `name` — so absence is normal and can never be treated as a mismatch.
    ///
    /// The subscription cap is enforced in the same pass, because an over-cap batch is rejected
    /// *anonymously*: the provider's rejection does not name the symbol it refused, leaving no
    /// partial recovery. Failing the whole batch before sending is the only outcome that does not
    /// leave the caller guessing which subscriptions survived.
    ///
    /// # Errors
    /// Returns [`SocketError::Subscribe`] if the key is rejected, the handshake times out, the
    /// batch exceeds the connection's subscription cap, or any requested symbol is not offered.
    async fn subscribe<Exchange, Instrument, Kind>(
        &self,
        subscriptions: &[Subscription<Exchange, Instrument, Kind>],
    ) -> Result<Subscribed<Instrument::Key>, SocketError>
    where
        Exchange: Connector + Send + Sync,
        Kind: SubscriptionKind + Send + Sync,
        Instrument: InstrumentData,
        Subscription<Exchange, Instrument, Kind>:
            Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
    {
        let exchange = Exchange::ID;
        let url = Exchange::url()?;
        debug!(%exchange, %url, ?subscriptions, "subscribing to London Strategic Edge WebSocket");

        let markets = requested_markets::<Exchange, Instrument, Kind>(subscriptions);

        let mut websocket = connect(url).await?;
        let authenticated = authenticate(&mut websocket, &self.credentials).await?;
        debug!(
            %exchange,
            tier = ?authenticated.tier,
            offered = authenticated.symbols.len(),
            "authenticated to London Strategic Edge WebSocket",
        );

        check_subscription_cap(exchange, &markets, authenticated.max_subscriptions)?;
        check_symbols_are_offered(exchange, &markets, &authenticated)?;

        // Only the instrument map is taken from the standard mapper. The subscribe payloads are
        // built here instead, because this subscriber is where a per-symbol replay window can be
        // attached -- `Connector::requests` is a static function with no access to it. Both routes
        // build their payloads with `subscribe_message`, so they cannot drift apart.
        let SubscriptionMeta {
            instrument_map,
            ws_subscriptions: _,
        } = Self::SubMapper::map::<Exchange, Instrument, Kind>(subscriptions);

        for market in &markets {
            let message = subscribe_message(market);
            debug!(%exchange, payload = ?message, "sending London Strategic Edge subscription");
            websocket
                .send(message)
                .await
                .map_err(|error| SocketError::WebSocket(Box::new(error)))?;
        }

        let (map, buffered_websocket_events) = Exchange::SubValidator::validate::<
            Exchange,
            Instrument::Key,
            Kind,
        >(instrument_map, &mut websocket)
        .await?;

        debug!(%exchange, "London Strategic Edge subscriptions confirmed");
        Ok(Subscribed {
            websocket,
            map,
            buffered_websocket_events,
        })
    }
}

/// Build the subscribe payload for one symbol.
///
/// Shared with [`Connector::requests`] so the two routes into this protocol cannot disagree about
/// its shape.
pub(super) fn subscribe_message(symbol: &str) -> WsMessage {
    WsMessage::text(json!({ "action": "subscribe", "symbol": symbol }).to_string())
}

/// The distinct symbols a batch will subscribe to, in the order they were requested.
///
/// Two subscriptions naming one symbol are one slot and one confirmation, and the instrument map
/// is keyed the same way — so sending a payload per *subscription* would over-count against the
/// cap and leave the validator waiting for a confirmation that never comes.
///
/// The linear scan is deliberate: the batch is bounded by the connection's subscription cap, which
/// was sixteen when measured, and at that size it beats building a hash set.
fn requested_markets<Exchange, Instrument, Kind>(
    subscriptions: &[Subscription<Exchange, Instrument, Kind>],
) -> Vec<SmolStr>
where
    Exchange: Connector,
    Subscription<Exchange, Instrument, Kind>: Identifier<Exchange::Market>,
{
    let mut markets = Vec::<SmolStr>::with_capacity(subscriptions.len());

    for subscription in subscriptions {
        let market = Identifier::<Exchange::Market>::id(subscription);
        let symbol = SmolStr::new(market.as_ref());

        if !markets.contains(&symbol) {
            markets.push(symbol);
        }
    }

    markets
}

/// Reject a batch larger than the connection may hold.
///
/// # Errors
/// Returns [`SocketError::Subscribe`] if `markets` exceeds `max_subscriptions`.
fn check_subscription_cap(
    exchange: ExchangeId,
    markets: &[SmolStr],
    max_subscriptions: Option<u32>,
) -> Result<(), SocketError> {
    let Some(max) = max_subscriptions else {
        // Refusing here would break any tier whose cap this integration cannot read, and the
        // provider still rejects an over-subscription observably. Proceed, but say so.
        warn!(
            %exchange,
            "London Strategic Edge reported no subscription cap; the batch cannot be checked \
             before it is sent",
        );
        return Ok(());
    };

    let cap = usize::try_from(max).unwrap_or(usize::MAX);
    if markets.len() > cap {
        return Err(SocketError::Subscribe(format!(
            "{} symbols requested on {exchange} but this connection holds at most {cap} - the \
             provider's rejection does not name the symbols it refuses, so there is no partial \
             subscription to recover and the batch is rejected before it is sent",
            markets.len(),
        )));
    }

    Ok(())
}

/// Check every requested symbol against the list the handshake published.
///
/// # Errors
/// Returns [`SocketError::Subscribe`] if a symbol is not offered, or if one is offered under a
/// category that contradicts the dataset being subscribed on.
fn check_symbols_are_offered(
    exchange: ExchangeId,
    markets: &[SmolStr],
    authenticated: &LseAuthenticated,
) -> Result<(), SocketError> {
    if authenticated.symbols.is_empty() {
        warn!(
            %exchange,
            "London Strategic Edge published no symbol list; a typo'd symbol will be confirmed and \
             then never tick",
        );
        return Ok(());
    }

    let offered: HashMap<&str, Option<&str>> = authenticated
        .symbols
        .iter()
        .map(|entry| (entry.symbol.as_str(), entry.category.as_deref()))
        .collect();
    let expected = expected_categories(exchange);

    let mut unknown = Vec::new();
    let mut miscategorised = Vec::new();

    for market in markets {
        match offered.get(market.as_str()) {
            None => unknown.push(market.as_str()),
            Some(category) => {
                if let Some(category) = *category
                    && !expected.is_empty()
                    && !expected.contains(&category)
                {
                    miscategorised.push(format!("{market} is {category}"));
                }
            }
        }
    }

    if !unknown.is_empty() {
        return Err(SocketError::Subscribe(format!(
            "London Strategic Edge does not offer {unknown:?} on its WebSocket - a symbol may be \
             reachable through the catalog or the candle store and still be absent here, and \
             subscribing to one the provider does not offer is CONFIRMED rather than rejected, \
             then never ticks, while holding a subscription slot for the life of the connection",
        )));
    }

    if !miscategorised.is_empty() {
        return Err(SocketError::Subscribe(format!(
            "London Strategic Edge categorises {miscategorised:?}, which does not match {exchange} \
             (expected {expected:?}) - the symbol exists, but on a different dataset than the one \
             being subscribed",
        )));
    }

    Ok(())
}

/// The categories the handshake may report for symbols belonging to `exchange`.
///
/// Empty means this integration has no expectation to check against — the provider publishes no
/// category for the datasets behind that identifier, so any category it did report would be
/// unrecognised rather than wrong.
fn expected_categories(exchange: ExchangeId) -> Vec<&'static str> {
    LseDataset::ALL
        .into_iter()
        .filter(|dataset| dataset.exchange_id() == exchange)
        .filter_map(|dataset| dataset.ws_category())
        .collect()
}

/// Authenticate, and return the frame the provider answers with.
async fn authenticate(
    websocket: &mut WebSocket,
    credentials: &LseCredentials,
) -> Result<LseAuthenticated, SocketError> {
    let auth = json!({ "action": "auth", "api_key": credentials.api_key }).to_string();

    websocket
        .send(WsMessage::text(auth))
        .await
        .map_err(|error| SocketError::WebSocket(Box::new(error)))?;

    tokio::time::timeout(AUTH_TIMEOUT, async {
        loop {
            match websocket.next().await {
                Some(Ok(WsMessage::Text(text))) => {
                    if let Some(result) = read_auth_frame(text.as_str()) {
                        return result;
                    }
                }
                Some(Ok(WsMessage::Binary(bytes))) => {
                    if let Ok(text) = std::str::from_utf8(&bytes)
                        && let Some(result) = read_auth_frame(text)
                    {
                        return result;
                    }
                }
                Some(Ok(WsMessage::Close(frame))) => {
                    return Err(SocketError::Subscribe(format!(
                        "WebSocket closed during London Strategic Edge authentication: {frame:?}"
                    )));
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(SocketError::WebSocket(Box::new(error))),
                None => {
                    return Err(SocketError::Subscribe(
                        "WebSocket closed before London Strategic Edge authentication completed"
                            .to_owned(),
                    ));
                }
            }
        }
    })
    .await
    .map_err(|_| {
        SocketError::Subscribe(format!(
            "London Strategic Edge authentication timed out after {AUTH_TIMEOUT:?}"
        ))
    })?
}

/// Classify one frame received while awaiting authentication.
///
/// `None` means "not an answer to the auth request" — the server opens with a `welcome` frame
/// before the key is ever sent, so waiting for a specific frame rather than any frame is what
/// separates being connected from being authenticated.
fn read_auth_frame(text: &str) -> Option<Result<LseAuthenticated, SocketError>> {
    match serde_json::from_str::<LseAuthFrame>(text).ok()? {
        LseAuthFrame::Authenticated(authenticated) => Some(Ok(authenticated)),
        LseAuthFrame::Error { code, message } => Some(Err(SocketError::Subscribe(format!(
            "London Strategic Edge rejected authentication ({}): {}",
            code.as_deref().unwrap_or("unknown"),
            message.as_deref().unwrap_or("no message"),
        )))),
        LseAuthFrame::Other => None,
    }
}

/// A frame that may arrive while awaiting authentication.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum LseAuthFrame {
    Authenticated(LseAuthenticated),
    Error {
        #[serde(default)]
        code: Option<SmolStr>,
        #[serde(default)]
        message: Option<SmolStr>,
    },
    /// The `welcome` frame, and anything else this integration does not model.
    #[serde(other)]
    Other,
}

/// The provider's answer to a successful `auth`.
///
/// Its symbol list is what the pre-subscribe guards check against — see
/// [`LseSubscriber::subscribe`].
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct LseAuthenticated {
    /// The key's plan, as the provider names it (`registered` on a free key).
    #[serde(default)]
    pub tier: Option<SmolStr>,

    /// Subscriptions this connection may hold, shared between symbols and option underlyings.
    ///
    /// `None` if the provider did not report one, in which case the batch cannot be checked before
    /// it is sent.
    #[serde(default)]
    pub max_subscriptions: Option<u32>,

    /// Every symbol the key may subscribe to.
    ///
    /// # ⚠️ This is not the same population as the catalog or the candle store
    /// A symbol reachable on one of the provider's surfaces need not be reachable on all of them —
    /// at least one series with tens of millions of historical ticks is absent from this list
    /// entirely. Its length also drifts week to week, so nothing should assert a count.
    #[serde(default)]
    pub symbols: Vec<LseSymbol>,
}

impl fmt::Debug for LseAuthenticated {
    /// Summarises the symbol list rather than printing it.
    ///
    /// The list ran to over eight thousand entries when measured, and this type is reachable from
    /// tracing fields — deriving `Debug` would put megabytes into a single log line.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LseAuthenticated")
            .field("tier", &self.tier)
            .field("max_subscriptions", &self.max_subscriptions)
            .field("symbols", &format_args!("<{} offered>", self.symbols.len()))
            .finish()
    }
}

/// One entry of the handshake's symbol list.
///
/// # ⚠️ Every field but `symbol` may be absent
/// Entries are not uniform: roughly half carry no `category`, and at least one arrives as
/// `{"symbol": "ES.F"}` — missing keys rather than null values. This is why the category check can
/// only reject a *contradiction* and never an absence.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct LseSymbol {
    /// The display symbol, exactly as a subscribe must spell it.
    pub symbol: SmolStr,

    /// The instrument's descriptive name, where the provider publishes one.
    #[serde(default)]
    pub name: Option<SmolStr>,

    /// The dataset the provider files this symbol under, where it publishes one.
    #[serde(default)]
    pub category: Option<SmolStr>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::exchange::lse::{LseCrypto, LseEquities, LseFx};
    use crate::subscription::trade::PublicTrades;
    use rustrade_instrument::instrument::market_data::MarketDataInstrument;
    use rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind;

    fn authenticated(symbols: &[(&str, Option<&str>)], max: Option<u32>) -> LseAuthenticated {
        LseAuthenticated {
            tier: Some("registered".into()),
            max_subscriptions: max,
            symbols: symbols
                .iter()
                .map(|(symbol, category)| LseSymbol {
                    symbol: SmolStr::new(symbol),
                    name: None,
                    category: category.map(SmolStr::new),
                })
                .collect(),
        }
    }

    fn markets(symbols: &[&str]) -> Vec<SmolStr> {
        symbols.iter().map(SmolStr::new).collect()
    }

    #[test]
    fn credentials_do_not_print_the_key() {
        let printed = format!("{:?}", LseCredentials::new("super-secret-key"));

        assert!(!printed.contains("super-secret-key"), "{printed}");
        assert!(printed.contains("REDACTED"), "{printed}");
    }

    #[test]
    fn a_subscribe_payload_names_the_symbol_and_nothing_else() {
        let WsMessage::Text(payload) = subscribe_message("EUR/USD") else {
            panic!("expected a text payload");
        };
        let payload: serde_json::Value = serde_json::from_str(payload.as_str()).unwrap();

        assert_eq!(payload, json!({"action": "subscribe", "symbol": "EUR/USD"}));
    }

    /// Two subscriptions naming one symbol are one slot and one confirmation. Sending two payloads
    /// would leave the validator waiting for a confirmation that never arrives.
    #[test]
    fn repeated_symbols_produce_one_subscribe_each() {
        let subscription = |base: &str| -> Subscription<LseFx, MarketDataInstrument, PublicTrades> {
            Subscription::from((
                LseFx::default(),
                base,
                "usd",
                MarketDataInstrumentKind::Spot,
                PublicTrades,
            ))
        };
        let subscriptions = [
            subscription("eur"),
            subscription("gbp"),
            subscription("eur"),
        ];

        assert_eq!(
            requested_markets(&subscriptions),
            markets(&["EUR/USD", "GBP/USD"])
        );
    }

    #[test]
    fn a_batch_within_the_cap_is_accepted() {
        let requested = markets(&["EUR/USD", "GBP/USD"]);
        assert!(check_subscription_cap(ExchangeId::LseFx, &requested, Some(2)).is_ok());
    }

    #[test]
    fn a_batch_over_the_cap_is_rejected_before_it_is_sent() {
        let requested = markets(&["EUR/USD", "GBP/USD", "XAU/USD"]);
        let error = check_subscription_cap(ExchangeId::LseFx, &requested, Some(2))
            .unwrap_err()
            .to_string();

        assert!(error.contains("at most 2"), "{error}");
    }

    /// Refusing a batch because the cap could not be read would break any tier whose cap this
    /// integration does not recognise, and the provider still rejects an over-subscription itself.
    #[test]
    fn an_unreported_cap_does_not_reject_the_batch() {
        let requested = markets(&["EUR/USD"]);
        assert!(check_subscription_cap(ExchangeId::LseFx, &requested, None).is_ok());
    }

    #[test]
    fn an_offered_symbol_passes_the_guard() {
        let frame = authenticated(&[("EUR/USD", Some("Forex"))], Some(16));
        let requested = markets(&["EUR/USD"]);

        assert!(check_symbols_are_offered(ExchangeId::LseFx, &requested, &frame).is_ok());
    }

    /// The failure this guard exists for: the provider confirms a symbol it does not offer, never
    /// ticks it, and holds the slot for the life of the connection.
    #[test]
    fn a_symbol_the_provider_does_not_offer_fails_the_batch() {
        let frame = authenticated(&[("EUR/USD", Some("Forex"))], Some(16));
        let requested = markets(&["EUR/USD", "NOPE_XYZ"]);

        let error = check_symbols_are_offered(ExchangeId::LseFx, &requested, &frame)
            .unwrap_err()
            .to_string();
        assert!(error.contains("NOPE_XYZ"), "{error}");
    }

    #[test]
    fn a_contradicting_category_fails_the_batch() {
        let frame = authenticated(&[("AAPL", Some("Stocks"))], Some(16));
        let requested = markets(&["AAPL"]);

        let error = check_symbols_are_offered(ExchangeId::LseFx, &requested, &frame)
            .unwrap_err()
            .to_string();
        assert!(error.contains("AAPL is Stocks"), "{error}");
    }

    /// Roughly half the published entries carry no category, so absence must never be a mismatch.
    #[test]
    fn a_missing_category_is_not_a_contradiction() {
        let frame = authenticated(&[("EUR/USD", None)], Some(16));
        let requested = markets(&["EUR/USD"]);

        assert!(check_symbols_are_offered(ExchangeId::LseFx, &requested, &frame).is_ok());
    }

    /// Datasets the provider publishes no category for must not reject the categories it does
    /// publish — there is nothing to compare against.
    #[test]
    fn a_dataset_with_no_published_category_accepts_whatever_is_reported() {
        assert!(expected_categories(ExchangeId::LseFutures).is_empty());

        let frame = authenticated(&[("ES.F", Some("Anything"))], Some(16));
        let requested = markets(&["ES.F"]);

        assert!(check_symbols_are_offered(ExchangeId::LseFutures, &requested, &frame).is_ok());
    }

    /// Equities and ETFs share one identifier, so both categories must satisfy it.
    #[test]
    fn stocks_and_etfs_both_satisfy_the_equities_identifier() {
        let expected = expected_categories(ExchangeId::LseEquities);
        assert!(expected.contains(&"Stocks"), "{expected:?}");
        assert!(expected.contains(&"ETFs"), "{expected:?}");

        let frame = authenticated(&[("AAPL", Some("Stocks")), ("SPY", Some("ETFs"))], Some(16));
        let requested = markets(&["AAPL", "SPY"]);

        assert!(check_symbols_are_offered(ExchangeId::LseEquities, &requested, &frame).is_ok());
    }

    /// The server opens with a `welcome` frame before the key is ever sent, so treating any frame
    /// as the answer would report success without authenticating.
    #[test]
    fn the_welcome_frame_is_not_mistaken_for_an_auth_response() {
        let welcome = r#"{"type":"welcome","message":"hello","symbols_available":8516}"#;
        assert!(read_auth_frame(welcome).is_none());
    }

    #[test]
    fn an_auth_response_yields_the_symbol_list_and_the_cap() {
        let input = r#"{"type":"authenticated","tier":"registered","key_type":"main",
            "max_subscriptions":16,"symbols":[
                {"symbol":"BTC/USD","name":"Bitcoin","category":"Crypto"},
                {"symbol":"ES.F"}
            ]}"#;
        let frame = read_auth_frame(input).unwrap().unwrap();

        assert_eq!(frame.tier.as_deref(), Some("registered"));
        assert_eq!(frame.max_subscriptions, Some(16));
        assert_eq!(frame.symbols.len(), 2);
        assert_eq!(frame.symbols[1].symbol, "ES.F");
        assert!(frame.symbols[1].name.is_none());
        assert!(frame.symbols[1].category.is_none());
    }

    #[test]
    fn a_rejected_key_is_reported_rather_than_awaited() {
        let input = r#"{"type":"error","code":"INVALID_KEY","message":"invalid api key"}"#;
        let error = read_auth_frame(input).unwrap().unwrap_err().to_string();

        assert!(error.contains("INVALID_KEY"), "{error}");
    }

    /// The handshake frame is reachable from tracing fields, and the real list runs to thousands of
    /// entries.
    #[test]
    fn the_auth_frame_summarises_its_symbol_list_when_printed() {
        let frame = authenticated(&[("BTC/USD", Some("Crypto")), ("ETH/USD", None)], Some(16));
        let printed = format!("{frame:?}");

        assert!(printed.contains("<2 offered>"), "{printed}");
        assert!(!printed.contains("BTC/USD"), "{printed}");
    }

    #[test]
    fn every_dataset_identifier_expects_only_categories_the_provider_publishes() {
        // A category expectation that no dataset publishes would reject every symbol on that
        // identifier, so the two must be derived from one another rather than listed twice.
        for exchange in [
            ExchangeId::LseFx,
            ExchangeId::LseCrypto,
            ExchangeId::LseEquities,
            ExchangeId::LseFutures,
            ExchangeId::LseCfd,
        ] {
            for category in expected_categories(exchange) {
                assert!(
                    LseDataset::ALL
                        .into_iter()
                        .any(|dataset| dataset.ws_category() == Some(category)),
                    "{exchange} expects an unpublished category {category:?}",
                );
            }
        }
    }

    #[test]
    fn unrelated_datasets_do_not_share_category_expectations() {
        assert_eq!(expected_categories(ExchangeId::LseFx), vec!["Forex"]);
        assert_eq!(expected_categories(ExchangeId::LseCrypto), vec!["Crypto"]);
        assert!(!expected_categories(ExchangeId::LseCfd).contains(&"Forex"));

        // Referenced so the connector aliases stay exercised by this module's tests.
        let _ = (LseCrypto::default(), LseEquities::default());
    }
}
