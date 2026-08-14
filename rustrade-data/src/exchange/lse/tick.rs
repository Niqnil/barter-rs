//! The London Strategic Edge WebSocket tick frame, and the three timestamp spellings it arrives in.

use super::channel::LseChannel;
use crate::{Identifier, exchange::ExchangeSub};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rustrade_integration::subscription::SubscriptionId;
use serde::{
    Deserialize, Deserializer,
    de::{self, Unexpected, Visitor},
};
use std::fmt;

/// A tick published on the London Strategic Edge WebSocket.
///
/// One frame shape serves every dataset — FX, crypto, equities, ETFs, indices, commodities,
/// futures, volatility, currency indices and interest rates all publish the same seven keys. This
/// is the opposite of the provider's bulk-export path, where the column set varies per dataset.
///
/// ```json
/// {"type":"tick","symbol":"BTC/USD","ts":"2026-01-02T09:37:24.760146+00:00",
///  "price":42000.5,"bid":42000.5,"ask":42001.0,"volume":0.00155}
/// ```
///
/// # ⚠️ The tick is a QUOTE, not a print
/// `price` equals `bid` exactly — measured on 3,966 of 3,966 sampled ticks spanning every dataset
/// family, plus every sample taken on the provider's other price endpoint. Anything decoded from
/// this frame as a trade is therefore a bid-side quote wearing a trade's shape, and is not evidence
/// that a transaction occurred at that price or at all. See the trade transformer for what ships
/// anyway and why.
///
/// # ⚠️ `volume` is real on some datasets and fabricated on others
/// Crypto and equity ticks carry a genuine per-tick size: summing it over three whole minutes of
/// one crypto symbol reproduced the provider's own one-minute candle volume to the last decimal,
/// ratio `1.000`. FX and commodity ticks carry a **hard-coded `1.0`** on every tick of every
/// symbol sampled — a fabricated size, not a measured one, and it will aggregate into a
/// legitimate-looking total at any resolution. Note this differs from the provider's REST vault,
/// which *omits* FX volume entirely; the WebSocket invents a value instead.
///
/// # ⚠️ Identical consecutive ticks are REAL and must not be filtered
/// Ticks routinely repeat a prior `(ts, price, bid, ask, volume)` signature — barely a third of a
/// sampled run was unique, and one signature recurred 49 times. They are nonetheless distinct
/// prints: de-duplicating them destroyed 3–10% of the volume that reconciles exactly against the
/// provider's candles. This is an aggregated cross-venue tape on which identical-size fills at the
/// same millisecond are ordinary. Do not add a de-duplication filter.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
pub struct LseTick {
    /// Subscription identifier built from `symbol` during deserialisation.
    ///
    /// Constructing it here keeps the per-tick `format!` out of the transformer's hot path, and
    /// mirrors how every other WebSocket integration in this crate resolves a frame to its
    /// instrument.
    #[serde(rename = "symbol", deserialize_with = "de_tick_subscription_id")]
    pub subscription_id: SubscriptionId,

    /// The instant the provider stamped the tick with.
    ///
    /// # The three spellings, all measured on one connection
    /// 1. **RFC 3339** — `2026-01-02T09:37:24.760146+00:00`.
    /// 2. **The same with a space** separating date and time —
    ///    `2026-01-02 09:37:21.690159+00:00`. [`DateTime::parse_from_rfc3339`] rejects this
    ///    outright, and it is what five of thirteen sampled dataset families send. A decoder
    ///    handling only spelling 1 silently loses them.
    /// 3. **Epoch seconds as a JSON number** — `1786091921.387`. Replayed ticks use this while
    ///    live ticks on the *same connection* use a string, so the field changes JSON type
    ///    mid-stream. A decoder typed as a string fails the moment replay is enabled.
    ///
    /// Sub-second precision also varies per symbol within one spelling: microseconds,
    /// milliseconds, and — on at least one symbol — no fractional component at all.
    ///
    /// # The epoch spelling is quantised to microseconds
    /// An `f64` cannot carry nanosecond resolution at epoch scale: near the present its spacing is
    /// roughly a quarter of a microsecond, so nanoseconds recovered from it are noise. Rounding to
    /// the nearest microsecond discards that noise and recovers the provider's own precision
    /// exactly, which matters for more than tidiness — **it is what makes spellings 1 and 3 of one
    /// instant decode to the same value.** The replay path re-sends ticks the live path already
    /// delivered, in the other spelling, so anything comparing the two for equality (resume
    /// arithmetic above all) depends on them agreeing.
    ///
    /// A string that is neither spelling, including one carrying no UTC offset, is rejected rather
    /// than guessed at — a mis-parsed timestamp is a silently misdated market event.
    #[serde(rename = "ts", deserialize_with = "de_timestamp")]
    pub time_exchange: DateTime<Utc>,

    /// The tick price. Equal to [`bid`](Self::bid) on every sample taken; see the type-level note.
    pub price: Decimal,

    /// The bid.
    pub bid: Decimal,

    /// The ask.
    pub ask: Decimal,

    /// The size traded at this tick — genuine on crypto and equities, fabricated on FX and
    /// commodities. See the type-level note.
    pub volume: Decimal,

    /// Whether the tick was served from the historical replay buffer rather than live.
    ///
    /// The provider sets this only on replayed ticks; live ticks carry no `replay` key at all, so
    /// absence means live and the default is load-bearing rather than cosmetic.
    #[serde(default)]
    pub replay: bool,
}

/// A frame received on the London Strategic Edge WebSocket data stream.
///
/// The stream carries control frames (subscription confirmations, replay boundaries, errors)
/// interleaved with ticks, on the same connection and after the handshake has completed. Anything
/// that is not a tick decodes to [`Other`](Self::Other) so a control frame cannot fail the parse
/// and take the stream down with it; the frames that carry *meaning* to a subscriber are decoded
/// by the subscription types instead, during the handshake where they can be acted on.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LseMessage {
    /// A tick.
    Tick(LseTick),

    /// Any other frame — a control frame, or one this integration does not model.
    #[serde(other)]
    Other,
}

impl Identifier<Option<SubscriptionId>> for LseMessage {
    /// A tick resolves to the subscription its symbol was registered under; anything else resolves
    /// to nothing.
    ///
    /// The `None` is what keeps control frames out of the market stream: the transformer drops an
    /// unidentifiable frame rather than attempting to convert it, so the two decoders never have to
    /// invent an event for a frame that describes none.
    fn id(&self) -> Option<SubscriptionId> {
        match self {
            Self::Tick(tick) => Some(tick.subscription_id.clone()),
            Self::Other => None,
        }
    }
}

/// Deserialise the tick's `symbol` into the [`SubscriptionId`] it was registered under.
fn de_tick_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|symbol| ExchangeSub::from((LseChannel::Tick, symbol)).id())
}

/// Deserialise a WebSocket timestamp from any of the three spellings the provider uses.
///
/// Every timestamp the WebSocket sends shares this problem, not just a tick's `ts` — a replay
/// window's `from` is decoded through it too. The spellings, and why the epoch form is quantised
/// to microseconds, are documented on [`LseTick::time_exchange`], which is where a reader of the
/// public API meets them.
pub(super) fn de_timestamp<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(TimestampVisitor)
}

struct TimestampVisitor;

impl Visitor<'_> for TimestampVisitor {
    type Value = DateTime<Utc>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "an RFC 3339 timestamp, the same with a space separating date and time, or epoch \
             seconds as a number",
        )
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        parse_timestamp(value).ok_or_else(|| E::invalid_value(Unexpected::Str(value), &self))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        time_from_epoch_seconds(value)
            .ok_or_else(|| E::invalid_value(Unexpected::Float(value), &self))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // An epoch that lands on a whole second has no fractional part to print, so JSON carries
        // it as an integer and never reaches `visit_f64`.
        i64::try_from(value)
            .ok()
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
            .ok_or_else(|| E::invalid_value(Unexpected::Unsigned(value), &self))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        DateTime::from_timestamp(value, 0)
            .ok_or_else(|| E::invalid_value(Unexpected::Signed(value), &self))
    }
}

/// Parse spellings 1 and 2 — see [`de_timestamp`].
fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
        return Some(parsed.with_timezone(&Utc));
    }

    // The space-separated spelling is the same instant in a form RFC 3339 does not admit. Swapping
    // the separator and re-parsing reuses the strict parser rather than introducing a second,
    // laxer grammar -- so an offset RFC 3339 would reject is still rejected here, and the two
    // spellings cannot drift apart in what they accept.
    let (date, time) = raw.split_once(' ')?;
    let mut rfc3339 = String::with_capacity(raw.len());
    rfc3339.push_str(date);
    rfc3339.push('T');
    rfc3339.push_str(time);

    DateTime::parse_from_rfc3339(&rfc3339)
        .ok()
        .map(|parsed| parsed.with_timezone(&Utc))
}

/// Parse spelling 3 — see [`de_timestamp`] for why this rounds to microseconds.
fn time_from_epoch_seconds(epoch: f64) -> Option<DateTime<Utc>> {
    /// Microseconds in a second, as the multiplier for the epoch conversion.
    const MICROS_PER_SECOND: f64 = 1_000_000.0;

    if !epoch.is_finite() {
        return None;
    }

    // The whole conversion stays inside `f64`'s exactly-representable integer range: epoch
    // microseconds near the present are ~1.8e15, comfortably under 2^53. Scaling first and
    // rounding once is therefore exact, where splitting into seconds and a fraction would round
    // twice for no gain.
    let micros = (epoch * MICROS_PER_SECOND).round();
    if micros < i64::MIN as f64 || micros > i64::MAX as f64 {
        return None;
    }

    // `micros` is integral (just rounded) and inside `i64`'s range (just checked), so this
    // conversion is exact -- neither fact is visible to the lint, and `f64` carries no fallible
    // conversion to `i64` that would express them.
    #[allow(clippy::cast_possible_truncation)]
    let micros = micros as i64;

    DateTime::from_timestamp_micros(micros)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// A live tick, in the RFC 3339 spelling with a `T` separator.
    const LIVE_T_SEPARATED: &str = r#"{"type":"tick","symbol":"BTC/USD",
        "ts":"2026-01-02T09:37:24.760146+00:00","price":42000.5,"bid":42000.5,
        "ask":42001.0,"volume":0.00155}"#;

    /// A live tick from one of the families that separates date and time with a space.
    const LIVE_SPACE_SEPARATED: &str = r#"{"type":"tick","symbol":"EUR/USD",
        "ts":"2026-01-02 09:37:21.690159+00:00","price":1.1,"bid":1.1,"ask":1.1001,
        "volume":1.0}"#;

    #[test]
    fn a_t_separated_timestamp_decodes() {
        let tick: LseTick = serde_json::from_str(LIVE_T_SEPARATED).unwrap();

        assert_eq!(
            tick.time_exchange,
            "2026-01-02T09:37:24.760146Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
        assert_eq!(tick.price, dec!(42000.5));
        assert_eq!(tick.volume, dec!(0.00155));
    }

    /// The spelling `DateTime::parse_from_rfc3339` rejects. Five of thirteen sampled dataset
    /// families send it, so an RFC-3339-only decoder loses them silently.
    #[test]
    fn a_space_separated_timestamp_decodes_to_the_same_instant_as_the_t_form() {
        let tick: LseTick = serde_json::from_str(LIVE_SPACE_SEPARATED).unwrap();

        assert_eq!(
            tick.time_exchange,
            "2026-01-02T09:37:21.690159Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
    }

    /// At least one symbol publishes no fractional component at all.
    #[test]
    fn a_timestamp_without_a_subsecond_component_decodes() {
        let input = r#"{"type":"tick","symbol":"VIX/USD","ts":"2026-01-02T19:59:46+00:00",
            "price":16.9,"bid":16.9,"ask":16.93,"volume":0.0}"#;
        let tick: LseTick = serde_json::from_str(input).unwrap();

        assert_eq!(
            tick.time_exchange,
            "2026-01-02T19:59:46Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    /// Replayed ticks send the timestamp as a JSON number while live ticks on the same connection
    /// send a string, so the field changes JSON type mid-stream.
    #[test]
    fn a_float_epoch_timestamp_decodes() {
        let input = r#"{"type":"tick","symbol":"BTC/USD","ts":1767346644.592,"name":"Bitcoin",
            "replay":true,"price":42000.5,"bid":42000.5,"ask":42001.0,"volume":0.00155}"#;
        let tick: LseTick = serde_json::from_str(input).unwrap();

        assert_eq!(
            tick.time_exchange,
            DateTime::from_timestamp_micros(1_767_346_644_592_000).unwrap()
        );
        assert!(tick.replay);
    }

    /// The property the resume arithmetic depends on: a replayed tick and the live tick it repeats
    /// describe the same instant in different spellings, and must decode equal. Nanosecond
    /// conversion would fail this — `f64` spacing near the present is ~0.24µs.
    #[test]
    fn the_string_and_float_spellings_of_one_instant_decode_equal() {
        let as_string = parse_timestamp("2026-01-02T09:37:24.760146+00:00").unwrap();
        let as_float = time_from_epoch_seconds(1_767_346_644.760_146).unwrap();

        assert_eq!(as_string, as_float);
    }

    /// A whole-second epoch has no fractional part to print, so JSON carries it as an integer.
    #[test]
    fn an_integer_epoch_timestamp_decodes() {
        let input = r#"{"type":"tick","symbol":"BTC/USD","ts":1767346644,"replay":true,
            "price":42000.5,"bid":42000.5,"ask":42001.0,"volume":0.00155}"#;
        let tick: LseTick = serde_json::from_str(input).unwrap();

        assert_eq!(
            tick.time_exchange,
            DateTime::from_timestamp(1_767_346_644, 0).unwrap()
        );
    }

    /// Live ticks carry no `replay` key at all, so the default decides whether every live tick is
    /// mistaken for a replayed one.
    #[test]
    fn replay_defaults_to_false_when_the_key_is_absent() {
        let tick: LseTick = serde_json::from_str(LIVE_T_SEPARATED).unwrap();
        assert!(!tick.replay);
    }

    #[test]
    fn the_subscription_id_is_built_from_the_symbol() {
        let tick: LseTick = serde_json::from_str(LIVE_T_SEPARATED).unwrap();
        assert_eq!(tick.subscription_id.as_ref(), "tick|BTC/USD");
    }

    /// A timestamp carrying no offset is ambiguous, and guessing at it would silently misdate the
    /// event rather than fail.
    #[test]
    fn a_timestamp_without_an_offset_is_rejected() {
        assert!(parse_timestamp("2026-01-02 09:37:21.690159").is_none());
        assert!(parse_timestamp("2026-01-02T09:37:21.690159").is_none());
    }

    #[test]
    fn a_non_timestamp_string_is_rejected() {
        assert!(parse_timestamp("").is_none());
        assert!(parse_timestamp("not a timestamp").is_none());
    }

    #[test]
    fn a_non_finite_epoch_is_rejected() {
        assert!(time_from_epoch_seconds(f64::NAN).is_none());
        assert!(time_from_epoch_seconds(f64::INFINITY).is_none());
        assert!(time_from_epoch_seconds(f64::MAX).is_none());
    }

    #[test]
    fn a_tick_frame_decodes_as_a_tick() {
        let message: LseMessage = serde_json::from_str(LIVE_T_SEPARATED).unwrap();
        assert!(matches!(message, LseMessage::Tick(_)));
    }

    #[test]
    fn a_tick_identifies_itself_by_its_subscription() {
        let message: LseMessage = serde_json::from_str(LIVE_T_SEPARATED).unwrap();
        assert_eq!(
            message.id(),
            Some(SubscriptionId::from("tick|BTC/USD")),
            "a tick must resolve to the subscription it was registered under"
        );
    }

    /// The transformer drops a frame that identifies no subscription, which is what keeps control
    /// frames from reaching the decoders at all.
    #[test]
    fn a_control_frame_identifies_no_subscription() {
        let message: LseMessage =
            serde_json::from_str(r#"{"type":"replay_complete","symbol":"BTC/USD","rows":41}"#)
                .unwrap();
        assert_eq!(message.id(), None);
    }

    /// Control frames share the data stream with ticks. Failing the parse on one would take the
    /// stream down over a message the transformer has no interest in.
    #[test]
    fn control_frames_decode_as_other_rather_than_failing_the_parse() {
        for input in [
            r#"{"type":"subscribed","symbol":"BTC/USD","count":1,"max":16}"#,
            r#"{"type":"replay_started","symbol":"BTC/USD","from":"2026-01-02T09:37:24+00:00"}"#,
            r#"{"type":"replay_complete","symbol":"BTC/USD","rows":41,"buffered_drained":9}"#,
            r#"{"type":"error","code":"LIMIT_REACHED","message":"Max 16 symbols"}"#,
            r#"{"type":"welcome","message":"hello","symbols_available":1}"#,
        ] {
            let message: LseMessage = serde_json::from_str(input).unwrap();
            assert_eq!(message, LseMessage::Other, "failed on {input}");
        }
    }
}
