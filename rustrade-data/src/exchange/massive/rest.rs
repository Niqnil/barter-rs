//! REST client for Massive historical and intraday data.
//!
//! Provides access to aggregates (OHLCV), trades, and quotes across all asset classes.

use super::error::MassiveError;
use super::pagination::PaginationGuard;
use super::transformer::{
    AggregatesResponse, QuotesResponse, TradesResponse, parse_aggregates_response,
    parse_quotes_response, parse_trades_response, timespan_to_step,
};
use crate::subscription::{
    book::OrderBookL1,
    candle::{Candle, open_time_from_close},
    trade::PublicTrade,
};
use async_stream::try_stream;
use futures::Stream;
use reqwest::{Client, StatusCode, header};
use std::env;
use std::time::Duration;
use tracing::debug;
use url::Url;

const BASE_URL: &str = "https://api.massive.com";
const ENV_API_KEY: &str = "MASSIVE_API_KEY";

/// Truncate response body for error messages (max 512 chars, UTF-8 safe).
fn truncate_body(body: &str) -> String {
    let boundary = body.floor_char_boundary(512);
    body[..boundary].to_owned()
}

/// REST client for Massive historical and intraday market data.
///
/// # Example
///
/// ```ignore
/// use rustrade_data::exchange::massive::MassiveRestClient;
/// use chrono::{Utc, Duration};
///
/// let client = MassiveRestClient::from_env()?;
/// let to = Utc::now();
/// let from = to - Duration::days(1);
///
/// let mut stream = client.fetch_aggregates("X:BTCUSD", 1, "minute", from, to);
/// while let Some(candle) = stream.next().await {
///     println!("{:?}", candle?);
/// }
/// ```
#[derive(Clone)]
pub struct MassiveRestClient {
    client: Client,
    #[allow(dead_code)] // Retained for WebSocket auth; HTTP auth is in client headers
    api_key: String,
    base_url: String,
}

impl std::fmt::Debug for MassiveRestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MassiveRestClient")
            .field("base_url", &self.base_url)
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

impl MassiveRestClient {
    /// Create a new client with explicit API key.
    ///
    /// # Arguments
    ///
    /// * `api_key` - Massive API key from <https://massive.com/dashboard/api-keys>
    ///
    /// # Redirects
    ///
    /// The client does **not** follow HTTP redirects. The API key is sent as an
    /// `Authorization: Bearer` header on every request, so auto-following a
    /// server-issued 3xx could carry it off the trusted origin. Massive uses
    /// explicit `next_url` cursor pagination and is not expected to redirect, so
    /// an unexpected 3xx surfaces as [`MassiveError::Api`] rather than being
    /// followed. A base URL set via [`Self::with_base_url`] must therefore serve
    /// responses directly, without redirect indirection.
    pub fn new(api_key: impl Into<String>) -> Result<Self, MassiveError> {
        let api_key = api_key.into();
        let mut headers = header::HeaderMap::new();
        let auth_value =
            header::HeaderValue::from_str(&format!("Bearer {}", api_key)).map_err(|e| {
                MassiveError::Auth {
                    message: format!("Invalid API key format: {}", e),
                }
            })?;
        headers.insert(header::AUTHORIZATION, auth_value);

        let client = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(30))
            // Transport-layer companion to the `validate_next_url` origin guard. The API key rides in
            // an `Authorization: Bearer` default header on every request, so a server-issued 3xx must
            // not be allowed to bounce an origin-validated request to another host: `Policy::none()`
            // stops reqwest auto-following any redirect, so an unexpected 3xx is returned unfollowed
            // and surfaces as a `MassiveError::Api` instead of silently carrying the token off-origin.
            // This makes the "token never leaves the trusted origin" guarantee structural rather than
            // relying on reqwest's internal cross-origin header stripping. (`https_only` is deliberately
            // NOT set: it would reject the `http://` base URL that `with_base_url` accepts for local
            // testing, while adding nothing against the leak — the origin check already rejects any
            // `http://` `next_url` under an `https` base, since scheme is part of the compared origin.)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        Ok(Self {
            client,
            api_key,
            base_url: BASE_URL.to_string(),
        })
    }

    /// Create a new client from `MASSIVE_API_KEY` environment variable.
    pub fn from_env() -> Result<Self, MassiveError> {
        let api_key =
            env::var(ENV_API_KEY).map_err(|_| MassiveError::EnvVar { var: ENV_API_KEY })?;
        Self::new(api_key)
    }

    /// Override the base URL (useful for testing or legacy polygon.io endpoint).
    ///
    /// The base URL defines the trusted origin: a paginated `next_url` is followed
    /// only when it shares this scheme, host, and port, and redirects are never
    /// followed (see the "Redirects" note on [`Self::new`]).
    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Get the base URL.
    pub(super) fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Validate ticker doesn't contain URL-breaking characters.
    pub(super) fn validate_ticker(ticker: &str) -> Result<(), MassiveError> {
        if ticker.is_empty() {
            return Err(MassiveError::InvalidInput {
                message: "ticker must not be empty".into(),
            });
        }
        if ticker.contains(['/', '?', '#', ' ', '%']) {
            return Err(MassiveError::InvalidInput {
                message: "ticker contains invalid URL characters".into(),
            });
        }
        Ok(())
    }

    /// Validate that `url` shares the client's configured origin before a request
    /// is issued, so the API token is never sent to an untrusted host.
    ///
    /// The client attaches `Authorization: Bearer <key>` as a reqwest default
    /// header, sent with **every** request this client issues regardless of
    /// destination host, so a URL naming a different origin would leak the token.
    /// A `starts_with(base_url)` prefix check is insufficient — a look-alike host
    /// such as `https://api.massive.com.evil.example` (or the separator-less
    /// `https://api.massive.comevil.example`) shares the prefix yet is a
    /// different origin. Both URLs are parsed and their
    /// [origins](url::Url::origin) (scheme + host + port) compared; a `url` that
    /// fails to parse or whose origin differs is rejected (fail-closed) with
    /// [`MassiveError::UntrustedNextUrl`].
    ///
    /// A `base_url` that fails to parse is a client-side misconfiguration
    /// (reachable only via [`Self::with_base_url`], since [`BASE_URL`] is a valid
    /// constant) and surfaces as [`MassiveError::InvalidInput`], not as a
    /// security event.
    ///
    /// On success returns the parsed [`Url`] so the caller can issue the request
    /// without re-parsing the string.
    fn validate_next_url(next_url: &str, base_url: &str) -> Result<Url, MassiveError> {
        let base_origin = Url::parse(base_url)
            .map_err(|e| MassiveError::InvalidInput {
                message: format!("base_url is not a valid URL ({e}): {base_url}"),
            })?
            .origin();

        let untrusted = || MassiveError::UntrustedNextUrl {
            next_url: next_url.to_owned(),
            expected_origin: base_origin.ascii_serialization(),
        };

        let parsed = Url::parse(next_url).map_err(|_| untrusted())?;
        if parsed.origin() != base_origin {
            return Err(untrusted());
        }
        Ok(parsed)
    }

    /// Fetch a page body from the given URL with standard error handling.
    ///
    /// Every URL is [origin-validated](Self::validate_next_url) against the
    /// client's base URL before the request is sent — this is the single
    /// chokepoint that issues an authenticated request, so validating here (not
    /// at each pagination call site) makes the token-leak guard structural: a new
    /// paginated fetch cannot bypass it by forgetting to call the validator.
    pub(super) async fn fetch_page_body(&self, url: &str) -> Result<String, MassiveError> {
        let validated_url = Self::validate_next_url(url, &self.base_url)?;
        let response = self.client.get(validated_url).send().await?;
        let status = response.status();

        // Extract retry-after before consuming response
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs);

        // Check rate limit before consuming body (avoids wasted I/O on 429)
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(MassiveError::RateLimited { retry_after });
        }

        let body = response.text().await?;

        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(MassiveError::Auth {
                message: truncate_body(&body),
            });
        }

        if !status.is_success() {
            return Err(MassiveError::Api {
                status: status.as_u16(),
                message: truncate_body(&body),
            });
        }

        Ok(body)
    }

    /// Fetch a single page of aggregates from the given URL.
    async fn fetch_aggregates_page(&self, url: &str) -> Result<AggregatesResponse, MassiveError> {
        let body = self.fetch_page_body(url).await?;
        parse_aggregates_response(&body)
    }

    /// Fetch aggregated OHLCV bars for a symbol.
    ///
    /// Returns a stream that handles pagination automatically. Does not collect
    /// results into memory — processes each page as it arrives.
    ///
    /// Each [`Candle`]'s `close_time` is the exclusive end-of-period boundary
    /// `bar_open + interval` (see [`Candle::close_time`](crate::subscription::candle::Candle)).
    /// Fixed units (`second`…`week`) are exact in UTC; **calendar units
    /// (`month`/`quarter`/`year`) use leap-year-correct calendar arithmetic** —
    /// e.g. a January monthly bar closes at `Feb 1 00:00 UTC`, aligning with
    /// Binance `1M` / IBKR monthly boundaries (previously an approximate
    /// `+30/91/365 days`).
    ///
    /// # Range contract
    ///
    /// Yields exactly the candles whose `close_time ∈ [from, to]` (both inclusive),
    /// matched on `close_time` — the field consumers receive. Massive's endpoint
    /// natively filters by the bar's open-time, so this method widens the request
    /// by one interval and trims by `close_time`, consistent with the library's
    /// other historical fetches.
    ///
    /// # Arguments
    ///
    /// * `ticker` - Symbol with asset class prefix (e.g., `X:BTCUSD`, `C:EURUSD`, `AAPL`)
    /// * `multiplier` - Size of the timespan multiplier (e.g., 1, 5, 15)
    /// * `timespan` - Size unit: `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`
    /// * `from` - Start timestamp
    /// * `to` - End timestamp
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stream = client.fetch_aggregates("X:BTCUSD", 1, "minute", from, to);
    /// ```
    pub fn fetch_aggregates<'a>(
        &'a self,
        ticker: &'a str,
        multiplier: u32,
        timespan: &'a str,
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> impl Stream<Item = Result<Candle, MassiveError>> + 'a {
        try_stream! {
            Self::validate_ticker(ticker)?;

            // Map the interval to a step once; the boundary is still computed
            // per-bar (calendar months are variable-length, so a single Duration
            // for the whole stream would be wrong for month/quarter/year).
            let bar_step = timespan_to_step(multiplier, timespan);

            // Range contract: yield candles whose `close_time ∈ [from, to]`. The
            // Massive (Polygon) endpoint filters by the bar's open-time, so widen
            // the lower bound by one interval to capture the candle whose
            // `close_time == from` (open == from − interval), then trim by
            // `close_time` below — consistent with the library's other fetches.
            // `None` (underflow near DateTime::MIN_UTC) is not an error: the boundary
            // candle would have an unrepresentable open and so cannot exist, making
            // the un-widened bound already correct. See `open_time_from_close`.
            let request_from = open_time_from_close(from, bar_step).unwrap_or(from);
            let from_ms = request_from.timestamp_millis();
            let to_ms = to.timestamp_millis();

            let initial_url = format!(
                "{}/v2/aggs/ticker/{}/range/{}/{}/{}/{}?adjusted=true&sort=asc&limit=50000",
                self.base_url, ticker, multiplier, timespan, from_ms, to_ms
            );

            let mut next_url: Option<String> = Some(initial_url);
            let mut guard = PaginationGuard::default();

            while let Some(url) = next_url.take() {
                guard.observe(&url)?;
                debug!(url = %url, "Fetching aggregates page");

                let parsed = self.fetch_aggregates_page(&url).await?;

                debug!(
                    results_count = parsed.results_count,
                    has_next = parsed.next_url.is_some(),
                    "Parsed aggregates response"
                );

                if let Some(results) = parsed.results {
                    for bar in results {
                        let candle = bar.into_candle_with_step(bar_step)?;
                        if candle.close_time >= from && candle.close_time <= to {
                            yield candle;
                        }
                    }
                }

                next_url = parsed.next_url;
            }
        }
    }

    /// Fetch tick-level trades for a symbol.
    ///
    /// Returns a stream that handles pagination automatically.
    ///
    /// # Arguments
    ///
    /// * `ticker` - Symbol with asset class prefix (e.g., `X:BTCUSD`, `AAPL`)
    /// * `from` - Start timestamp
    /// * `to` - End timestamp
    pub fn fetch_trades<'a>(
        &'a self,
        ticker: &'a str,
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> impl Stream<Item = Result<PublicTrade, MassiveError>> + 'a {
        try_stream! {
            Self::validate_ticker(ticker)?;

            let from_ns = from.timestamp_nanos_opt().ok_or_else(|| MassiveError::InvalidInput {
                message: "from timestamp out of nanosecond range (~1678-2262)".into(),
            })?;
            let to_ns = to.timestamp_nanos_opt().ok_or_else(|| MassiveError::InvalidInput {
                message: "to timestamp out of nanosecond range (~1678-2262)".into(),
            })?;

            let initial_url = format!(
                "{}/v3/trades/{}?timestamp.gte={}&timestamp.lte={}&limit=50000&sort=timestamp&order=asc",
                self.base_url, ticker, from_ns, to_ns
            );

            let mut next_url: Option<String> = Some(initial_url);
            let mut guard = PaginationGuard::default();

            while let Some(url) = next_url.take() {
                guard.observe(&url)?;
                debug!(url = %url, "Fetching trades page");

                let parsed = self.fetch_trades_page(&url).await?;

                debug!(
                    results_count = parsed.results_count,
                    has_next = parsed.next_url.is_some(),
                    "Parsed trades response"
                );

                if let Some(results) = parsed.results {
                    for trade in results {
                        yield trade.into_public_trade();
                    }
                }

                next_url = parsed.next_url;
            }
        }
    }

    /// Fetch a single page of trades from the given URL.
    async fn fetch_trades_page(&self, url: &str) -> Result<TradesResponse, MassiveError> {
        let body = self.fetch_page_body(url).await?;
        parse_trades_response(&body)
    }

    /// Fetch quotes (BBO/NBBO) for a symbol.
    ///
    /// Returns a stream that handles pagination automatically.
    ///
    /// # Arguments
    ///
    /// * `ticker` - Symbol with asset class prefix (e.g., `C:EURUSD`, `AAPL`)
    /// * `from` - Start timestamp
    /// * `to` - End timestamp
    pub fn fetch_quotes<'a>(
        &'a self,
        ticker: &'a str,
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> impl Stream<Item = Result<OrderBookL1, MassiveError>> + 'a {
        try_stream! {
            Self::validate_ticker(ticker)?;

            let from_ns = from.timestamp_nanos_opt().ok_or_else(|| MassiveError::InvalidInput {
                message: "from timestamp out of nanosecond range (~1678-2262)".into(),
            })?;
            let to_ns = to.timestamp_nanos_opt().ok_or_else(|| MassiveError::InvalidInput {
                message: "to timestamp out of nanosecond range (~1678-2262)".into(),
            })?;

            let initial_url = format!(
                "{}/v3/quotes/{}?timestamp.gte={}&timestamp.lte={}&limit=50000&sort=timestamp&order=asc",
                self.base_url, ticker, from_ns, to_ns
            );

            let mut next_url: Option<String> = Some(initial_url);
            let mut guard = PaginationGuard::default();

            while let Some(url) = next_url.take() {
                guard.observe(&url)?;
                debug!(url = %url, "Fetching quotes page");

                let parsed = self.fetch_quotes_page(&url).await?;

                debug!(
                    results_count = parsed.results_count,
                    has_next = parsed.next_url.is_some(),
                    "Parsed quotes response"
                );

                if let Some(results) = parsed.results {
                    for quote in results {
                        yield quote.into_order_book_l1();
                    }
                }

                next_url = parsed.next_url;
            }
        }
    }

    /// Fetch a single page of quotes from the given URL.
    async fn fetch_quotes_page(&self, url: &str) -> Result<QuotesResponse, MassiveError> {
        let body = self.fetch_page_body(url).await?;
        parse_quotes_response(&body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = MassiveRestClient::new("test_api_key");
        assert!(client.is_ok());
    }

    #[test]
    fn test_from_env_missing() {
        temp_env::with_var_unset(ENV_API_KEY, || {
            let result = MassiveRestClient::from_env();
            assert!(matches!(result, Err(MassiveError::EnvVar { .. })));
        });
    }

    // --- validate_next_url (#182: token-leak guard) ---------------------------

    const BASE: &str = "https://api.massive.com";

    fn assert_untrusted(next_url: &str) {
        match MassiveRestClient::validate_next_url(next_url, BASE) {
            Err(MassiveError::UntrustedNextUrl {
                next_url: got,
                expected_origin,
            }) => {
                assert_eq!(got, next_url);
                assert_eq!(expected_origin, "https://api.massive.com");
            }
            other => panic!("expected UntrustedNextUrl for {next_url:?}, got {other:?}"),
        }
    }

    #[test]
    fn validate_next_url_accepts_same_origin_different_path() {
        // The documented shape: same origin, path + `cursor` query differ.
        assert!(
            MassiveRestClient::validate_next_url(
                "https://api.massive.com/v3/reference/tickers?cursor=YWN0aXZlPXRydWU",
                BASE,
            )
            .is_ok()
        );
    }

    #[test]
    fn validate_next_url_accepts_explicit_default_port() {
        // `:443` is the default for https, so origins are equal.
        assert!(
            MassiveRestClient::validate_next_url("https://api.massive.com:443/v2/aggs", BASE)
                .is_ok()
        );
    }

    #[test]
    fn validate_next_url_rejects_lookalike_subdomain() {
        // The reported #182 vector: shares the base URL as a string prefix
        // (so `starts_with` passed) but is a different host.
        assert_untrusted("https://api.massive.com.attacker.example/v2/aggs?cursor=abc");
    }

    #[test]
    fn validate_next_url_rejects_no_dot_suffix_host() {
        // Proves the fix closes the separator-less bypass, not just subdomains:
        // `api.massive.comevil.example` also `starts_with("https://api.massive.com")`.
        assert_untrusted("https://api.massive.comevil.example/v2/aggs");
    }

    #[test]
    fn validate_next_url_rejects_scheme_downgrade() {
        // https -> http would send the bearer token in cleartext.
        assert_untrusted("http://api.massive.com/v2/aggs");
    }

    #[test]
    fn validate_next_url_rejects_port_mismatch() {
        assert_untrusted("https://api.massive.com:8443/v2/aggs");
    }

    #[test]
    fn validate_next_url_rejects_userinfo_confusion() {
        // Host is `attacker.example`; `api.massive.com` is only userinfo.
        assert_untrusted("https://api.massive.com@attacker.example/v2/aggs");
    }

    #[test]
    fn validate_next_url_rejects_unparseable_next_url() {
        // Fail-closed: an unverifiable URL must never receive the token.
        assert_untrusted("not a url");
    }

    #[test]
    fn validate_next_url_rejects_non_http_scheme() {
        // Non-http(s) schemes yield opaque origins that never compare equal.
        assert_untrusted("file:///etc/passwd");
    }

    #[test]
    fn validate_next_url_unparseable_base_is_invalid_input_not_security() {
        // A broken client-configured base URL is a misconfiguration, not a
        // token-leak attempt — surfaces as InvalidInput.
        assert!(matches!(
            MassiveRestClient::validate_next_url("https://api.massive.com/v2/aggs", "not a url"),
            Err(MassiveError::InvalidInput { .. })
        ));
    }
}
