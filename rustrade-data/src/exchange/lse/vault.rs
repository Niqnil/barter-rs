//! Authenticated REST transport for the London Strategic Edge vault.
//!
//! The vault (`api.londonstrategicedge.com/vault`) is the provider's data plane. It is a distinct
//! host from their catalog/discovery API, with a different symbol key: **the vault keys candles on
//! the display symbol** (`EUR/USD`, `AAPL`, `ES.F`), not on a dataset slug.
//!
//! # Authentication
//! Every vault endpoint requires an API key, sent as the `x-api-key` header. Supply it explicitly
//! via [`LseVaultClient::new`] or from `LSE_API_KEY` via [`LseVaultClient::from_env`]. Keys are
//! free and require no account.
//!
//! # ⚠️ Unknown query parameters are ignored, not rejected
//! The vault answers `200` to a request carrying a misspelled parameter, having silently applied
//! its default instead. Measured: `resolution=1d` returns **1-minute** bars, byte-identical in
//! shape to a correct response, and `from`/`since`/`after`/`begin`/`start_date`/`start_time`/`to`/
//! `until`/`end_date` are all ignored in favour of full history from page one. Only `symbol`,
//! `timeframe`, `start`, `end` and `limit` are honoured. **Any parameter added here must be
//! verified against a known-answer query** — a wrong name is never an error.

use crate::exchange::http::{MAX_ERROR_BODY_DOWNLOAD_BYTES, read_body_capped};
use crate::exchange::lse::error::{LseError, extract_detail};
use crate::exchange::lse::quota::QuotaStatus;
use reqwest::header::{HeaderMap, HeaderValue};
use std::{env, time::Duration};
use tracing::debug;

/// Base URL of the vault data plane.
const VAULT_BASE_URL: &str = "https://api.londonstrategicedge.com/vault";

/// Header carrying the API key.
const API_KEY_HEADER: &str = "x-api-key";

/// Environment variable read by [`LseVaultClient::from_env`].
const API_KEY_ENV: &str = "LSE_API_KEY";

/// Per-request HTTP timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// `User-Agent` sent on every vault request.
///
/// Set explicitly because the vault sits behind a CDN that rejects some agents outright —
/// measured: `Python-urllib` receives a `403` with `error code: 1010`, never reaching the API.
/// `reqwest` sends no `User-Agent` by default, which leaves that outcome to the CDN's discretion
/// rather than to anything this crate controls. An edge rejection is also invisible to the
/// provider's allowance accounting, so it fails in a way that looks nothing like an API error.
const USER_AGENT: &str = concat!("rustrade-data/", env!("CARGO_PKG_VERSION"));

/// Default delay between pages of a paged fetch.
///
/// Derived from the provider's documented allowance of 200 calls per minute, which is one call per
/// 300ms. This is *proactive courtesy only* — it never inspects a `429`, never retries, and never
/// adapts. Override with [`LseVaultClient::with_pace`].
const DEFAULT_PACE: Duration = Duration::from_millis(300);

/// Authenticated client for the London Strategic Edge vault.
///
/// Holds one configured [`reqwest::Client`] (auth header + timeout) plus the vault base URL.
/// Endpoint families build on it: [`usage`](Self::usage) here, paged candles in
/// [`historical`](super::historical).
///
/// # ⚠️ Licensing
/// Data retrieved through this client is **not redistributable**. See the
/// [module documentation](super) and <https://londonstrategicedge.com/terms>.
#[derive(Clone)]
pub struct LseVaultClient {
    http: reqwest::Client,
    base_url: String,
    pace: Duration,
}

impl std::fmt::Debug for LseVaultClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately omits the `reqwest::Client`, and therefore its auth header, so the API key
        // never leaks through `Debug` into logs or error reports.
        f.debug_struct("LseVaultClient")
            .field("base_url", &self.base_url)
            .field("pace", &self.pace)
            .finish_non_exhaustive()
    }
}

impl LseVaultClient {
    /// Create a client with an explicit API key.
    ///
    /// # Errors
    /// Returns [`LseError::InvalidCredential`] if the key cannot be encoded as an HTTP header
    /// value (e.g. non-ASCII bytes), or [`LseError::Http`] if the HTTP client cannot be built.
    pub fn new(api_key: &str) -> Result<Self, LseError> {
        let mut headers = HeaderMap::new();
        let mut key = HeaderValue::from_str(api_key)
            .map_err(|error| LseError::InvalidCredential(format!("invalid API key: {error}")))?;
        // Marks the value as sensitive so `HeaderMap`'s own `Debug` prints it redacted, closing the
        // leak path that bypasses this type's `Debug`.
        key.set_sensitive(true);
        headers.insert(API_KEY_HEADER, key);

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .user_agent(USER_AGENT)
            .timeout(REQUEST_TIMEOUT)
            .build()?;

        Ok(Self {
            http,
            base_url: VAULT_BASE_URL.to_owned(),
            pace: DEFAULT_PACE,
        })
    }

    /// Create a client from the `LSE_API_KEY` environment variable.
    ///
    /// # Errors
    /// Returns [`LseError::EnvVar`] if the variable is unset or does not hold valid UTF-8, plus
    /// anything [`new`](Self::new) returns. Neither message ever contains the variable's value.
    pub fn from_env() -> Result<Self, LseError> {
        // `VarError`'s own `Display` is NOT safe to interpolate here: its `NotUnicode` arm is
        // "environment variable was not valid unicode: {:?}" and embeds the raw `OsString`, so a
        // key with one stray non-UTF-8 byte (a mis-encoded paste, a wrong-encoding `.env`) would
        // put essentially the whole key into an error string that callers routinely log. That
        // would defeat the redaction this type does everywhere else. Both arms are therefore
        // reported by a fixed message that names the variable and nothing more.
        let api_key = env::var(API_KEY_ENV).map_err(|error| {
            LseError::EnvVar(match error {
                env::VarError::NotPresent => format!("{API_KEY_ENV} is not set"),
                env::VarError::NotUnicode(_) => {
                    format!("{API_KEY_ENV} is set but is not valid UTF-8")
                }
            })
        })?;

        Self::new(&api_key)
    }

    /// Override the vault base URL, for tests against a mock server or a proxy.
    ///
    /// Infallible by design: this client only ever requests URLs it builds itself from this base —
    /// the vault returns bare arrays with no cursor, so there is never a server-supplied URL to
    /// follow and no trusted origin to derive.
    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Inject a pre-built [`reqwest::Client`].
    ///
    /// # ⚠️ The injected client must carry the `x-api-key` header itself
    /// This replaces the authenticated client built by [`new`](Self::new), auth header included.
    /// It is intended for transport configuration (proxy, TLS, a shared connection pool) where the
    /// caller supplies credentials; a client without the header will see every request `401`.
    #[must_use]
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.http = client;
        self
    }

    /// Override the delay applied between pages of a paged fetch.
    ///
    /// Pass [`Duration::ZERO`] to disable pacing entirely, at which point staying within the
    /// provider's [`calls_per_minute`](QuotaStatus::calls_per_minute) allowance becomes the
    /// caller's responsibility. The default is derived from that documented allowance.
    #[must_use]
    pub fn with_pace(mut self, pace: Duration) -> Self {
        self.pace = pace;
        self
    }

    /// The configured inter-page delay.
    pub(crate) fn pace(&self) -> Duration {
        self.pace
    }

    /// The configured vault base URL.
    ///
    /// Endpoint families build their own URLs from this, preserving the invariant that this client
    /// never follows a server-supplied URL.
    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The configured, authenticated HTTP client.
    ///
    /// For endpoint families that need a verb or a response handling this module's
    /// [`get_json`](Self::get_json) does not cover — a `POST` that answers `202`, or a streamed
    /// download with `Range` resume.
    pub(crate) fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Fetch the current allowance position.
    ///
    /// The allowance is **shared between streaming and bulk export**, so a consumer running both
    /// must budget against this single pool. See [`QuotaStatus`] for what is and is not reported —
    /// notably, no window reset time is available.
    ///
    /// # Errors
    /// See [`LseError`].
    pub async fn usage(&self) -> Result<QuotaStatus, LseError> {
        self.get_json("usage", &[]).await
    }

    /// Issue an authenticated `GET` against a vault path and deserialise the JSON body.
    ///
    /// # Errors
    /// Maps a `429` to [`LseError::RateLimited`] (carrying `Retry-After` when present), any other
    /// non-success status to [`LseError::Api`] with the provider's diagnostic unwrapped from its
    /// envelope, and a body that will not decode to [`LseError::Deserialize`].
    pub(crate) async fn get_json<T>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, LseError>
    where
        T: serde::de::DeserializeOwned,
    {
        let url = format!("{}/{path}", self.base_url);
        let response = self.http.get(&url).query(query).send().await?;
        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            // Surfaced and terminal: pacing policy belongs to the caller, so this never sleeps and
            // retries on their behalf.
            return Err(LseError::RateLimited {
                retry_after: parse_retry_after(response.headers()),
            });
        }

        if !status.is_success() {
            let body = read_body_capped(response, MAX_ERROR_BODY_DOWNLOAD_BYTES).await?;
            return Err(LseError::Api {
                status: status.as_u16(),
                message: extract_detail(&body),
            });
        }

        let body = response.text().await?;
        debug!(len = body.len(), path, "vault response received");

        serde_json::from_str(&body).map_err(|error| LseError::Deserialize {
            message: format!("{path}: {error}"),
        })
    }
}

/// Parse the `Retry-After` header as a delay.
///
/// Only the delta-seconds form is read. The HTTP-date form is not parsed rather than being
/// guessed at: a misread date would produce a wildly wrong delay, and `None` (caller decides)
/// is safer than a confident wrong number.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn debug_does_not_leak_the_api_key() {
        let client = LseVaultClient::new("super-secret-key").unwrap();

        assert!(!format!("{client:?}").contains("super-secret-key"));
    }

    #[test]
    fn a_key_carrying_a_control_character_is_a_typed_error_not_a_panic() {
        // The realistic case is a key copied with a trailing newline. It is rejected rather than
        // trimmed: silently mutating a credential would hide the malformed value from the user,
        // and the typed error names the problem.
        // Note a horizontal tab is *not* here: HTTP header values permit it, so it reaches the
        // provider and comes back as a plain `401` instead.
        for key in [
            "key-with-\nnewline",
            "key-with-\rcarriage",
            "key\u{0}with-nul",
        ] {
            assert!(
                matches!(
                    LseVaultClient::new(key).unwrap_err(),
                    LseError::InvalidCredential(_)
                ),
                "{key:?} should be rejected"
            );
        }
    }

    #[test]
    fn from_env_reports_the_variable_name_when_unset() {
        temp_env::with_var_unset(API_KEY_ENV, || {
            let error = LseVaultClient::from_env().unwrap_err();

            assert!(matches!(error, LseError::EnvVar(_)));
            assert!(error.to_string().contains(API_KEY_ENV));
        });
    }

    /// `VarError::NotUnicode`'s own `Display` embeds the raw `OsString`, so interpolating it would
    /// put a mis-encoded key straight into an error string that callers routinely log. Unix-only:
    /// the invalid value has to be built from raw bytes.
    #[cfg(unix)]
    #[test]
    fn from_env_never_reports_a_non_unicode_key() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};

        // A plausible mis-encoded paste: a real-looking key carrying one stray non-UTF-8 byte.
        let mut raw = b"lse-live-super-secret-".to_vec();
        raw.push(0xff);
        raw.extend_from_slice(b"-tail");

        temp_env::with_var(API_KEY_ENV, Some(OsString::from_vec(raw)), || {
            let error = LseVaultClient::from_env().unwrap_err();
            let message = error.to_string();

            assert!(matches!(error, LseError::EnvVar(_)));
            assert!(message.contains(API_KEY_ENV));
            assert!(
                !message.contains("super-secret"),
                "the key must never reach the error message: {message}"
            );
        });
    }

    #[test]
    fn from_env_builds_a_client_when_set() {
        temp_env::with_var(API_KEY_ENV, Some("test-key"), || {
            assert!(LseVaultClient::from_env().is_ok());
        });
    }

    #[test]
    fn retry_after_reads_delta_seconds() {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, HeaderValue::from_static("42"));

        assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(42)));
    }

    #[test]
    fn retry_after_declines_to_guess_at_an_http_date() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            HeaderValue::from_static("Wed, 21 Oct 2026 07:28:00 GMT"),
        );

        assert_eq!(parse_retry_after(&headers), None);
    }

    #[test]
    fn retry_after_is_none_when_absent() {
        assert_eq!(parse_retry_after(&HeaderMap::new()), None);
    }
}
