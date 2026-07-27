use crate::exchange::lse::quota::QuotaStatus;
use crate::subscription::candle::CandleInterval;
use chrono::{DateTime, Utc};
use std::time::Duration;
use thiserror::Error;

/// Errors produced by the London Strategic Edge integration.
///
/// `#[non_exhaustive]`: further variants are added alongside the endpoints that raise them.
///
/// Deliberately not `Clone`/`PartialEq` — [`Http`](Self::Http) wraps a [`reqwest::Error`], which is
/// neither, matching every other REST-backed integration in this crate.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum LseError {
    /// More than one distinct dataset series resolves to the requested slug, so no single dataset
    /// can be identified.
    ///
    /// Returned rather than picking one, because the provider's `/dataset/info` endpoint answers
    /// `200` for an ambiguous slug — silently serving whichever series it prefers. Two measured
    /// families produce this: eleven Eurex futures that publish both a bare and a `.F` series
    /// containing *different* data (the bare series is frequently the far larger one and has no
    /// slug of its own), and futures whose stripped symbol collides with an unrelated equity
    /// ticker.
    #[error(
        "ambiguous dataset slug {slug:?} for symbol {symbol:?}: more than one series resolves to \
         it, so it cannot identify a dataset - query the catalog and select explicitly"
    )]
    AmbiguousSlug { symbol: String, slug: String },

    /// The provided string does not name a known London Strategic Edge price dataset.
    #[error("unknown dataset {0:?}")]
    UnknownDataset(String),

    /// A required environment variable is not set (see [`LseVaultClient::from_env`]).
    ///
    /// [`LseVaultClient::from_env`]: super::vault::LseVaultClient::from_env
    #[error("environment variable error: {0}")]
    EnvVar(String),

    /// The supplied API key cannot be encoded as an HTTP header value (e.g. non-ASCII bytes).
    #[error("invalid credential: {0}")]
    InvalidCredential(String),

    /// The request could not be completed at the transport layer.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// The API returned a non-success status.
    ///
    /// `message` is the provider's own diagnostic, unwrapped from its response envelope and
    /// truncated to a bounded length.
    #[error("API error ({status}): {message}")]
    Api { status: u16, message: String },

    /// The request rate was exceeded.
    ///
    /// Terminal — this integration never sleeps and retries on the caller's behalf. A paged fetch
    /// yields this and **ends**; resume by re-requesting from the last `close_time` received.
    ///
    /// The provider permits [`calls_per_minute`](QuotaStatus::calls_per_minute) requests per
    /// minute, reported by [`usage`](super::vault::LseVaultClient::usage).
    ///
    /// # Caveat
    /// The provider's encoding of a *shared-allowance* rejection (as opposed to a plain rate
    /// limit) is not yet characterised, so an exhausted byte or export allowance may currently
    /// arrive here or as [`Api`](Self::Api) rather than under a dedicated variant. Both are
    /// observable and terminal; neither is silently retried.
    #[error("rate limited{}", match .retry_after {
        Some(delay) => format!("; retry after {}s", delay.as_secs()),
        None => String::new(),
    })]
    RateLimited { retry_after: Option<Duration> },

    /// A response body could not be decoded into the expected shape.
    #[error("invalid response: {message}")]
    Deserialize { message: String },

    /// The request is malformed in a way the caller must fix, detected before it is sent.
    #[error("invalid input: {message}")]
    InvalidInput { message: String },

    /// The requested resolution is not one the provider serves.
    ///
    /// [`CandleInterval`] is the venue-agnostic union of every resolution any connector in this
    /// crate serves; this provider serves 14 of them. Rejected before the request is sent rather
    /// than relayed as a `400`, so the caller gets a typed answer.
    #[error("unsupported candle interval {interval}: the provider does not serve this resolution")]
    UnsupportedInterval { interval: CandleInterval },

    /// A candle's period-end boundary is not representable.
    ///
    /// `close_time == open_time + interval` can overflow the representable [`DateTime<Utc>`]
    /// range. Surfaced rather than clamped: a silently substituted boundary would be a
    /// plausible-looking wrong timestamp on a real candle.
    #[error("candle boundary overflow: open time {open} + {interval} is not representable")]
    TimestampOverflow {
        open: DateTime<Utc>,
        interval: CandleInterval,
    },

    /// The shared allowance is exhausted.
    ///
    /// Carries the allowance state at the point of rejection so the caller can decide how to pace.
    /// Terminal — never retried internally.
    ///
    /// # ⚠️ Not raised by the candle path
    /// The bulk-export endpoints are where the provider reports allowance exhaustion, and they are
    /// not part of this integration yet. A candle fetch that runs into a limit currently surfaces
    /// as [`RateLimited`](Self::RateLimited) or [`Api`](Self::Api) instead. Matching on this
    /// variant today is therefore dead code — poll
    /// [`usage`](super::vault::LseVaultClient::usage) to observe the allowance.
    #[error("quota exceeded: {status:?}")]
    QuotaExceeded { status: QuotaStatus },
}

/// Maximum retained length of a provider diagnostic, in bytes.
///
/// Well above any real message, and far below a pathological proxy error page.
const MAX_DETAIL_BYTES: usize = 2 * 1024;

/// Extract the provider's human-readable diagnostic from an error response body.
///
/// # Why this is not `body["detail"].as_str()`
///
/// The provider encodes errors inconsistently, and both forms are measured:
///
/// ```text
/// 400/404: {"detail":"{\"detail\":\"invalid timeframe '7q'; valid: 1s, 5s, ...\"}"}
/// 401:     {"detail":"invalid api key"}
/// ```
///
/// On `400`/`404` the value of `detail` is *itself* a JSON document encoded as a string, so
/// reading one level surfaces raw JSON in a user-facing error message. This unwraps repeatedly
/// until `detail` stops being re-encoded, and falls back to the whole body when the response is
/// not in either shape (a proxy or CDN page, say). The result is always truncated.
pub(crate) fn extract_detail(body: &str) -> String {
    let mut current = body.trim().to_owned();

    // Bounded rather than `loop`: two levels are measured, and a handful of iterations is ample for
    // any further nesting the provider might add without risking a pathological input spinning here.
    for _ in 0..4 {
        let Ok(serde_json::Value::Object(object)) =
            serde_json::from_str::<serde_json::Value>(&current)
        else {
            break;
        };
        let Some(serde_json::Value::String(detail)) = object.get("detail") else {
            break;
        };
        current = detail.trim().to_owned();
    }

    crate::exchange::http::truncate_str(&current, MAX_DETAIL_BYTES)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn single_encoded_detail_is_read_directly() {
        // The measured 401 shape.
        assert_eq!(
            extract_detail(r#"{"detail":"invalid api key"}"#),
            "invalid api key"
        );
    }

    #[test]
    fn double_encoded_detail_is_unwrapped_to_the_message() {
        // The measured 400 shape: `detail` is a JSON document encoded as a string. Reading one
        // level would put raw JSON in front of the user.
        let body = r#"{"detail":"{\"detail\":\"invalid timeframe '7q'; valid: 1s, 5s, 15s\"}"}"#;

        assert_eq!(
            extract_detail(body),
            "invalid timeframe '7q'; valid: 1s, 5s, 15s"
        );
    }

    #[test]
    fn double_encoded_not_found_is_unwrapped() {
        // The measured 404 shape.
        let body =
            r#"{"detail":"{\"detail\":\"'NOPE_XYZ' has no candle data; browse /catalog\"}"}"#;

        assert_eq!(
            extract_detail(body),
            "'NOPE_XYZ' has no candle data; browse /catalog"
        );
    }

    #[test]
    fn a_non_json_body_is_returned_as_is() {
        // A proxy or CDN error page is not in either shape, and is still the best diagnostic there is.
        assert_eq!(
            extract_detail("<html>502 Bad Gateway</html>"),
            "<html>502 Bad Gateway</html>"
        );
    }

    #[test]
    fn json_without_a_detail_field_is_returned_whole() {
        assert_eq!(extract_detail(r#"{"error":"nope"}"#), r#"{"error":"nope"}"#);
    }

    #[test]
    fn a_non_string_detail_is_not_unwrapped() {
        // Only a re-encoded *string* is a nesting level; an object is already the message.
        assert_eq!(
            extract_detail(r#"{"detail":{"code":7}}"#),
            r#"{"detail":{"code":7}}"#
        );
    }

    #[test]
    fn an_oversized_diagnostic_is_truncated() {
        let body = format!(r#"{{"detail":"{}"}}"#, "a".repeat(MAX_DETAIL_BYTES * 3));

        assert_eq!(extract_detail(&body).len(), MAX_DETAIL_BYTES);
    }

    #[test]
    fn pathological_nesting_terminates_instead_of_spinning() {
        // Deeper than the unwrap budget: it must stop and return something, not loop.
        let mut body = r#"{"detail":"bottom"}"#.to_owned();
        for _ in 0..20 {
            body = serde_json::to_string(&serde_json::json!({ "detail": body })).unwrap();
        }

        assert!(!extract_detail(&body).is_empty());
    }

    #[test]
    fn an_empty_body_yields_an_empty_diagnostic() {
        assert_eq!(extract_detail(""), "");
    }
}
