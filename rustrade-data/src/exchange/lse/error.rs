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
    /// On the *export* endpoints an exhausted allowance is distinguished and reported as
    /// [`QuotaExceeded`](Self::QuotaExceeded). On the candle path the provider offers no way to
    /// tell a per-minute rate limit from an exhausted byte allowance, so both arrive here. Either
    /// way it is observable and terminal, and never silently retried.
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
    /// # Raised by the export path only
    /// The bulk-export endpoints report allowance exhaustion as a `429` carrying no `Retry-After`
    /// and no rate-limit headers, which this integration distinguishes from a per-minute rate
    /// limit and reports here, populating the position from
    /// [`usage`](super::vault::LseVaultClient::usage). A *candle* fetch that runs into a limit
    /// still surfaces as [`RateLimited`](Self::RateLimited) — the provider gives no way to tell
    /// the two apart on that path.
    #[error("quota exceeded: {status:?}")]
    QuotaExceeded { status: QuotaStatus },

    /// An export job reached a terminal state without producing an artifact.
    ///
    /// `expired` means the artifact was built and has since been reaped — roughly 48 hours after
    /// it was created. Both are terminal; re-exporting costs another export.
    #[error("export job {job_id} {status}{}", match .message.as_str() {
        "" => String::new(),
        message => format!(": {message}"),
    })]
    ExportFailed {
        job_id: String,
        status: String,
        message: String,
    },

    /// An export job did not become ready within the caller's timeout.
    ///
    /// **The job is not cancelled and the identifier stays valid** — it keeps building, so polling
    /// [`export_status`](super::vault::LseVaultClient::export_status) later picks it up without
    /// spending another export. This is a timeout on waiting, not on the job.
    #[error(
        "export job {job_id} still {status} when the caller's timeout elapsed; it keeps building, so poll it again rather than re-exporting"
    )]
    ExportTimeout { job_id: String, status: String },

    /// A downloaded artifact does not match the integrity metadata the job reported.
    ///
    /// The destination is left untouched rather than holding a corrupt artifact.
    ///
    /// `discarded` reports what happened to the partial file at `path`:
    /// - `false` — it is **retained**, because this call fetched it and the bytes are a real prefix
    ///   the next call can resume from with a `Range` request.
    /// - `true` — it was **removed**. A pre-existing partial file already looked complete, so no
    ///   transfer was attempted; failing verification then proves it is a leftover from a different
    ///   job that used the same destination, not a prefix of this one. Retaining it would fail
    ///   identically forever, so a re-call restarts instead.
    #[error("integrity check failed for {}: expected {expected}, got {actual} ({})", .path.display(), match .discarded {
        true => "unusable partial file discarded; re-call to restart",
        false => "partial file kept for resume",
    })]
    IntegrityMismatch {
        path: std::path::PathBuf,
        expected: String,
        actual: String,
        discarded: bool,
    },

    /// A filesystem operation failed.
    ///
    /// `message` names the operation and the path; the underlying [`std::io::Error`] is retained as
    /// the error [source](std::error::Error::source), so a caller can match on
    /// [`std::io::ErrorKind`] rather than parse a string.
    #[error("io error: {message}: {source}")]
    Io {
        message: String,
        #[source]
        source: std::io::Error,
    },

    /// An export artifact's columns match none of the layouts this integration decodes.
    ///
    /// The provider's tick schema **varies by dataset** — `fx` publishes `bid`/`ask`, `stocks`
    /// publishes `price`/`volume`, and the synthetic classes publish `price`/`volume`/`ask` — so
    /// the layout is resolved from the columns present. Reported rather than guessed at: a wrong
    /// column guess yields plausible numbers in the wrong field.
    #[error("unsupported export schema; columns were: {columns}")]
    UnsupportedSchema { columns: String },

    /// A row's `symbol` column does not match the symbol the export descriptor names.
    ///
    /// Every export is single-symbol, so this means the file and its descriptor disagree — the
    /// artifact is not the one the caller thinks it is. Caught because attributing it to the
    /// descriptor's instrument would be silent misattribution, and because `BP` and `BP.L` are
    /// different instruments quoted in different currencies.
    #[error("export symbol mismatch: descriptor says {expected:?} but a row carries {found:?}")]
    SymbolMismatch { expected: String, found: String },

    /// An artifact's timestamps go backwards.
    ///
    /// A backtest fed an unsorted stream produces a non-monotonic clock and wrong results in
    /// release, with no failure point, so this is rejected at decode. Note ties are **permitted** —
    /// the tape is non-decreasing rather than strictly ascending.
    #[error("export timestamps are not ascending: {found} follows {previous}")]
    NonMonotonicTimestamps {
        previous: DateTime<Utc>,
        found: DateTime<Utc>,
    },

    /// A row's timestamp is outside the representable [`DateTime<Utc>`] range.
    #[error("export timestamp {micros}µs is not representable")]
    TimestampNotRepresentable { micros: i64 },

    /// A provider `f64` has no [`rust_decimal::Decimal`] representation.
    ///
    /// Surfaced rather than substituted: a zero or a clamp here would put a real-looking price
    /// into fees, PnL and risk notional.
    #[error("price {value} is not representable as a decimal: {message}")]
    PriceNotRepresentable { value: f64, message: String },

    /// No registered instrument on this exchange carries the requested display symbol.
    ///
    /// Raised when deriving an [`InstrumentIndex`] from the caller's registry rather than
    /// accepting one. That derivation is what makes a fabricated index unrepresentable — the index
    /// is a public, unbounded `usize` and engine state indexes positionally — and it is the only
    /// check that catches a symbol typo, which would otherwise leave one instrument silently
    /// receiving no data at all.
    ///
    /// [`InstrumentIndex`]: rustrade_instrument::instrument::InstrumentIndex
    #[error(
        "no instrument registered on {exchange} with exchange name {symbol:?}; registered there: [{registered}]"
    )]
    UnknownInstrument {
        symbol: String,
        exchange: rustrade_instrument::exchange::ExchangeId,
        registered: String,
    },

    /// The Parquet artifact could not be read.
    #[cfg(feature = "lse-parquet")]
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
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
