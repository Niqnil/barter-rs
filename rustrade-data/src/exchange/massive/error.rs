//! Error types for Massive integration.

use crate::error::DataError;
use std::time::Duration;

/// Massive-specific errors.
///
/// The library returns these errors without automatic retry or reconnection.
/// Consumers decide how to handle rate limits, disconnections, and auth failures.
///
/// `#[non_exhaustive]`: new variants may be added without a major-version bump, so
/// downstream `match`es must include a wildcard arm.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum MassiveError {
    /// Rate limited by the API. Contains optional retry-after duration.
    ///
    /// Returned on HTTP 429 responses. The consumer decides whether and when to retry.
    RateLimited { retry_after: Option<Duration> },

    /// WebSocket connection dropped or ping timeout exceeded.
    ///
    /// Returned when:
    /// - WebSocket connection closes unexpectedly
    /// - Pong not received within 19 seconds of ping
    ///
    /// The consumer owns reconnection policy (backoff, credential refresh, dedup).
    Disconnected { reason: String },

    /// Authentication failed.
    ///
    /// Returned when:
    /// - API key is invalid or expired
    /// - API key lacks permission for the requested resource
    Auth { message: String },

    /// API returned an error response.
    ///
    /// Covers non-auth, non-rate-limit API errors (invalid parameters, not found, etc.)
    Api { status: u16, message: String },

    /// Network or HTTP client error.
    Http { message: String },

    /// JSON deserialization failed.
    Deserialize { message: String, payload: String },

    /// Environment variable not set.
    EnvVar { var: &'static str },

    /// Client-side input validation failed.
    ///
    /// Returned when input parameters are invalid before making an API request
    /// (e.g., timestamp out of representable range).
    InvalidInput { message: String },

    /// A paginated fetch exceeded the maximum number of pages before the API
    /// reported the end of the result set.
    ///
    /// Yielded as a terminal error on the affected stream. For a market-data
    /// client a silent truncation is indistinguishable from a genuinely small
    /// result set, so pagination fails loudly rather than returning a partial
    /// `Vec`. Hitting this limit indicates either an unexpectedly large query or
    /// a server that never signals the final page — inspect the query before
    /// retrying.
    PaginationLimitExceeded { pages: usize, limit: usize },

    /// A paginated fetch revisited a URL it had already fetched.
    ///
    /// Yielded as a terminal error on the affected stream. A `next_url` that
    /// points back to an already-visited page would otherwise loop forever;
    /// detecting the cycle turns silent non-termination into an observable
    /// failure.
    CyclicPagination { url: String },

    /// A paginated fetch received a `next_url` outside the client's configured
    /// origin (scheme + host + port), or one that failed to parse as a URL.
    ///
    /// Yielded as a terminal error *before* the request is issued. The client
    /// attaches its API key as an `Authorization: Bearer` header only after this
    /// origin check passes, so a `next_url` that names an unexpected origin is
    /// rejected before the token is ever attached. A prefix check is insufficient
    /// — a look-alike host such as `https://api.massive.com.evil.example` (or the
    /// separator-less `https://api.massive.comevil.example`) shares the prefix
    /// yet is a different origin — so origins are parsed and compared, and a
    /// mismatched or unparseable `next_url` is rejected (fail-closed).
    ///
    /// A well-behaved Massive API only ever returns a `next_url` under its own
    /// origin (the path and `cursor` query differ; the origin is fixed). Hitting
    /// this indicates a server-side bug, response tampering, or a misconfigured
    /// base URL. Consumers may match on it to alert distinctly from ordinary
    /// input-validation failures.
    UntrustedNextUrl {
        /// The rejected `next_url`, exactly as received (unparsed if it failed
        /// to parse as a URL at all).
        next_url: String,
        /// ASCII-serialized origin (`scheme://host[:port]`) the client trusts,
        /// derived from its configured base URL.
        expected_origin: String,
    },
}

impl std::fmt::Display for MassiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MassiveError::RateLimited { retry_after } => {
                write!(f, "Massive rate limited")?;
                if let Some(duration) = retry_after {
                    write!(f, " (retry after {:?})", duration)?;
                }
                Ok(())
            }
            MassiveError::Disconnected { reason } => {
                write!(f, "Massive disconnected: {}", reason)
            }
            MassiveError::Auth { message } => {
                write!(f, "Massive auth failed: {}", message)
            }
            MassiveError::Api { status, message } => {
                write!(f, "Massive API error ({}): {}", status, message)
            }
            MassiveError::Http { message } => {
                write!(f, "Massive HTTP error: {}", message)
            }
            MassiveError::Deserialize { message, payload } => {
                let boundary = payload.floor_char_boundary(100);
                let truncated = &payload[..boundary];
                let ellipsis = if boundary < payload.len() { "..." } else { "" };
                write!(
                    f,
                    "Massive deserialize error: {} (payload: {truncated}{ellipsis})",
                    message
                )
            }
            MassiveError::EnvVar { var } => {
                write!(f, "Massive environment variable not set: {}", var)
            }
            MassiveError::InvalidInput { message } => {
                write!(f, "Massive invalid input: {}", message)
            }
            MassiveError::PaginationLimitExceeded { pages, limit } => {
                write!(
                    f,
                    "Massive pagination exceeded {limit}-page limit (fetched {pages} pages) — result may be incomplete"
                )
            }
            MassiveError::CyclicPagination { url } => {
                write!(
                    f,
                    "Massive pagination cycle detected: next_url revisits {url}"
                )
            }
            MassiveError::UntrustedNextUrl {
                next_url,
                expected_origin,
            } => {
                write!(
                    f,
                    "Massive rejected next_url outside trusted origin {expected_origin}: {next_url}"
                )
            }
        }
    }
}

impl std::error::Error for MassiveError {}

impl From<MassiveError> for DataError {
    fn from(err: MassiveError) -> Self {
        DataError::Socket(err.to_string())
    }
}

impl From<reqwest::Error> for MassiveError {
    fn from(err: reqwest::Error) -> Self {
        MassiveError::Http {
            message: err.to_string(),
        }
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for MassiveError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        MassiveError::Disconnected {
            reason: err.to_string(),
        }
    }
}
