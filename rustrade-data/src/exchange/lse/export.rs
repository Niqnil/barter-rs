//! Bulk export jobs against the vault: submit, poll, download.
//!
//! The vault serves candles over REST ([`historical`](super::historical)), but the **raw tick tape
//! is reachable only here** — there is no streaming or REST path to it. Exports are asynchronous:
//! submit a job, poll it to `ready`, then download the artifact.
//!
//! # ⚠️ This is a different API surface from the candles endpoint
//! Measured differences that a client written against the candles path gets wrong:
//!
//! - Submit answers **`202 Accepted`**, not `200`. A `== 200` check never polls.
//! - The job-id field is **renamed between responses**: the submit body says `job_id`, the status
//!   body says `id`. Hence [`LseExportJob`] and [`LseExportJobStatus`] are separate types.
//! - Allowance exhaustion is a **`429` with no `Retry-After`**, mapped here to
//!   [`LseError::QuotaExceeded`] rather than [`LseError::RateLimited`] — an exhausted export
//!   allowance is a different condition from a per-minute rate limit, and only the former carries a
//!   [`QuotaStatus`](super::quota::QuotaStatus).
//!
//! # ⚠️ A rejected submit still costs you an export
//! The allowance is **5 exports per hour**, and the counter increments on *attempt*: a `400` for a
//! misspelled dataset consumes 20% of the hour's budget just as a successful job does. (Requests
//! rejected at the CDN edge do not count.) This is why [`LseExportRequest::new`] validates
//! everything it can before a request is ever sent — pre-flight validation here is an **economic**
//! requirement, not defensive style.
//!
//! # ⚠️ `symbol: "all"` does not mean "every symbol", and every artifact is single-symbol
//! It is a literal that matches nothing. Measured as a controlled pair on one dataset, timeframe
//! and range: `symbol: "all"` returned `202` → `ready` → a **valid Parquet file with the full
//! schema and zero rows**, while the same request naming a real symbol returned rows. There is no
//! error and no warning, and it costs an export. The tick path behaves identically, and omitting
//! `symbol` is a hard `400` — so **no spelling produces a multi-symbol export**, and an export
//! naming `"all"` is rejected before it is sent; see [`LseExportRequest::new`].
//!
//! The consequence reaches the consumer: combining N instruments means N artifacts merged with
//! [`merge_time_sorted`](crate::streams::merge::merge_time_sorted), as on the candle replay path.
//! A decoded artifact is a synchronous iterator of already-tagged events, so it needs adapting to
//! that merge's stream item first; `read_export` documents the adapter. (Not linked: the decoder is
//! behind its own feature, so the link would be dead in a build without it.)
//!
//! The rejection is **case-sensitive on purpose**: `ALL` is Allstate's real ticker, so refusing it
//! would forbid a correct export with no way around it. "Matches nothing" is a property of the
//! (dataset, symbol) pair, not of the string.
//!
//! # Range semantics
//! `start` is **inclusive** and `end` is **exclusive**, matching the candles endpoint. Measured
//! twice: a tick export over `[2026-07-01, 2026-07-02)` yielded only 2026-07-01, and a daily candle
//! export ending `2026-07-20` produced no bar for the 20th. The range is **date-granular**, which
//! [`LseExportRange`] makes explicit by taking [`NaiveDate`] rather than silently truncating a
//! timestamp.
//!
//! # ⚠️ Licensing
//! Exported data is **not redistributable**. An export produces a file on your disk that is
//! trivially easy to commit or share by accident; it must not be. See the
//! [module documentation](super) and <https://londonstrategicedge.com/terms>.

use crate::exchange::http::{MAX_ERROR_BODY_DOWNLOAD_BYTES, read_body_capped};
use crate::exchange::lse::error::{LseError, extract_detail};
use crate::exchange::lse::market::{LseDataset, candle_interval_str};
use crate::exchange::lse::vault::LseVaultClient;
use crate::subscription::candle::CandleInterval;
use chrono::{DateTime, NaiveDate, Utc};
use futures_util::StreamExt;
use rustrade_instrument::exchange::ExchangeId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, info, warn};

/// Size of the buffer used to read an existing `.part` file back into the hasher.
const HASH_READ_BUFFER_BYTES: usize = 64 * 1024;

/// Size of the write buffer coalescing download chunks before they reach the `.part` file.
const DOWNLOAD_BUFFER_BYTES: usize = 256 * 1024;

/// The literal the provider treats as a symbol rather than as "every symbol".
const ALL_SYMBOLS_LITERAL: &str = "all";

/// The only export format this integration decodes.
const EXPORT_FORMAT: &str = "parquet";

/// What an export job covers, in the provider's own timeframe vocabulary.
///
/// [`Tick`](Self::Tick) is the whole point of the export path: the raw tape is not reachable over
/// REST or WebSocket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LseExportTimeframe {
    /// The raw tick tape.
    ///
    /// # ⚠️ The tick schema varies by dataset
    /// Measured: `fx` exports `{ts, symbol, bid, ask}` (a quote tape), `stocks` exports
    /// `{ts, symbol, price, volume}` (a trade tape). A decoder must dispatch on the columns
    /// actually present rather than assume one shape.
    Tick,

    /// Aggregated candles at the given resolution.
    Candle(CandleInterval),
}

impl LseExportTimeframe {
    /// Returns the provider's spelling of this timeframe.
    ///
    /// # Errors
    /// Returns [`LseError::UnsupportedInterval`] for a resolution the provider does not serve.
    pub fn as_lse_str(&self) -> Result<&'static str, LseError> {
        match self {
            Self::Tick => Ok("tick"),
            Self::Candle(interval) => {
                candle_interval_str(*interval).ok_or(LseError::UnsupportedInterval {
                    interval: *interval,
                })
            }
        }
    }
}

/// The date range an export covers.
///
/// `start` is inclusive, `end` is **exclusive**, and both are dates rather than timestamps —
/// the export endpoint is date-granular, and taking [`NaiveDate`] states that in the type instead
/// of silently discarding a caller's time-of-day.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LseExportRange {
    start: NaiveDate,
    end: NaiveDate,
}

impl LseExportRange {
    /// Construct a range.
    ///
    /// # Errors
    /// Returns [`LseError::InvalidInput`] unless `start < end`. An empty or inverted range would
    /// produce a valid, empty artifact at the cost of one export.
    pub fn new(start: NaiveDate, end: NaiveDate) -> Result<Self, LseError> {
        if start >= end {
            return Err(LseError::InvalidInput {
                message: format!(
                    "export range start {start} must be before end {end} (end is exclusive)"
                ),
            });
        }

        Ok(Self { start, end })
    }

    /// The inclusive first date.
    pub fn start(&self) -> NaiveDate {
        self.start
    }

    /// The exclusive last date.
    pub fn end(&self) -> NaiveDate {
        self.end
    }
}

/// A validated export request.
///
/// Construction is the validation step; see [`new`](Self::new). Holding one of these means the
/// request is worth spending an export on, as far as anything checkable client-side goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LseExportRequest {
    dataset: LseDataset,
    symbol: String,
    timeframe: LseExportTimeframe,
    range: LseExportRange,
}

impl LseExportRequest {
    /// Validate and build an export request.
    ///
    /// # Why this validates so much
    /// A rejected submit consumes one of five hourly exports. Every check here is a rejection the
    /// provider would otherwise bill for.
    ///
    /// # Errors
    /// - [`LseError::InvalidInput`] if `symbol` is blank. Surrounding whitespace is trimmed before
    ///   any check, so a padded symbol is neither smuggled past the `"all"` rejection below nor
    ///   sent verbatim into a `400` that would cost an export.
    /// - [`LseError::InvalidInput`] if `symbol` is `"all"`, on **either** timeframe — measured on
    ///   both to produce a valid, **empty** artifact with no error (see the
    ///   [module documentation](self)).
    /// - [`LseError::InvalidInput`] if a candle resolution is requested for one of the provider's
    ///   synthetic classes. Those serve candles over REST but are **tick-only on the export path**
    ///   — measured: `{dataset: volatility, timeframe: 1d}` → `400 … it is tick-only`.
    /// - [`LseError::UnsupportedInterval`] for a resolution the provider does not serve.
    pub fn new(
        dataset: LseDataset,
        symbol: impl Into<String>,
        timeframe: LseExportTimeframe,
        range: LseExportRange,
    ) -> Result<Self, LseError> {
        // Trimmed before anything reads it, including the `"all"` comparison below and the request
        // body. Incidental whitespace is a copy-paste artefact, and leaving it in would both slip
        // `" all"` past an exact-match guard and buy a billed 400 for a symbol that is otherwise
        // correct.
        let symbol: String = symbol.into();
        let symbol = symbol.trim().to_owned();

        if symbol.is_empty() {
            return Err(LseError::InvalidInput {
                message: "export symbol must not be blank; the provider rejects a missing symbol \
                          with a 400, which still consumes an export"
                    .to_owned(),
            });
        }

        // Rejecting the resolution before the request is sent, not after being billed for a 400.
        let _ = timeframe.as_lse_str()?;

        // Exact match, deliberately NOT case-insensitive: `ALL` is Allstate's real ticker (verified
        // live against the provider's own price endpoint), and rejecting it would forbid a correct
        // export with no escape hatch. "Matches nothing" is a property of the (dataset, symbol)
        // pair, not of this string alone. Lowercase is safe to reject because the provider
        // publishes symbols uppercase, so `all` is nobody's real symbol - and the decoder's
        // symbol-column assert would reject the artifact anyway.
        if symbol == ALL_SYMBOLS_LITERAL {
            return Err(LseError::InvalidInput {
                message: format!(
                    "symbol {ALL_SYMBOLS_LITERAL:?} is a literal the provider matches against the \
                     symbol column, not a request for every symbol: an export naming it returns a \
                     valid but EMPTY artifact, with no error, and still consumes one of five \
                     hourly exports - name a real symbol instead. Measured on both the candle and \
                     the tick path; every artifact this provider will produce is single-symbol. \
                     (If you meant Allstate, its symbol is uppercase {:?}.)",
                    ALL_SYMBOLS_LITERAL.to_uppercase()
                ),
            });
        }

        if let LseExportTimeframe::Candle(interval) = timeframe
            && !dataset.is_candle_class()
        {
            return Err(LseError::InvalidInput {
                message: format!(
                    "dataset {:?} is tick-only on the export path, so resolution {interval} \
                     cannot be exported for it - it serves candles over REST, but export \
                     timeframes follow the provider's candle-class split; use \
                     LseExportTimeframe::Tick",
                    dataset.as_catalog_str()
                ),
            });
        }

        Ok(Self {
            dataset,
            symbol,
            timeframe,
            range,
        })
    }

    /// The dataset this request targets.
    pub fn dataset(&self) -> LseDataset {
        self.dataset
    }

    /// The display symbol this request targets.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// The requested timeframe.
    pub fn timeframe(&self) -> LseExportTimeframe {
        self.timeframe
    }

    /// The requested date range.
    pub fn range(&self) -> LseExportRange {
        self.range
    }

    /// The JSON body the vault expects.
    ///
    /// # Errors
    /// Returns [`LseError::UnsupportedInterval`] if the timeframe has no provider spelling — a
    /// state [`new`](Self::new) already rejects, re-checked here rather than unwrapped.
    fn to_body(&self) -> Result<ExportRequestBody<'_>, LseError> {
        Ok(ExportRequestBody {
            dataset: self.dataset.as_catalog_str(),
            symbol: &self.symbol,
            timeframe: self.timeframe.as_lse_str()?,
            start: self.range.start().to_string(),
            end: self.range.end().to_string(),
            format: EXPORT_FORMAT,
        })
    }
}

/// The wire shape of an export submission.
#[derive(Debug, Serialize)]
struct ExportRequestBody<'a> {
    dataset: &'a str,
    symbol: &'a str,
    timeframe: &'a str,
    start: String,
    end: String,
    format: &'a str,
}

/// Lifecycle state of an export job.
///
/// `queued` and `ready` are measured; `failed` and `expired` are documented by the provider's own
/// client. [`Other`](Self::Other) preserves anything else rather than collapsing it to a guess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LseExportStatus {
    /// Accepted, not yet building.
    Queued,
    /// Building.
    Running,
    /// The artifact is available to download.
    Ready,
    /// The job will not produce an artifact.
    Failed,
    /// The artifact existed and has since been reaped (roughly 48 hours after it was built).
    Expired,
    /// A status this integration does not know. Preserved verbatim.
    Other(String),
}

impl LseExportStatus {
    /// Whether polling this job further is pointless.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Ready | Self::Failed | Self::Expired)
    }

    /// The provider's spelling of this status.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::Expired => "expired",
            Self::Other(other) => other,
        }
    }
}

impl From<&str> for LseExportStatus {
    fn from(value: &str) -> Self {
        match value {
            "queued" => Self::Queued,
            "running" => Self::Running,
            "ready" => Self::Ready,
            "failed" => Self::Failed,
            "expired" => Self::Expired,
            other => Self::Other(other.to_owned()),
        }
    }
}

impl std::fmt::Display for LseExportStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for LseExportStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::from(String::deserialize(deserializer)?.as_str()))
    }
}

/// The response to an export submission.
///
/// Distinct from [`LseExportJobStatus`] because the provider renames the identifier between the
/// two responses: `job_id` here, `id` there.
///
/// `#[non_exhaustive]` because this mirrors a provider response: the provider may add a field, and
/// mirroring it here should stay a non-breaking change.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct LseExportJob {
    /// The job identifier, to be passed to [`LseVaultClient::export_status`].
    pub job_id: String,

    /// Status at submission time — measured as `queued`.
    pub status: LseExportStatus,

    /// The provider's size estimate.
    ///
    /// # ⚠️ Do not act on this
    /// It is `null` for candle exports, and for tick exports it appears to ignore the symbol and
    /// date filters entirely: measured at `7,111,419,512` for an artifact that turned out to be
    /// `596,985` bytes (**11,900×**) and `7,677,490,312` for one of `1,618,641` bytes (**4,700×**).
    /// Surfaced for completeness only; never use it for preflight, allocation or budgeting.
    #[serde(default)]
    pub est_bytes: Option<u64>,
}

/// The state of an export job, as reported by the status endpoint.
///
/// `#[non_exhaustive]` for the same reason as [`LseExportJob`] — it mirrors a provider response.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct LseExportJobStatus {
    /// The job identifier. Note the provider spells this `job_id` on the submit response.
    pub id: String,

    /// Current lifecycle state.
    pub status: LseExportStatus,

    /// Row count of the artifact, once known.
    ///
    /// # ⚠️ Check this
    /// A zero-row export is still a well-formed Parquet file carrying the complete schema, so
    /// "status is ready and the file parses" does **not** imply the request matched anything.
    #[serde(default)]
    pub rows: Option<u64>,

    /// Artifact size in bytes, once known. Unlike [`LseExportJob::est_bytes`], this is exact.
    #[serde(default)]
    pub bytes: Option<u64>,

    /// Lowercase hex SHA-256 of the artifact, once known.
    #[serde(default)]
    pub sha256: Option<String>,

    /// The provider's diagnostic when [`status`](Self::status) is `failed`.
    #[serde(default)]
    pub error: Option<String>,

    /// When the artifact is reaped — roughly 48 hours after it is built.
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,

    /// The provider's internal source table, e.g. `candles_etf_1d` or `ticks_fx`.
    #[serde(default)]
    pub table_name: Option<String>,
}

/// A downloaded export artifact, with the provenance needed to decode it safely.
///
/// # Why this is not a bare `PathBuf`
/// An export job targets exactly one dataset, and [`LseDataset::exchange_id`] is total, so **one
/// file has exactly one [`ExchangeId`]**. Carrying the dataset makes that derivable rather than
/// re-accepted as an unchecked argument at decode time — which is what turns "rows from a futures
/// export tagged onto an equities-registered instrument" from a silent misattribution into a
/// construction-time error.
///
/// A caller holding a file obtained out of band constructs one explicitly with
/// [`new`](Self::new), which puts the obligation in the type rather than in prose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LseExport {
    path: PathBuf,
    dataset: LseDataset,
    symbol: String,
    timeframe: LseExportTimeframe,
    range: LseExportRange,
}

impl LseExport {
    /// Describe an export artifact already on disk.
    ///
    /// Prefer [`LseVaultClient::download_export`], which fills this in from the job it downloaded.
    /// Use this for a file obtained out of band — and note that the dataset you name here is what
    /// every decoded event's [`ExchangeId`] will be derived from, while `symbol` is what the
    /// decoder checks every row against.
    pub fn new(
        path: impl Into<PathBuf>,
        dataset: LseDataset,
        symbol: impl Into<String>,
        timeframe: LseExportTimeframe,
        range: LseExportRange,
    ) -> Self {
        Self {
            path: path.into(),
            dataset,
            symbol: symbol.into(),
            timeframe,
            range,
        }
    }

    /// The artifact's location on disk.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The dataset the artifact was exported from.
    pub fn dataset(&self) -> LseDataset {
        self.dataset
    }

    /// The vault display symbol the artifact was exported for.
    ///
    /// Every export is single-symbol — the provider offers no multi-symbol spelling — so this is
    /// the value every row's `symbol` column must carry.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// The timeframe the artifact contains.
    pub fn timeframe(&self) -> LseExportTimeframe {
        self.timeframe
    }

    /// The date range the artifact covers.
    pub fn range(&self) -> LseExportRange {
        self.range
    }

    /// The venue every event decoded from this artifact is stamped with.
    pub fn exchange_id(&self) -> ExchangeId {
        self.dataset.exchange_id()
    }
}

impl LseVaultClient {
    /// Submit an export job.
    ///
    /// Returns as soon as the provider accepts the job (`202`); the artifact is built
    /// asynchronously. Poll with [`export_status`](Self::export_status), or use
    /// [`await_export`](Self::await_export).
    ///
    /// # ⚠️ This spends allowance whether or not it succeeds
    /// The allowance is five exports per hour and a rejection still consumes one, which is why
    /// [`LseExportRequest`] validates at construction. Poll
    /// [`usage`](LseVaultClient::usage) to see where you are — the position is deliberately not
    /// fetched here, since that would add a network round trip to every submission.
    ///
    /// # Errors
    /// - [`LseError::QuotaExceeded`] when the export allowance is exhausted, carrying the
    ///   allowance position at the point of rejection.
    /// - [`LseError::Api`] for any other non-success status, with the provider's diagnostic
    ///   unwrapped from its envelope.
    /// - [`LseError::Deserialize`] if the response will not decode.
    pub async fn submit_export(
        &self,
        request: &LseExportRequest,
    ) -> Result<LseExportJob, LseError> {
        let body = request.to_body()?;

        info!(
            dataset = body.dataset,
            symbol = body.symbol,
            timeframe = body.timeframe,
            start = body.start,
            end = body.end,
            "submitting vault export job; this consumes one of the hourly export allowance"
        );

        self.post_json("export", &body).await
    }

    /// Fetch the current state of an export job.
    ///
    /// Polling costs nothing from the export allowance — only from the per-minute call budget.
    ///
    /// # Errors
    /// - [`LseError::RateLimited`] on a `429` — deliberately **not**
    ///   [`LseError::QuotaExceeded`], which is what
    ///   [`submit_export`](Self::submit_export) and [`download_export`](Self::download_export)
    ///   report. Those two spend metered allowance (an export, and bytes respectively), so a `429`
    ///   there means the allowance is gone; a status read spends neither, so a `429` here is the
    ///   per-minute limit. Reporting it as an exhausted allowance would also add a `usage()` round
    ///   trip to every iteration of a polling loop, and would assert a provider behaviour that has
    ///   not been measured on this endpoint.
    /// - [`LseError::Api`] for any other non-success status, including the `404` an unknown job id
    ///   returns.
    /// - [`LseError::Deserialize`] if the response will not decode.
    pub async fn export_status(&self, job_id: &str) -> Result<LseExportJobStatus, LseError> {
        self.get_json(&format!("export/{job_id}"), &[]).await
    }

    /// Poll an export job until it reaches a terminal state.
    ///
    /// # Cadence is the caller's
    /// `poll_interval` and `timeout` are both supplied by the caller and neither is defaulted:
    /// this integration exposes the allowance signal and never decides pacing on the consumer's
    /// behalf.
    ///
    /// # Errors
    /// - [`LseError::ExportFailed`] if the job reaches `failed` or `expired`.
    /// - [`LseError::ExportTimeout`] if `timeout` elapses first. The job keeps building — the
    ///   identifier stays valid, so a later [`export_status`](Self::export_status) can pick it up
    ///   without spending another export.
    /// - Anything [`export_status`](Self::export_status) returns.
    pub async fn await_export(
        &self,
        job_id: &str,
        poll_interval: Duration,
        timeout: Duration,
    ) -> Result<LseExportJobStatus, LseError> {
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let status = self.export_status(job_id).await?;

            match &status.status {
                LseExportStatus::Ready => return Ok(status),
                LseExportStatus::Failed | LseExportStatus::Expired => {
                    return Err(LseError::ExportFailed {
                        job_id: job_id.to_owned(),
                        status: status.status.to_string(),
                        message: status.error.unwrap_or_default(),
                    });
                }
                pending => {
                    debug!(job_id, status = %pending, "export job not ready");
                }
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(LseError::ExportTimeout {
                    job_id: job_id.to_owned(),
                    status: status.status.to_string(),
                });
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Download a `ready` export artifact to `destination`.
    ///
    /// # Resume and integrity
    /// Downloads into `destination` + `.part`, resuming an interrupted transfer with a `Range`
    /// request (the provider advertises `Accept-Ranges: bytes` and answers `206`). A `206` is
    /// accepted only if its `Content-Range` starts at the byte that was asked for; a `416` is
    /// treated as "the `.part` already holds the whole artifact" rather than as a failure, so a
    /// transfer interrupted between the final write and the rename still converges. On completion
    /// the artifact is verified against the job's `sha256` and `bytes`, then atomically renamed
    /// into place. **On a mismatch an error is returned and the destination is left untouched**,
    /// so it never contains a corrupt file.
    ///
    /// **Verification covers only the dimensions the job actually reports.** Both fields are
    /// [`Option`], and every measured `ready` job carried both — but that is an observation, not a
    /// guarantee the provider makes. A `ready` job missing one is downloaded and renamed with that
    /// check skipped, and a `warn!` naming the skipped check is emitted. Skipping is deliberate:
    /// refusing an artifact the provider considers ready, over metadata this integration merely
    /// expects, would forbid a correct download with no way around it.
    ///
    /// # ⚠️ Caller obligation: one download per destination at a time
    /// Concurrent calls sharing a `destination` are **not supported**. Each reads the `.part` file's
    /// length to decide where to resume from, then opens it; two overlapping calls interleave writes
    /// against independently-primed hashers, corrupting the `.part` or failing verification
    /// spuriously. Serialise them, or give each its own destination.
    ///
    /// The `.part` is normally **kept**, so re-calling resumes rather than restarting. The one
    /// exception is a `.part` that failed verification after this call appended nothing to it —
    /// either it already looked complete from the job's byte count, or the server rejected the
    /// resume `Range` as unsatisfiable. Either way it belongs to a *different* job that used this
    /// destination, so it is removed and a re-call restarts. Keeping it would fail identically
    /// forever. [`LseError::IntegrityMismatch`] reports which happened via `discarded`.
    ///
    /// The URL is built from the client's base URL and the job id rather than from the job's
    /// `download_url`, preserving this client's invariant that it only ever requests URLs it
    /// constructed itself.
    ///
    /// # ⚠️ A `ready` job may legitimately contain zero rows
    /// Check [`LseExportJobStatus::rows`]. A zero-row artifact is a well-formed Parquet file with
    /// the full schema, so nothing downstream will complain.
    ///
    /// # Errors
    /// - [`LseError::InvalidInput`] if the job is not `ready`.
    /// - [`LseError::IntegrityMismatch`] if the downloaded bytes do not match the job's `sha256`
    ///   or `bytes`.
    /// - [`LseError::Io`] for any filesystem failure.
    /// - [`LseError::QuotaExceeded`] / [`LseError::Api`] as for
    ///   [`submit_export`](Self::submit_export).
    pub async fn download_export(
        &self,
        job: &LseExportJobStatus,
        destination: impl AsRef<Path>,
        request: &LseExportRequest,
    ) -> Result<LseExport, LseError> {
        if job.status != LseExportStatus::Ready {
            return Err(LseError::InvalidInput {
                message: format!(
                    "export job {} is {}, not ready; nothing to download",
                    job.id, job.status
                ),
            });
        }

        let destination = destination.as_ref();
        let part = part_path(destination);

        if let Some(parent) = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| LseError::Io {
                    message: format!("creating {}", parent.display()),
                    source,
                })?;
        }

        // Hash the already-downloaded prefix so a resumed transfer still verifies end to end.
        let (mut hasher, mut downloaded) = hash_existing_part(&part).await?;

        // A `.part` that already holds the whole artifact means a previous run was interrupted
        // between the final write and the rename. Re-requesting would only earn a 416. The
        // `downloaded > 0` guard keeps a job reporting `bytes: 0` from satisfying this vacuously
        // when no `.part` exists at all, which would skip the fetch and then rename a file that was
        // never created.
        let mut complete = downloaded > 0 && job.bytes.is_some_and(|total| downloaded >= total);

        if !complete {
            'download: {
                let url = format!("{}/export/{}/download", self.base_url(), job.id);
                let mut builder = self.http().get(&url);
                if downloaded > 0 {
                    builder =
                        builder.header(reqwest::header::RANGE, format!("bytes={downloaded}-"));
                }

                let response = builder.send().await?;
                let status = response.status();

                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    return Err(self.quota_exceeded().await);
                }

                // A `416` answers a `Range` starting at or past the artifact's length: the `.part`
                // already holds everything the server has. That is precisely the case the `complete`
                // check above cannot decide for itself, because it needs `job.bytes` and the job is
                // entitled to report `None` — verification tolerates that absence (see below), so
                // resuming must too. Failing here instead would make the documented "re-calling
                // resumes" never converge: every retry would re-request the same unsatisfiable range.
                // Whether those bytes are actually *this* job's artifact is still decided below.
                if downloaded > 0 && status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
                    warn!(
                        job_id = job.id,
                        downloaded,
                        "range request rejected as unsatisfiable; the existing part file already holds \
                     the whole artifact"
                    );
                    complete = true;
                    break 'download;
                }

                if !status.is_success() {
                    let body = read_body_capped(response, MAX_ERROR_BODY_DOWNLOAD_BYTES).await?;
                    return Err(LseError::Api {
                        status: status.as_u16(),
                        message: extract_detail(&body),
                    });
                }

                // The server is entitled to ignore `Range` and answer `200` with the whole artifact.
                // Restarting is then the only correct response — appending would duplicate the prefix.
                let resuming = downloaded > 0 && status == reqwest::StatusCode::PARTIAL_CONTENT;
                if resuming {
                    verify_resume_offset(&response, downloaded)?;
                }
                if downloaded > 0 && !resuming {
                    warn!(
                        job_id = job.id,
                        downloaded, "range request answered in full; restarting the download"
                    );
                    hasher = Sha256::new();
                    downloaded = 0;
                }

                let file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(!resuming)
                    .append(resuming)
                    .open(&part)
                    .await
                    .map_err(|source| LseError::Io {
                        message: format!("opening {}", part.display()),
                        source,
                    })?;

                // Buffered because `tokio::fs` dispatches each write to the blocking pool: writing every
                // HTTP chunk straight through costs one round trip per chunk, and artifacts run to
                // gigabytes. An interrupted transfer loses at most one buffer of resume progress — the
                // next call re-reads the `.part`'s real on-disk length, so it resumes correctly from
                // whatever landed, just slightly further back.
                let mut file = tokio::io::BufWriter::with_capacity(DOWNLOAD_BUFFER_BYTES, file);

                let mut stream = response.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    hasher.update(&chunk);
                    downloaded += chunk.len() as u64;
                    file.write_all(&chunk)
                        .await
                        .map_err(|source| LseError::Io {
                            message: format!("writing {}", part.display()),
                            source,
                        })?;
                }

                file.flush().await.map_err(|source| LseError::Io {
                    message: format!("flushing {}", part.display()),
                    source,
                })?;
            }
        }

        let digest = hex::encode(hasher.finalize());

        // Retaining the `.part` is only useful when this call appended something: those bytes are
        // then a real prefix that a `Range` request can continue. `complete` means it appended
        // nothing — either the transfer was skipped because a pre-existing `.part` already looked
        // complete, or the server answered the resume `Range` with a 416. A failed check then proves
        // that file is NOT this job's artifact — it is a leftover from a different job that used the
        // same destination. Keeping it would fail identically on every retry, so the documented
        // "re-calling resumes" would never converge. Discarding is loud, and only ever removes a
        // file this integration is the sole writer of.
        let discard = complete;

        if let Some(expected) = job.bytes.filter(|expected| *expected != downloaded) {
            return Err(self
                .integrity_mismatch(
                    &part,
                    format!("{expected} bytes"),
                    format!("{downloaded} bytes"),
                    discard,
                )
                .await);
        }

        if let Some(expected) = job
            .sha256
            .as_ref()
            .filter(|expected| !expected.eq_ignore_ascii_case(&digest))
        {
            return Err(self
                .integrity_mismatch(&part, expected.clone(), digest, discard)
                .await);
        }

        // Verification is only as complete as the job's metadata. Every measured `ready` job
        // reported both fields, so this should not fire — which is exactly why it is worth saying
        // out loud if it ever does, rather than renaming an unverified artifact silently.
        if job.bytes.is_none() || job.sha256.is_none() {
            warn!(
                job_id = job.id,
                path = %destination.display(),
                checked_length = job.bytes.is_some(),
                checked_sha256 = job.sha256.is_some(),
                "export job reports no integrity metadata for one or both checks; accepting the \
                 artifact with that verification skipped"
            );
        }

        tokio::fs::rename(&part, destination)
            .await
            .map_err(|source| LseError::Io {
                message: format!("renaming {} to {}", part.display(), destination.display()),
                source,
            })?;

        info!(
            job_id = job.id,
            path = %destination.display(),
            bytes = downloaded,
            rows = job.rows.unwrap_or_default(),
            "export downloaded and verified"
        );

        Ok(LseExport::new(
            destination,
            request.dataset(),
            request.symbol(),
            request.timeframe(),
            request.range(),
        ))
    }

    /// Issue an authenticated `POST` against a vault path and deserialise the JSON body.
    ///
    /// Accepts `202` as success — the export endpoint's normal answer.
    ///
    /// # Errors
    /// Maps a `429` to [`LseError::QuotaExceeded`], any other non-success status to
    /// [`LseError::Api`], and an undecodable body to [`LseError::Deserialize`].
    pub(crate) async fn post_json<B, T>(&self, path: &str, body: &B) -> Result<T, LseError>
    where
        B: Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        let url = format!("{}/{path}", self.base_url());
        let response = self.http().post(&url).json(body).send().await?;
        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(self.quota_exceeded().await);
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

    /// Build a [`LseError::IntegrityMismatch`], discarding the partial file when it cannot be
    /// resumed from.
    ///
    /// `discard` is set when this call appended no bytes to the `.part` — it already looked
    /// complete, or the resume `Range` came back `416` — so the file cannot be a partial download of
    /// this job and no retry can advance it. A removal failure is logged rather than replacing the
    /// integrity error, which is the more useful diagnostic of the two.
    async fn integrity_mismatch(
        &self,
        part: &Path,
        expected: String,
        actual: String,
        discard: bool,
    ) -> LseError {
        if discard {
            warn!(
                path = %part.display(),
                %expected,
                %actual,
                "a pre-existing partial file failed verification and this call appended no bytes to \
                 it, so it cannot be a partial download of this job; discarding it so a re-call \
                 restarts"
            );

            if let Err(error) = tokio::fs::remove_file(part).await {
                warn!(
                    path = %part.display(),
                    %error,
                    "could not remove the unusable partial file; remove it by hand, or this job \
                     will keep failing verification"
                );
            }
        }

        LseError::IntegrityMismatch {
            path: part.to_path_buf(),
            expected,
            actual,
            discarded: discard,
        }
    }

    /// Build a [`LseError::QuotaExceeded`] describing where the allowance stands.
    ///
    /// The export endpoints answer a `429` with no `Retry-After` and no rate-limit headers, so the
    /// position has to be fetched. If that follow-up call fails there is nothing useful to report,
    /// and the rejection is surfaced as a plain [`LseError::Api`] rather than a fabricated status.
    async fn quota_exceeded(&self) -> LseError {
        match self.usage().await {
            Ok(status) => LseError::QuotaExceeded { status },
            Err(error) => {
                warn!(
                    %error,
                    "export allowance exhausted, and the follow-up usage call failed; reporting \
                     the rejection without an allowance position"
                );
                LseError::Api {
                    status: reqwest::StatusCode::TOO_MANY_REQUESTS.as_u16(),
                    message: "export allowance exhausted; the allowance position could not be \
                              retrieved"
                        .to_owned(),
                }
            }
        }
    }
}

/// The in-progress filename for `destination`.
///
/// Appends to the whole filename rather than replacing the extension, so `x.parquet` becomes
/// `x.parquet.part` and never collides with a sibling of a different type.
fn part_path(destination: &Path) -> PathBuf {
    let mut name = destination.as_os_str().to_owned();
    name.push(".part");
    PathBuf::from(name)
}

/// Confirm a `206` resumes from exactly the offset that was requested.
///
/// A `206` starting anywhere else gets appended to the `.part` as though it continued it, leaving a
/// file that is neither the artifact nor a resumable prefix of it. The `bytes` and `sha256` checks
/// would normally catch that — but both are `Option` on the job, so a `ready` job reporting neither
/// would rename the corrupt result into place. Checking the header closes that gap where the
/// mistake is made, and says what actually went wrong instead of "digest mismatch".
///
/// A `206` carrying no parseable `Content-Range` is itself a protocol violation (RFC 9110 §15.3.7
/// requires the header), so it is surfaced rather than assumed benign.
fn verify_resume_offset(response: &reqwest::Response, expected_start: u64) -> Result<(), LseError> {
    let status = reqwest::StatusCode::PARTIAL_CONTENT.as_u16();

    let header = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| LseError::Api {
            status,
            message: "partial-content response carries no usable Content-Range header".to_owned(),
        })?;

    // `bytes <first>-<last>/<complete-length|*>`. Only `<first>` decides where these bytes belong.
    let first = header
        .trim()
        .strip_prefix("bytes ")
        .and_then(|range| range.split('-').next())
        .and_then(|first| first.trim().parse::<u64>().ok())
        .ok_or_else(|| LseError::Api {
            status,
            message: format!("unparseable Content-Range on a partial-content response: {header}"),
        })?;

    if first != expected_start {
        return Err(LseError::Api {
            status,
            message: format!(
                "partial-content response resumes at byte {first}, not the requested \
                 {expected_start}"
            ),
        });
    }

    Ok(())
}

/// Read an existing `.part` back into a hasher, returning it and the byte count.
///
/// A resumed download must hash the bytes it did not fetch, or the final digest is meaningless.
async fn hash_existing_part(part: &Path) -> Result<(Sha256, u64), LseError> {
    let mut hasher = Sha256::new();

    let mut file = match tokio::fs::File::open(part).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((hasher, 0)),
        Err(source) => {
            return Err(LseError::Io {
                message: format!("opening {}", part.display()),
                source,
            });
        }
    };

    let mut total = 0_u64;
    let mut buffer = vec![0_u8; HASH_READ_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|source| LseError::Io {
                message: format!("reading {}", part.display()),
                source,
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total += read as u64;
    }

    debug!(path = %part.display(), bytes = total, "resuming a partial export download");

    Ok((hasher, total))
}
