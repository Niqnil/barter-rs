//! London Strategic Edge bulk-export shape canary (network + credential gated).
//!
//! # Why this exists
//!
//! Every in-repo test for the export path runs against synthetic fixtures — `wiremock` for the job
//! lifecycle, and Parquet written by the `parquet` crate's own writer for the decoder — because the
//! provider prohibits redistributing its data (<https://londonstrategicedge.com/terms>), so no
//! recorded response and no downloaded artifact may be committed. Those fixtures validate the
//! decoder against *our own assumptions* about the wire format and the file layout. This canary is
//! what validates those assumptions against the **real API**.
//!
//! It is the only test that exercises the tick tape at all: neither REST nor the WebSocket reaches
//! it, so an export is the sole path, and the tick schema is the part most likely to drift.
//!
//! # ⚠️ This test SPENDS one of five hourly exports per run
//!
//! Unlike the vault canary, which spends only ordinary rate-limited calls, each run of this file
//! consumes **one export from the shared hourly allowance**, and the allowance is a wall-clock hour
//! bucket that resets at the top of the hour rather than rolling. Budget accordingly: five runs in
//! one clock hour exhausts it for every other consumer of the same key.
//!
//! Deliberately one export, not one per assertion — the export is the expensive part, so a single
//! artifact carries every assertion this file makes.
//!
//! # What it asserts, and why these
//!
//! - **`rows > 0`.** The single worst trap on this API surface is that a request matching nothing
//!   returns `202` → `ready` → a *valid* Parquet file with the complete schema and zero rows. No
//!   error, no warning, and it still costs an export. "Ready and it parses" is therefore not
//!   evidence of success, and a canary that only checked parseability would pass on an empty file.
//! - **The artifact decodes to two-sided quotes.** The tick schema is **dataset-dependent** — FX is
//!   `{ts, symbol, bid, ask}`, equities are `{ts, symbol, price, volume}` — and the decoder
//!   dispatches on the columns present. Asserting the *decoded event type* is what catches a
//!   schema change, since a renamed or dropped column would silently re-dispatch to a different
//!   `DataKind` (or to a typed schema error) rather than fail to parse.
//! - **Ascending timestamps, ties permitted.** The streaming backtest source delegates the ordering
//!   obligation to its source, so the decoder owns it. Ties are the common case on an equity tape,
//!   so this must not require strict ascent.
//! - **Integrity.** The download verifies the job's SHA-256 before renaming into place, so a
//!   successful download *is* the assertion — a mismatch leaves the destination absent.
//!
//! # Skip vs. fail contract
//!
//! - `LSE_API_KEY` **unset** → **SKIP** (logged, test passes), so CI without secrets stays green.
//! - `LSE_API_KEY` set but unusable → **FAIL**. A skip here would be indistinguishable from "no
//!   secrets configured", so a mistyped key would report green forever — and this canary would
//!   never once reach the provider it exists to check.
//! - Key present but the assertion fails → **FAIL** (the real signal).
//!
//! # Running
//!
//! ```bash
//! set -a && . ./.env && set +a
//! cargo test --test lse_export_canary --features lse-parquet -- --ignored --nocapture
//! ```
//!
//! Marked `#[ignore]` so a default test run never spends the allowance.

#![cfg(feature = "lse-parquet")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{Datelike, Duration as ChronoDuration, Utc, Weekday};
use rustrade_data::event::DataKind;
use rustrade_data::exchange::lse::export::{
    LseExportRange, LseExportRequest, LseExportStatus, LseExportTimeframe,
};
use rustrade_data::exchange::lse::market::LseDataset;
use rustrade_data::exchange::lse::parquet::read_export;
use rustrade_data::exchange::lse::vault::LseVaultClient;
use rustrade_instrument::instrument::InstrumentIndex;
use std::time::Duration;

const KEY_ENV: &str = "LSE_API_KEY";

/// The provider's flagship dataset, and the one whose tick schema is the two-sided quote shape.
const SYMBOL: &str = "EUR/USD";

/// How often to poll, and how long to wait before giving up. Both are caller policy by design —
/// the library takes them rather than deciding a cadence.
const POLL_INTERVAL: Duration = Duration::from_secs(3);
const POLL_TIMEOUT: Duration = Duration::from_secs(300);

/// Build a client, or `None` when the key is **absent** (skip rather than fail).
///
/// Only an unset variable skips. A key that is *set but unusable* — a stray newline from a `.env`
/// edit, a mis-encoded paste — is a misconfiguration, and reporting it as a skip would let this
/// canary pass green while never once reaching the provider. The error is safe to print:
/// [`LseError`] redacts the key from every message.
///
/// [`LseError`]: rustrade_data::exchange::lse::error::LseError
fn client() -> Option<LseVaultClient> {
    if std::env::var_os(KEY_ENV).is_none() {
        println!("CANARY_SKIP: {KEY_ENV} is not set - skipping");
        return None;
    }

    Some(
        LseVaultClient::from_env()
            .unwrap_or_else(|error| panic!("{KEY_ENV} is set but unusable: {error}")),
    )
}

/// A single recent **weekday**, well clear of today so the range is settled.
///
/// `end` is **exclusive**, so this is exactly one day of tape.
///
/// The weekday walk-back is not cosmetic. Spot FX trades Sunday 22:00 UTC to Friday 22:00 UTC, so a
/// Saturday — which is what "four days ago" is on any Wednesday run — has no tape at all. The
/// provider reports that as a *successful* zero-row export, so the `rows > 0` assertion below would
/// fail on two days in seven, after spending one of the five hourly exports on nothing. Walking
/// back costs at most two extra days of settling time and nothing else.
fn one_settled_weekday() -> LseExportRange {
    let mut start = (Utc::now() - ChronoDuration::days(4)).date_naive();
    while matches!(start.weekday(), Weekday::Sat | Weekday::Sun) {
        start -= ChronoDuration::days(1);
    }

    LseExportRange::new(start, start + ChronoDuration::days(1)).expect("a one-day range")
}

#[tokio::test]
#[ignore = "spends one of five hourly exports; run on demand"]
async fn an_fx_tick_export_round_trips_and_decodes_to_two_sided_quotes() {
    let Some(client) = client() else { return };

    let request = LseExportRequest::new(
        LseDataset::Fx,
        SYMBOL,
        LseExportTimeframe::Tick,
        one_settled_weekday(),
    )
    .expect("a valid export request");

    // From here on, one export is spent whatever happens - including on a rejection.
    let job = client
        .submit_export(&request)
        .await
        .expect("submitting the export");
    println!("CANARY: submitted job {} ({})", job.job_id, job.status);

    let status = client
        .await_export(&job.job_id, POLL_INTERVAL, POLL_TIMEOUT)
        .await
        .expect("the export job to reach a terminal state");

    assert_eq!(
        status.status,
        LseExportStatus::Ready,
        "export finished in {} (provider error: {:?})",
        status.status,
        status.error
    );

    // The silent-empty trap: a matched-nothing request is `ready` with a valid, complete-schema,
    // zero-row file. Parseability alone would not catch it.
    let rows = status.rows.expect("a ready job to report its row count");
    assert!(
        rows > 0,
        "the export is ready but contains zero rows - the request matched nothing, which the \
         provider reports as success"
    );

    let directory = tempfile::tempdir().expect("a temporary directory");
    let destination = directory.path().join("fx_tick.parquet");

    // Downloading verifies the job's SHA-256 before renaming into place, so reaching the next line
    // is itself the integrity assertion.
    let export = client
        .download_export(&status, &destination, &request)
        .await
        .expect("downloading and verifying the artifact");

    println!(
        "CANARY: downloaded {rows} rows to {} (table {:?})",
        export.path().display(),
        status.table_name
    );

    // The instrument index is irrelevant to schema validation; the decoder checks the symbol
    // column against the descriptor, which is the assertion that matters here.
    let events = read_export(&export, InstrumentIndex::new(0)).expect("the artifact to decode");

    let mut decoded = 0_u64;
    let mut previous = None;

    for event in events {
        let event = event.expect("every row to decode");

        // FX ticks are `{ts, symbol, bid, ask}`, so the decoder must dispatch to a two-sided quote.
        // A renamed or dropped column would land on a different `DataKind`, not a parse failure.
        let DataKind::OrderBookL1(book) = &event.kind else {
            panic!(
                "expected an FX tick to decode to OrderBookL1, got {:?} - the tick schema appears \
                 to have changed",
                event.kind
            );
        };
        assert!(
            book.best_bid.is_some() && book.best_ask.is_some(),
            "an FX tick decoded to a one-sided book; both bid and ask are expected on this dataset"
        );

        // Non-decreasing, not strictly ascending: ties are legitimate and common.
        if let Some(previous) = previous {
            assert!(
                event.time_exchange >= previous,
                "timestamps went backwards: {previous} then {}",
                event.time_exchange
            );
        }
        previous = Some(event.time_exchange);

        decoded += 1;
    }

    assert_eq!(
        decoded, rows,
        "decoded {decoded} events from an artifact the provider says has {rows} rows"
    );

    println!("CANARY_OK: {decoded} FX tick quotes decoded and verified");
}
