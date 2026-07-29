//! Export-job lifecycle tests for the London Strategic Edge vault.
//!
//! Every response here is **synthetic**, shaped from measurements of the live API. No provider
//! data is committed — the provider prohibits redistribution
//! (<https://londonstrategicedge.com/terms>), so `wiremock` is the only in-repo option.
//!
//! The measurements these encode, and which a change to `export.rs` must not regress:
//!
//! - Submit answers `202`, not `200`.
//! - The job identifier is `job_id` on submit and `id` on status.
//! - A `429` from an export endpoint means the allowance is exhausted, not a per-minute rate
//!   limit, and carries no `Retry-After`.
//! - The artifact download honours `Range` with a `206`, and the job record carries a `sha256`.
//! - A rejected submit costs an export, so anything checkable is rejected before it is sent.
//!
//! Run with: `cargo test --test lse_export --features lse`

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use rustrade_data::exchange::lse::error::LseError;
use rustrade_data::exchange::lse::export::{
    LseExportJobStatus, LseExportRange, LseExportRequest, LseExportStatus, LseExportTimeframe,
};
use rustrade_data::exchange::lse::market::LseDataset;
use rustrade_data::exchange::lse::vault::LseVaultClient;
use rustrade_data::subscription::candle::CandleInterval;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::time::Duration;
use wiremock::matchers::{body_json_string, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(server: &MockServer) -> LseVaultClient {
    LseVaultClient::new("test-key")
        .unwrap()
        .with_base_url(format!("{}/vault", server.uri()))
        .with_pace(Duration::ZERO)
}

fn range() -> LseExportRange {
    LseExportRange::new("2026-07-01".parse().unwrap(), "2026-07-02".parse().unwrap()).unwrap()
}

fn tick_request() -> LseExportRequest {
    LseExportRequest::new(LseDataset::Fx, "EUR/USD", LseExportTimeframe::Tick, range()).unwrap()
}

/// The measured submit response: `202`, `job_id`, `status: queued`.
fn submit_body(job_id: &str) -> String {
    format!(
        r#"{{"job_id":"{job_id}","status":"queued","dataset":"fx","symbol":"EUR/USD",
            "timeframe":"tick","start":"2026-07-01","end":"2026-07-02","format":"parquet",
            "est_bytes":7111419512,"poll":"/export/{job_id}"}}"#
    )
}

/// The measured ready-job record, including the `sha256` this integration verifies against.
fn ready_body(job_id: &str, payload: &[u8]) -> String {
    let digest = hex::encode(Sha256::digest(payload));
    format!(
        r#"{{"id":"{job_id}","status":"ready","dataset":"fx","table_name":"ticks_fx",
            "symbol":"EUR/USD","start":"2026-07-01","end":"2026-07-02","format":"parquet",
            "rows":64958,"bytes":{},"sha256":"{digest}","error":null,
            "created_at":"2026-07-28T02:06:49.072387+00:00",
            "updated_at":"2026-07-28T02:07:44.862841+00:00",
            "expires_at":"2026-07-30T02:07:44.862806+00:00","timeframe":"tick",
            "download_url":"/export/{job_id}/download"}}"#,
        payload.len()
    )
}

/// A `ready` job reporting neither `bytes` nor `sha256`.
///
/// Both fields are independent `Option`s, and every measured `ready` job carried both — but the
/// provider makes no guarantee, and the download path deliberately accepts an artifact it cannot
/// fully verify rather than refusing one the provider calls ready. These are the tests for what
/// that concession costs.
fn job_without_integrity_metadata(job_id: &str) -> LseExportJobStatus {
    serde_json::from_str(&format!(
        r#"{{"id":"{job_id}","status":"ready","symbol":"EUR/USD","format":"parquet"}}"#
    ))
    .unwrap()
}

#[tokio::test]
async fn submit_accepts_the_202_the_provider_actually_returns() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/vault/export"))
        .respond_with(ResponseTemplate::new(202).set_body_string(submit_body("abc123")))
        .mount(&server)
        .await;

    let job = client(&server)
        .submit_export(&tick_request())
        .await
        .unwrap();

    assert_eq!(job.job_id, "abc123");
    assert_eq!(job.status, LseExportStatus::Queued);
}

#[tokio::test]
async fn submit_sends_the_measured_payload_shape() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/vault/export"))
        .and(header("x-api-key", "test-key"))
        .and(body_json_string(
            r#"{"dataset":"fx","symbol":"EUR/USD","timeframe":"tick","start":"2026-07-01","end":"2026-07-02","format":"parquet"}"#,
        ))
        .respond_with(ResponseTemplate::new(202).set_body_string(submit_body("abc123")))
        .mount(&server)
        .await;

    assert!(client(&server).submit_export(&tick_request()).await.is_ok());
}

#[tokio::test]
async fn the_job_id_is_read_from_job_id_on_submit_and_id_on_status() {
    // The provider renames the field between the two responses. One shared struct silently fails
    // to deserialise, which is why these are two types.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/vault/export"))
        .respond_with(ResponseTemplate::new(202).set_body_string(submit_body("renamed")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/vault/export/renamed"))
        .respond_with(ResponseTemplate::new(200).set_body_string(ready_body("renamed", b"data")))
        .mount(&server)
        .await;

    let client = client(&server);
    let job = client.submit_export(&tick_request()).await.unwrap();
    let status = client.export_status(&job.job_id).await.unwrap();

    assert_eq!(job.job_id, status.id);
}

#[tokio::test]
async fn an_exhausted_allowance_is_quota_exceeded_not_rate_limited() {
    // The measured rejection: 429, single-encoded detail, NO Retry-After. It is a different
    // condition from a per-minute rate limit, and only this one carries an allowance position.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/vault/export"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_string(r#"{"detail":"too many export requests; try again shortly"}"#),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/vault/usage"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"bytes_used_month":1,"bytes_cap_month":2,"bytes_used_week":1,"bytes_cap_week":2,
                "exports_this_hour":5,"exports_cap_hour":5,"historical_data_months":-1,
                "calls_per_minute":200,"max_rows_per_request":5000,"vault_concurrency":2}"#,
        ))
        .mount(&server)
        .await;

    let error = client(&server)
        .submit_export(&tick_request())
        .await
        .unwrap_err();

    let LseError::QuotaExceeded { status } = error else {
        panic!("expected QuotaExceeded, got {error:?}");
    };
    assert_eq!(status.exports_this_hour, 5);
    assert_eq!(status.exports_cap_hour, 5);
}

#[tokio::test]
async fn a_429_whose_usage_lookup_fails_still_surfaces_observably() {
    // No fabricated allowance position: if the follow-up call fails there is nothing truthful to
    // report, so the rejection degrades to a plain API error rather than an invented status.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/vault/export"))
        .respond_with(ResponseTemplate::new(429))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/vault/usage"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    assert!(matches!(
        client(&server).submit_export(&tick_request()).await,
        Err(LseError::Api { status: 429, .. })
    ));
}

#[tokio::test]
async fn await_export_returns_the_ready_job() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(ready_body("job1", b"payload")))
        .mount(&server)
        .await;

    let status = client(&server)
        .await_export("job1", Duration::ZERO, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(status.status, LseExportStatus::Ready);
    assert_eq!(status.rows, Some(64958));
    assert_eq!(status.table_name.as_deref(), Some("ticks_fx"));
    assert!(status.expires_at.is_some());
}

#[tokio::test]
async fn a_failed_job_is_terminal_and_carries_the_providers_diagnostic() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"id":"job1","status":"failed","error":"upstream table unavailable"}"#,
        ))
        .mount(&server)
        .await;

    let error = client(&server)
        .await_export("job1", Duration::ZERO, Duration::from_secs(5))
        .await
        .unwrap_err();

    assert!(matches!(error, LseError::ExportFailed { .. }));
    assert!(error.to_string().contains("upstream table unavailable"));
}

#[tokio::test]
async fn a_job_that_never_readies_times_out_without_cancelling_it() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"id":"job1","status":"queued"}"#),
        )
        .mount(&server)
        .await;

    let error = client(&server)
        .await_export("job1", Duration::ZERO, Duration::ZERO)
        .await
        .unwrap_err();

    assert!(matches!(error, LseError::ExportTimeout { .. }));
    // The message must steer the caller to re-poll rather than re-export, which costs allowance.
    assert!(error.to_string().contains("poll it again"));
}

#[tokio::test]
async fn an_unknown_status_is_preserved_rather_than_guessed_at() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"id":"job1","status":"materialising"}"#),
        )
        .mount(&server)
        .await;

    let status = client(&server).export_status("job1").await.unwrap();

    assert_eq!(
        status.status,
        LseExportStatus::Other("materialising".to_owned())
    );
    assert!(!status.status.is_terminal());
}

#[tokio::test]
async fn download_verifies_the_sha256_and_renames_atomically() {
    let payload = b"synthetic parquet bytes";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let client = client(&server);
    let job = serde_json::from_str(&ready_body("job1", payload)).unwrap();

    let export = client
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap();

    assert_eq!(export.path(), destination);
    assert_eq!(export.dataset(), LseDataset::Fx);
    assert_eq!(export.exchange_id(), LseDataset::Fx.exchange_id());
    assert_eq!(std::fs::read(&destination).unwrap(), payload);
    // The in-progress file is gone, so nothing is left to confuse a later resume.
    assert!(!destination.with_extension("parquet.part").exists());
}

#[tokio::test]
async fn a_corrupt_download_keeps_the_partial_file_and_leaves_the_destination_absent() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"wrong bytes".to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let job = serde_json::from_str(&ready_body("job1", b"the real payload")).unwrap();

    let error = client(&server)
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        LseError::IntegrityMismatch {
            discarded: false,
            ..
        }
    ));
    // A corrupt artifact must never appear at the destination path.
    assert!(!destination.exists());
    // This call fetched the bytes, so they are a real prefix and the next call can resume them.
    assert!(destination.with_extension("parquet.part").exists());
}

#[tokio::test]
async fn a_stale_oversized_partial_file_is_discarded_rather_than_failing_forever() {
    // A `.part` left by a DIFFERENT, larger job at the same destination looks complete, so no
    // transfer is attempted — and it then fails verification. Keeping it would take exactly this
    // branch on every retry, so the documented "re-call to resume" would never converge.
    let payload = b"the real payload";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let mut part = destination.clone().into_os_string();
    part.push(".part");
    std::fs::write(
        &part,
        b"leftovers from a larger job that used this same destination",
    )
    .unwrap();

    let job: LseExportJobStatus = serde_json::from_str(&ready_body("job1", payload)).unwrap();
    let client = client(&server);

    let error = client
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        LseError::IntegrityMismatch {
            discarded: true,
            ..
        }
    ));
    assert!(!destination.exists());
    // Discarded, not kept: that is what lets the retry below make progress.
    assert!(!PathBuf::from(&part).exists());
    // Nothing was requested, because the stale file looked complete.
    assert!(server.received_requests().await.unwrap().is_empty());

    // The same call now succeeds instead of failing identically forever.
    client
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap();

    assert_eq!(std::fs::read(&destination).unwrap(), payload);
}

#[tokio::test]
async fn an_interrupted_download_resumes_from_the_partial_file() {
    // The provider advertises `Accept-Ranges: bytes` and answers `206`, so resume is real. The
    // hash must cover the prefix that was never re-fetched, or verification is meaningless.
    let payload = b"0123456789abcdef";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .and(header("range", "bytes=10-"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 10-15/16")
                .set_body_bytes(payload[10..].to_vec()),
        )
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let mut part = destination.clone().into_os_string();
    part.push(".part");
    std::fs::write(&part, &payload[..10]).unwrap();

    let job = serde_json::from_str(&ready_body("job1", payload)).unwrap();
    client(&server)
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap();

    assert_eq!(std::fs::read(&destination).unwrap(), payload);
}

#[tokio::test]
async fn a_server_that_ignores_range_restarts_the_download_instead_of_appending() {
    // A server is entitled to answer a `Range` request with `200` and the whole artifact. Appending
    // that to the existing prefix would double it, so the transfer must restart from zero -- hasher
    // included, or the digest would cover the duplicated bytes and reject a correct download.
    let payload = b"0123456789abcdef";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .and(header("range", "bytes=10-"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let mut part = destination.clone().into_os_string();
    part.push(".part");
    std::fs::write(&part, &payload[..10]).unwrap();

    let job = serde_json::from_str(&ready_body("job1", payload)).unwrap();
    client(&server)
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap();

    // Exactly the artifact -- not the prefix twice over.
    assert_eq!(std::fs::read(&destination).unwrap(), payload);
    assert!(!PathBuf::from(&part).exists());
}

#[tokio::test]
async fn a_206_resuming_from_the_wrong_offset_is_rejected_before_it_reaches_the_file() {
    // The `bytes`/`sha256` checks would catch this eventually, but both are optional on the job, so
    // a `ready` job reporting neither would rename a corrupt file into place. The `Content-Range`
    // says what actually went wrong, at the point it went wrong.
    let payload = b"0123456789abcdef";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .and(header("range", "bytes=10-"))
        .respond_with(
            // Asked for byte 10, answered from byte 4.
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 4-15/16")
                .set_body_bytes(payload[4..].to_vec()),
        )
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let mut part = destination.clone().into_os_string();
    part.push(".part");
    std::fs::write(&part, &payload[..10]).unwrap();

    let error = client(&server)
        .download_export(
            &job_without_integrity_metadata("job1"),
            &destination,
            &tick_request(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, LseError::Api { status: 206, .. }));
    assert!(!destination.exists());
    // The prefix is untouched, so a later correct `206` still resumes from it.
    assert_eq!(std::fs::read(&part).unwrap(), &payload[..10]);
}

#[tokio::test]
async fn a_206_with_no_usable_content_range_is_rejected_rather_than_assumed_to_resume() {
    // The other two arms of the same seam check. RFC 9110 §15.3.7 requires `Content-Range` on a
    // `206`, so absent or unparseable is a protocol violation -- and treating either as benign would
    // append bytes at an offset the server never claimed, which on a job reporting neither `bytes`
    // nor `sha256` nothing downstream would catch.
    let payload = b"0123456789abcdef";

    for content_range in [None, Some("bytes abc-15/16")] {
        let server = MockServer::start().await;
        let mut response = ResponseTemplate::new(206).set_body_bytes(payload[10..].to_vec());
        if let Some(value) = content_range {
            response = response.insert_header("content-range", value);
        }
        Mock::given(method("GET"))
            .and(path("/vault/export/job1/download"))
            .and(header("range", "bytes=10-"))
            .respond_with(response)
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("out.parquet");
        let mut part = destination.clone().into_os_string();
        part.push(".part");
        std::fs::write(&part, &payload[..10]).unwrap();

        let error = client(&server)
            .download_export(
                &job_without_integrity_metadata("job1"),
                &destination,
                &tick_request(),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, LseError::Api { status: 206, .. }),
            "Content-Range {content_range:?} should be rejected, got {error:?}"
        );
        assert!(!destination.exists());
        // Kept, not discarded: this call appended nothing, but the prefix is still a real one that a
        // correct `206` can resume from -- unlike the `416` case, where the file is provably not
        // this job's.
        assert_eq!(std::fs::read(&part).unwrap(), &payload[..10]);
    }
}

#[tokio::test]
async fn a_416_on_an_already_complete_part_file_converges_instead_of_failing_forever() {
    // A run interrupted between the final write and the rename leaves a `.part` holding the whole
    // artifact. Without `bytes` on the job there is no way to know that up front, so the resume is
    // attempted and earns a spec-compliant `416`. Treating that as an error would make every retry
    // fail identically, contradicting the documented "re-calling resumes".
    let payload = b"0123456789abcdef";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .and(header("range", "bytes=16-"))
        .respond_with(ResponseTemplate::new(416))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let mut part = destination.clone().into_os_string();
    part.push(".part");
    std::fs::write(&part, payload).unwrap();

    let export = client(&server)
        .download_export(
            &job_without_integrity_metadata("job1"),
            &destination,
            &tick_request(),
        )
        .await
        .unwrap();

    assert_eq!(export.path(), destination);
    assert_eq!(std::fs::read(&destination).unwrap(), payload);
    assert!(!PathBuf::from(&part).exists());
}

#[tokio::test]
async fn a_416_on_a_part_file_that_fails_verification_discards_it_and_converges() {
    // The other half of the `416` story. A `.part` left by a DIFFERENT job can sit at or past this
    // artifact's length, so the resume earns a `416` — but those bytes are not this job's. Unlike
    // the stale-oversized case, a request *was* sent and answered here, and the file still has to
    // go: keeping it would earn the same `416` and fail the same hash on every retry.
    //
    // Reaching this needs `sha256` without `bytes`. With `bytes` present the length check would
    // have declared the file complete before any request, taking the no-transfer path instead — so
    // this combination is the only door to a discard decision made *after* talking to the server.
    let payload = b"0123456789abcdef";
    // Same length as the artifact, so the resume offset lands exactly on its end and earns a `416`.
    let stale = b"leftovers-from-a";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .and(header("range", "bytes=16-"))
        .respond_with(ResponseTemplate::new(416))
        .mount(&server)
        .await;
    // The retry after the discard starts from scratch, so it carries no `Range` and falls through
    // to this one.
    Mock::given(method("GET"))
        .and(path("/vault/export/job1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out.parquet");
    let mut part = destination.clone().into_os_string();
    part.push(".part");
    std::fs::write(&part, stale).unwrap();

    let job: LseExportJobStatus = serde_json::from_str(&format!(
        r#"{{"id":"job1","status":"ready","symbol":"EUR/USD","format":"parquet",
            "sha256":"{}"}}"#,
        hex::encode(Sha256::digest(payload))
    ))
    .unwrap();
    let client = client(&server);

    let error = client
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        LseError::IntegrityMismatch {
            discarded: true,
            ..
        }
    ));
    assert!(!destination.exists());
    // Discarded, not kept: that is what lets the retry below make progress.
    assert!(!PathBuf::from(&part).exists());
    // Unlike the stale-oversized discard, this one did reach the server — which is why the warning
    // it logs must not claim the file went unfetched.
    assert_eq!(server.received_requests().await.unwrap().len(), 1);

    // The same call now succeeds instead of failing identically forever.
    client
        .download_export(&job, &destination, &tick_request())
        .await
        .unwrap();

    assert_eq!(std::fs::read(&destination).unwrap(), payload);
    assert!(!PathBuf::from(&part).exists());
}

#[tokio::test]
async fn downloading_a_job_that_is_not_ready_is_rejected_before_a_request_is_sent() {
    let server = MockServer::start().await;
    let job = serde_json::from_str(r#"{"id":"job1","status":"queued"}"#).unwrap();

    let error = client(&server)
        .download_export(&job, "/dev/null/nope", &tick_request())
        .await
        .unwrap_err();

    assert!(matches!(error, LseError::InvalidInput { .. }));
    // Nothing was requested: the guard is client-side.
    assert!(server.received_requests().await.unwrap().is_empty());
}

// ── pre-flight validation: every one of these would otherwise cost a real export ──

#[test]
fn a_candle_export_naming_all_is_rejected_because_it_silently_returns_nothing() {
    // Measured: `symbol: "all"` returns 202 -> ready -> a valid Parquet file with the full schema
    // and ZERO rows, with no error, and consumes an export.
    let error = LseExportRequest::new(
        LseDataset::Etf,
        "all",
        LseExportTimeframe::Candle(CandleInterval::Day1),
        range(),
    )
    .unwrap_err();

    assert!(matches!(error, LseError::InvalidInput { .. }));
    assert!(error.to_string().contains("EMPTY"));
}

#[test]
fn a_tick_export_naming_all_is_rejected_too_because_it_fails_the_same_way() {
    // Measured on the tick path as well: 202 -> ready -> zero rows. Combined with a missing symbol
    // being a hard 400, no spelling reaches more than one symbol, so every artifact is
    // single-symbol and this literal is never the right request on either path.
    let error = LseExportRequest::new(LseDataset::Fx, "all", LseExportTimeframe::Tick, range())
        .unwrap_err();

    assert!(matches!(error, LseError::InvalidInput { .. }));
    assert!(error.to_string().contains("EMPTY"));
}

#[test]
fn uppercase_all_is_accepted_because_it_is_allstates_real_ticker() {
    // Verified live against the provider's price endpoint: `ALL` is a real, priced symbol. A
    // case-insensitive guard here would forbid a correct export with no escape hatch, which is the
    // failure mode a blanket safety rule is supposed to avoid. "Matches nothing" is a property of
    // the (dataset, symbol) pair, not of the string.
    assert!(
        LseExportRequest::new(
            LseDataset::Stocks,
            "ALL",
            LseExportTimeframe::Candle(CandleInterval::Day1),
            range(),
        )
        .is_ok()
    );
}

#[test]
fn a_candle_export_of_a_tick_only_dataset_is_rejected() {
    // These serve candles over REST but are tick-only on the export path.
    for dataset in [
        LseDataset::Volatility,
        LseDataset::InterestRates,
        LseDataset::CurrencyIndex,
    ] {
        let error = LseExportRequest::new(
            dataset,
            "VIX/USD",
            LseExportTimeframe::Candle(CandleInterval::Day1),
            range(),
        )
        .unwrap_err();

        assert!(
            matches!(error, LseError::InvalidInput { .. }),
            "{dataset:?} should be rejected"
        );
        assert!(error.to_string().contains("tick-only"));
    }

    // The same datasets are exportable as ticks.
    assert!(
        LseExportRequest::new(
            LseDataset::Volatility,
            "VIX/USD",
            LseExportTimeframe::Tick,
            range()
        )
        .is_ok()
    );
}

#[test]
fn a_resolution_the_provider_does_not_serve_is_rejected_before_it_is_billed() {
    let error = LseExportRequest::new(
        LseDataset::Etf,
        "SPY",
        LseExportTimeframe::Candle(CandleInterval::Hour2),
        range(),
    )
    .unwrap_err();

    assert!(matches!(error, LseError::UnsupportedInterval { .. }));
}

#[test]
fn a_blank_symbol_is_rejected_rather_than_earning_a_400() {
    // Measured: omitting `symbol` returns `400 invalid or missing symbol` -- and still consumes an
    // export.
    for symbol in ["", "   "] {
        assert!(matches!(
            LseExportRequest::new(LseDataset::Fx, symbol, LseExportTimeframe::Tick, range()),
            Err(LseError::InvalidInput { .. })
        ));
    }
}

#[test]
fn an_inverted_or_empty_range_is_rejected() {
    let day = "2026-07-01".parse().unwrap();
    let earlier = "2026-06-01".parse().unwrap();

    assert!(matches!(
        LseExportRange::new(day, day),
        Err(LseError::InvalidInput { .. })
    ));
    assert!(matches!(
        LseExportRange::new(day, earlier),
        Err(LseError::InvalidInput { .. })
    ));
}

#[test]
fn the_export_range_end_is_documented_and_carried_as_exclusive() {
    let range = range();

    assert_eq!(range.start().to_string(), "2026-07-01");
    assert_eq!(range.end().to_string(), "2026-07-02");
}
