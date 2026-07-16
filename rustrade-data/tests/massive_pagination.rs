//! Massive REST pagination robustness tests (mocked HTTP, no live API key).
//!
//! Unlike `massive_integration.rs` (which is `#[ignore]` and hits the real Massive
//! API), these tests stand up a local `wiremock` server via
//! [`MassiveRestClient::with_base_url`] and drive the full paginated stream against
//! crafted `next_url` chains. They verify the [`PaginationGuard`] wiring end-to-end:
//!
//! - a server that keeps returning the same `next_url` is caught as a cycle
//!   (terminal `MassiveError::CyclicPagination`), and
//! - a normal finite `next_url` chain paginates to completion with no false
//!   positive.
//!
//! The numeric page cap (`MAX_PAGES`) is covered by fast, deterministic unit tests
//! in `massive::pagination`; exercising it here would mean serving thousands of
//! pages, so it is intentionally not re-tested through HTTP.

#![cfg(feature = "massive")]
// Tests use unwrap/expect for concise failure messages; panics are the intended failure mode.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{Duration, Utc};
use futures_util::StreamExt;
use rustrade_data::exchange::massive::{
    MassiveError, MassiveRestClient, OptionContractQuery, TickerQuery,
};
use std::pin::pin;
use std::sync::atomic::{AtomicU32, Ordering};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// Serves pre-configured JSON pages in registration order.
///
/// Each request advances an atomic counter and returns the next page body,
/// panicking if called more times than pages were supplied — so an unexpected
/// extra request surfaces as an explicit test failure rather than a stale reply.
struct Sequential {
    call: AtomicU32,
    pages: Vec<serde_json::Value>,
}

impl Sequential {
    fn new(pages: Vec<serde_json::Value>) -> Self {
        Self {
            call: AtomicU32::new(0),
            pages,
        }
    }
}

impl Respond for Sequential {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        let i = self.call.fetch_add(1, Ordering::Relaxed) as usize;
        let body = self.pages.get(i).unwrap_or_else(|| {
            panic!(
                "Sequential: request #{i} has no configured response (only {} page(s) supplied)",
                self.pages.len()
            )
        });
        ResponseTemplate::new(200).set_body_json(body)
    }
}

fn client_for(server: &MockServer) -> MassiveRestClient {
    MassiveRestClient::new("test_api_key")
        .expect("client builds")
        .with_base_url(server.uri())
}

/// A `next_url` that always points back to the same page must terminate the stream
/// with `CyclicPagination` rather than looping forever.
#[tokio::test]
async fn aggregates_stream_detects_next_url_cycle() {
    let server = MockServer::start().await;
    // Every page reports no bars and a `next_url` that points at a single fixed
    // page under the mock's origin — so following it revisits the same URL.
    let loop_url = format!("{}/loop", server.uri());
    let body = serde_json::json!({
        "resultsCount": 0,
        "results": [],
        "next_url": loop_url,
    });
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let client = client_for(&server);
    let to = Utc::now();
    let from = to - Duration::minutes(5);
    let mut stream = pin!(client.fetch_aggregates("X:BTCUSD", 1, "minute", from, to));

    let mut terminal_err = None;
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => continue,
            Err(e) => {
                terminal_err = Some(e);
                break;
            }
        }
    }

    assert!(
        matches!(terminal_err, Some(MassiveError::CyclicPagination { .. })),
        "expected CyclicPagination, got {terminal_err:?}"
    );
}

/// The `Vec`-returning fetches (`fetch_option_contracts` / `fetch_option_chain_snapshot`)
/// collect eagerly rather than streaming, so the guard error must propagate through the
/// plain `async fn -> Result<Vec<_>>` return path. A self-referential `next_url` must fail
/// with `CyclicPagination` instead of growing the `Vec` without bound.
#[tokio::test]
async fn option_contracts_detects_next_url_cycle() {
    let server = MockServer::start().await;
    // Empty results + a `next_url` that points back at a single fixed page under the
    // mock's origin — so following it revisits the same URL.
    let loop_url = format!("{}/loop", server.uri());
    let body = serde_json::json!({
        "results": [],
        "next_url": loop_url,
        "status": "OK",
    });
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let client = client_for(&server);
    let query = OptionContractQuery::new();
    let result = client.fetch_option_contracts(&query).await;

    assert!(
        matches!(result, Err(MassiveError::CyclicPagination { .. })),
        "expected CyclicPagination, got {result:?}"
    );
}

/// A normal finite `next_url` chain paginates to completion: every item is yielded,
/// the stream ends cleanly (no error), and the guard does not false-positive on
/// legitimately distinct page URLs.
#[tokio::test]
async fn tickers_stream_paginates_finite_chain_without_false_positive() {
    let server = MockServer::start().await;
    let page2_url = format!("{}/v3/reference/tickers?cursor=page2", server.uri());
    Mock::given(method("GET"))
        .respond_with(Sequential::new(vec![
            serde_json::json!({
                "results": [{ "ticker": "AAA" }],
                "next_url": page2_url,
                "status": "OK",
            }),
            serde_json::json!({
                "results": [{ "ticker": "BBB" }],
                "next_url": serde_json::Value::Null,
                "status": "OK",
            }),
        ]))
        .mount(&server)
        .await;

    let client = client_for(&server);
    let query = TickerQuery::new();
    let mut stream = pin!(client.fetch_tickers(&query));

    let mut tickers = Vec::new();
    while let Some(item) = stream.next().await {
        tickers.push(item.expect("no error on a well-formed finite chain"));
    }

    let symbols: Vec<_> = tickers.iter().map(|t| t.ticker.as_str()).collect();
    assert_eq!(symbols, vec!["AAA", "BBB"]);
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        2,
        "exactly two pages should have been fetched"
    );
}
