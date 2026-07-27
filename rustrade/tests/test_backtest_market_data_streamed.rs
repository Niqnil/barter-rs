#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable

//! Integration coverage for the streaming backtest market-data seam.
//!
//! These tests drive [`MarketDataStreamed`] end-to-end through the public [`backtest`] /
//! [`run_backtests`] API — the only place the *whole* failure contract is observable. The pieces
//! are unit-tested elsewhere and neither piece proves the contract on its own:
//! - `backtest::market_data::tests` proves the source resolves, caches and re-yields correctly.
//! - `backtest::tests` proves the merge records a source error in the shared slot and ends.
//!
//! What only this file proves is the join between them: that a mid-stream source failure travels
//! out of a stream consumed by a **spawned forwarding task with no return path**, survives a normal
//! shutdown, and comes back to the caller as `Err` in place of a `BacktestSummary`. That is the
//! whole point of making the stream item fallible — a truncated read must never be reported as a
//! complete run — and it is one `if let` in `backtest` away from silently regressing to a summary
//! computed over partial data.
//!
//! The happy-path test doubles as end-to-end coverage for `DefaultInstrumentMarketData` consuming
//! `DataKind::Candle`: the struct-level precedence rules are unit-tested in
//! `engine::state::instrument::data::tests`, but nothing else proves a candle actually reaches
//! instrument state through the real merge/engine path.

use std::{
    fs::File,
    io::BufReader,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic,
        aux_events::NoAuxEvents,
        backtest,
        market_data::{BacktestMarketData, MarketDataStreamed},
        run_backtests,
    },
    engine::state::{
        EngineState,
        builder::EngineStateBuilder,
        global::DefaultGlobalData,
        instrument::data::{DefaultInstrumentMarketData, InstrumentDataState},
        trading::TradingState,
    },
    error::BarterError,
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::DefaultStrategy,
    system::config::SystemConfig,
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::candle::Candle,
};
use rustrade_instrument::{
    exchange::ExchangeId, index::IndexedInstruments, instrument::InstrumentIndex,
};
use serde::Deserialize;

const CONFIG_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/config/backtest_config.json"
);

type StreamedBacktestState = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;

/// One scripted market stream, replayed verbatim by the factory on every call.
type Script = Vec<Result<MarketStreamEvent<InstrumentIndex, DataKind>, BarterError>>;
type ScriptStream = futures::stream::Iter<std::vec::IntoIter<ScriptItem>>;
type ScriptItem = Result<MarketStreamEvent<InstrumentIndex, DataKind>, BarterError>;

#[derive(Deserialize)]
struct Config {
    // `risk_free_return` is also present in the JSON but unused here; serde ignores it.
    system: SystemConfig,
}

fn load_config() -> Config {
    let reader = BufReader::new(File::open(CONFIG_PATH).expect("backtest_config.json must exist"));
    serde_json::from_reader(reader).expect("backtest_config.json must deserialize")
}

fn ts(raw: &str) -> DateTime<Utc> {
    raw.parse().unwrap()
}

/// A candle `MarketEvent` for BTCUSDT (instrument index 0), stamped at its `close_time`.
fn candle(close_time: &str, close: Decimal) -> ScriptItem {
    let close_time = ts(close_time);
    Ok(MarketStreamEvent::Item(MarketEvent {
        time_exchange: close_time,
        time_received: close_time,
        exchange: ExchangeId::BinanceSpot,
        instrument: InstrumentIndex::new(0),
        kind: DataKind::Candle(Candle {
            close_time,
            open: close,
            high: close,
            low: close,
            close,
            volume: Some(dec!(1)),
            trade_count: Some(1),
        }),
    }))
}

/// The failure a streaming source reports mid-read: a page fetch, a decode, a truncated file.
fn source_failure() -> BarterError {
    BarterError::BacktestMarketData("page 3 fetch failed".to_string())
}

/// A stream factory replaying `script`, counting how many times it is called.
///
/// Stands in for a real streaming source (a Parquet reader, a paginated provider fetch) without
/// touching the network or the filesystem — the source-agnostic half of the seam is all `backtest`
/// can observe.
fn factory(
    script: Script,
    calls: Arc<AtomicUsize>,
) -> impl Fn() -> std::future::Ready<Result<ScriptStream, BarterError>> {
    move || {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(futures::stream::iter(script.clone())))
    }
}

/// Build backtest constants around any market data source, seeding the clock from its first event.
async fn args_constant<MarketData>(
    market_data: MarketData,
) -> Arc<BacktestArgsConstant<MarketData, Daily, StreamedBacktestState, NoAuxEvents>>
where
    MarketData: BacktestMarketData<Kind = DataKind>,
{
    let Config {
        system: SystemConfig {
            instruments,
            executions,
        },
    } = load_config();

    let instruments = IndexedInstruments::new(instruments);
    let time_engine_start = market_data.time_first_event().await.unwrap();

    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(time_engine_start)
    .trading_state(TradingState::Enabled)
    .build();

    Arc::new(BacktestArgsConstant {
        instruments,
        executions,
        market_data,
        summary_interval: Daily,
        engine_state,
        aux_events: NoAuxEvents,
    })
}

fn args_dynamic(
    id: &str,
) -> BacktestArgsDynamic<
    DefaultStrategy<StreamedBacktestState>,
    DefaultRiskManager<StreamedBacktestState>,
> {
    BacktestArgsDynamic {
        id: id.into(),
        risk_free_return: dec!(0.05),
        strategy: DefaultStrategy::default(),
        risk: DefaultRiskManager::default(),
    }
}

/// The healthy path: a streamed source runs to completion and its events reach instrument state.
///
/// Asserting the terminal `data` — not merely that the run finished — is what distinguishes "the
/// stream was consumed" from "the stream was opened and dropped": a source that yielded nothing
/// would still produce a summary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streamed_source_drives_a_complete_backtest_and_reaches_instrument_state() {
    let calls = Arc::new(AtomicUsize::new(0));
    let market_data = MarketDataStreamed::<_, DataKind>::new(factory(
        vec![
            candle("2025-03-24T22:00:00Z", dec!(60_000)),
            candle("2025-03-24T22:30:00Z", dec!(60_100)),
            candle("2025-03-24T23:00:00Z", dec!(60_200)),
        ],
        Arc::clone(&calls),
    ))
    .await
    .unwrap();

    let result = backtest(args_constant(market_data).await, args_dynamic("streamed"))
        .await
        .expect("a healthy streamed source must run to completion");

    assert_eq!(result.summary.id, "streamed");

    let data = &result
        .engine_state
        .instruments
        .instrument_index(&InstrumentIndex::new(0))
        .data;

    // The last scripted candle is the one held, and `price()` resolves to its close — proving the
    // candles traversed factory -> merge -> forwarding task -> engine -> instrument state.
    assert_eq!(
        data.candle.unwrap().close_time,
        ts("2025-03-24T23:00:00Z"),
        "the most recent streamed candle must be the one retained"
    );
    assert_eq!(data.price(), Some(dec!(60_200)));
}

/// **The contract this file exists for.** A mid-stream source failure aborts the run and surfaces
/// as `Err`, rather than a `BacktestSummary` computed over the prefix that happened to be read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mid_stream_source_failure_aborts_the_backtest() {
    let calls = Arc::new(AtomicUsize::new(0));
    let market_data = MarketDataStreamed::<_, DataKind>::new(factory(
        vec![
            candle("2025-03-24T22:00:00Z", dec!(60_000)),
            candle("2025-03-24T22:30:00Z", dec!(60_100)),
            Err(source_failure()),
            candle("2025-03-24T23:00:00Z", dec!(60_200)),
        ],
        Arc::clone(&calls),
    ))
    .await
    .unwrap();

    let error = backtest(args_constant(market_data).await, args_dynamic("truncated"))
        .await
        .expect_err("a mid-stream source failure must abort the run, not summarise a prefix");

    assert_eq!(error, source_failure());
}

/// The abort propagates out of the concurrent sweep too — `run_backtests` must not return a
/// `MultiBacktestSummary` in which one run silently covered less data than the others.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mid_stream_source_failure_aborts_a_run_backtests_sweep() {
    let calls = Arc::new(AtomicUsize::new(0));
    let market_data = MarketDataStreamed::<_, DataKind>::new(factory(
        vec![
            candle("2025-03-24T22:00:00Z", dec!(60_000)),
            Err(source_failure()),
        ],
        Arc::clone(&calls),
    ))
    .await
    .unwrap();

    let error = run_backtests(
        args_constant(market_data).await,
        [args_dynamic("sweep-a"), args_dynamic("sweep-b")],
    )
    .await
    .expect_err("a failing source must fail the whole sweep");

    assert_eq!(error, source_failure());
}

/// The documented 1 + N cost model, measured through the real `backtest` path rather than by
/// calling `stream()` directly: construction reads once, and every run reads again.
///
/// Also covers the per-run error slot: the second run succeeds on an `args_constant` a first run
/// has already consumed, which a slot shared across runs (rather than created inside `backtest`)
/// would not guarantee.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_backtest_re_reads_the_source() {
    let calls = Arc::new(AtomicUsize::new(0));
    let market_data = MarketDataStreamed::<_, DataKind>::new(factory(
        vec![candle("2025-03-24T22:00:00Z", dec!(60_000))],
        Arc::clone(&calls),
    ))
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1, "construction reads once");

    let args_constant = args_constant(market_data).await;
    backtest(Arc::clone(&args_constant), args_dynamic("first"))
        .await
        .unwrap();
    backtest(args_constant, args_dynamic("second"))
        .await
        .unwrap();

    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "1 construction + 1 read per backtest - the documented 1 + N cost of a streamed source"
    );
}
