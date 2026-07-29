//! Decoder tests for London Strategic Edge bulk-export artifacts.
//!
//! Every fixture here is **written by this test file** using the `parquet` crate's own writer,
//! shaped from measurements of the live API. No provider data is committed — the provider
//! prohibits redistribution (<https://londonstrategicedge.com/terms>).
//!
//! The measured facts these encode:
//!
//! - The tick schema **varies by dataset**: `fx` is `{ts, symbol, bid, ask}`, `stocks` is
//!   `{ts, symbol, price, volume}`, and the synthetic classes are `{ts, symbol, price, volume,
//!   ask}` with `volume`/`ask` nullable.
//! - The candle schema varies too: `etf` carries `volume`, `fx` omits the column entirely.
//! - `price` is the **bid** (the provider's price endpoint returns `price == bid` on every symbol
//!   tested), so `price` beside an `ask` is a quote.
//! - `ts` is the bar's **open** time, and timestamps are **non-decreasing**, not strictly
//!   ascending.
//!
//! Run with: `cargo test --test lse_parquet --features lse-parquet`

#![cfg(feature = "lse-parquet")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rust_decimal_macros::dec;
use rustrade_data::event::DataKind;
use rustrade_data::exchange::lse::error::LseError;
use rustrade_data::exchange::lse::export::{LseExport, LseExportRange, LseExportTimeframe};
use rustrade_data::exchange::lse::market::LseDataset;
use rustrade_data::exchange::lse::parquet::{instrument_index_for, read_export, symbols_in_export};
use rustrade_data::subscription::candle::CandleInterval;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_instrument::index::builder::IndexedInstrumentsBuilder;
use rustrade_instrument::instrument::InstrumentIndex;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A column of values to write: `None` entries become SQL nulls (OPTIONAL columns only).
enum Col {
    Ts(Vec<i64>),
    Sym(Vec<&'static str>),
    Dbl(Vec<f64>),
    OptDbl(Vec<Option<f64>>),
}

/// Write a Parquet file with the given schema and columns.
fn write_parquet(path: &Path, message: &str, columns: Vec<Col>) {
    let schema = Arc::new(parse_message_type(message).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(File::create(path).unwrap(), schema, props).unwrap();
    let mut group = writer.next_row_group().unwrap();

    for column in columns {
        let mut col = group.next_column().unwrap().unwrap();
        match column {
            Col::Ts(values) => {
                col.typed::<Int64Type>()
                    .write_batch(&values, None, None)
                    .unwrap();
            }
            Col::Sym(values) => {
                let values: Vec<ByteArray> = values.iter().map(|s| (*s).into()).collect();
                col.typed::<ByteArrayType>()
                    .write_batch(&values, None, None)
                    .unwrap();
            }
            Col::Dbl(values) => {
                col.typed::<DoubleType>()
                    .write_batch(&values, None, None)
                    .unwrap();
            }
            Col::OptDbl(values) => {
                // Top-level OPTIONAL: definition level 1 means present, 0 means null, and only
                // the present values are supplied.
                let def: Vec<i16> = values.iter().map(|v| i16::from(v.is_some())).collect();
                let present: Vec<f64> = values.iter().flatten().copied().collect();
                col.typed::<DoubleType>()
                    .write_batch(&present, Some(&def), None)
                    .unwrap();
            }
        }
        col.close().unwrap();
    }

    group.close().unwrap();
    writer.close().unwrap();
}

fn dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

fn range() -> LseExportRange {
    LseExportRange::new("2026-07-01".parse().unwrap(), "2026-07-03".parse().unwrap()).unwrap()
}

/// 2026-07-01T00:00:00Z in microseconds — the first day of [`range`].
const T0: i64 = 1_782_864_000_000_000;
const HOUR: i64 = 3_600_000_000;
const DAY: i64 = 86_400_000_000;

fn export(path: PathBuf, dataset: LseDataset, symbol: &str, tf: LseExportTimeframe) -> LseExport {
    LseExport::new(path, dataset, symbol, tf, range())
}

fn idx() -> InstrumentIndex {
    InstrumentIndex::new(0)
}

// ── the three measured tick layouts ──────────────────────────────────────────

const FX_TICK: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE bid;
  REQUIRED DOUBLE ask;
}";

const STOCK_TICK: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE price;
  REQUIRED DOUBLE volume;
}";

const SYNTH_TICK: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE price;
  OPTIONAL DOUBLE volume;
  OPTIONAL DOUBLE ask;
}";

const CANDLE_WITH_VOLUME: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE open;
  REQUIRED DOUBLE high;
  REQUIRED DOUBLE low;
  REQUIRED DOUBLE close;
  REQUIRED DOUBLE volume;
}";

const CANDLE_NO_VOLUME: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE open;
  REQUIRED DOUBLE high;
  REQUIRED DOUBLE low;
  REQUIRED DOUBLE close;
}";

#[test]
fn an_fx_tick_export_decodes_to_a_two_sided_quote() {
    let dir = dir();
    let path = dir.path().join("fx.parquet");
    write_parquet(
        &path,
        FX_TICK,
        vec![
            Col::Ts(vec![T0, T0 + HOUR]),
            Col::Sym(vec!["EUR/USD", "EUR/USD"]),
            Col::Dbl(vec![1.14126, 1.14130]),
            Col::Dbl(vec![1.14135, 1.14138]),
        ],
    );

    let export = export(path, LseDataset::Fx, "EUR/USD", LseExportTimeframe::Tick);
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].exchange, ExchangeId::LseFx);
    let DataKind::OrderBookL1(book) = &events[0].kind else {
        panic!("expected OrderBookL1, got {:?}", events[0].kind);
    };
    assert_eq!(book.best_bid.unwrap().price, dec!(1.14126));
    assert_eq!(book.best_ask.unwrap().price, dec!(1.14135));
}

#[test]
fn a_stocks_tick_export_decodes_to_a_trade() {
    let dir = dir();
    let path = dir.path().join("stocks.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![289.75]),
            Col::Dbl(vec![100.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(events[0].exchange, ExchangeId::LseEquities);
    let DataKind::Trade(trade) = &events[0].kind else {
        panic!("expected Trade, got {:?}", events[0].kind);
    };
    assert_eq!(trade.price, dec!(289.75));
    assert_eq!(trade.amount, dec!(100));
    // No aggressor side is published, and a bid-side price cannot imply one.
    assert!(trade.side.is_none());
    // No trade identifier exists on the tape; timestamps tie, so one cannot be synthesised.
    assert!(trade.id.is_empty());
}

#[test]
fn a_synth_tick_export_decodes_price_as_the_bid() {
    // `price` sits beside an `ask` and the provider's price endpoint reports `price == bid`, so
    // this layout is a quote despite the column name.
    let dir = dir();
    let path = dir.path().join("synth.parquet");
    write_parquet(
        &path,
        SYNTH_TICK,
        vec![
            Col::Ts(vec![T0, T0 + HOUR]),
            Col::Sym(vec!["VIX/USD", "VIX/USD"]),
            Col::Dbl(vec![16.92, 16.95]),
            Col::OptDbl(vec![Some(0.0), None]),
            Col::OptDbl(vec![Some(16.93), None]),
        ],
    );

    let export = export(
        path,
        LseDataset::Volatility,
        "VIX/USD",
        LseExportTimeframe::Tick,
    );
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(events[0].exchange, ExchangeId::LseCfd);
    let DataKind::OrderBookL1(book) = &events[0].kind else {
        panic!("expected OrderBookL1");
    };
    assert_eq!(book.best_bid.unwrap().price, dec!(16.92));
    assert_eq!(book.best_ask.unwrap().price, dec!(16.93));

    // A null ask yields a one-sided book rather than a fabricated price.
    let DataKind::OrderBookL1(book) = &events[1].kind else {
        panic!("expected OrderBookL1");
    };
    assert_eq!(book.best_bid.unwrap().price, dec!(16.95));
    assert!(book.best_ask.is_none());
}

// ── candles ──────────────────────────────────────────────────────────────────

#[test]
fn a_candle_export_derives_close_time_from_the_bars_open_time() {
    // `ts` is the bar's OPEN time; the library's contract is an exclusive `close_time`, so passing
    // `ts` through would shift every bar by one period.
    let dir = dir();
    let path = dir.path().join("etf.parquet");
    write_parquet(
        &path,
        CANDLE_WITH_VOLUME,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["SPY"]),
            Col::Dbl(vec![746.77]),
            Col::Dbl(vec![749.44]),
            Col::Dbl(vec![742.39]),
            Col::Dbl(vec![744.97]),
            Col::Dbl(vec![110173547528.0]),
        ],
    );

    let export = export(
        path,
        LseDataset::Etf,
        "SPY",
        LseExportTimeframe::Candle(CandleInterval::Day1),
    );
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    let DataKind::Candle(candle) = &events[0].kind else {
        panic!("expected Candle");
    };
    // The `ts` column held 2026-07-01T00:00Z, so the exclusive close is the next midnight.
    assert_eq!(candle.close_time.to_rfc3339(), "2026-07-02T00:00:00+00:00");
    assert_eq!(candle.open, dec!(746.77));
    assert_eq!(candle.close, dec!(744.97));
    assert_eq!(candle.volume, Some(dec!(110173547528)));
    // Never published on any dataset.
    assert!(candle.trade_count.is_none());
    // `time_exchange` is the close, NOT the `ts` it was read from: a bar enters the timeline when
    // its period ends. Stamping the open would be silent lookahead, and would put this decoder out
    // of step with the candle replay path it is merged against.
    assert_eq!(events[0].time_exchange, candle.close_time);
    assert_eq!(events[0].time_received, candle.close_time);
}

#[test]
fn an_fx_candle_export_omitting_the_volume_column_yields_none() {
    // Measured: `candles_fx_1d` has SIX columns. A fixed-arity schema assert would reject the
    // provider's flagship dataset outright.
    let dir = dir();
    let path = dir.path().join("fxcandle.parquet");
    write_parquet(
        &path,
        CANDLE_NO_VOLUME,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["EUR/USD"]),
            Col::Dbl(vec![1.14126]),
            Col::Dbl(vec![1.14137]),
            Col::Dbl(vec![1.13614]),
            Col::Dbl(vec![1.13772]),
        ],
    );

    let export = export(
        path,
        LseDataset::Fx,
        "EUR/USD",
        LseExportTimeframe::Candle(CandleInterval::Day1),
    );
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    let DataKind::Candle(candle) = &events[0].kind else {
        panic!("expected Candle");
    };
    assert_eq!(candle.volume, None);
}

#[test]
fn a_literal_zero_volume_is_passed_through_rather_than_rewritten_to_none() {
    // The provider reports 0 for a majority of one-minute equity bars that demonstrably had
    // trades. Rewriting that to `None` would be this library inventing a fact; `None` is reserved
    // for a column the provider does not publish at all.
    let dir = dir();
    let path = dir.path().join("zerovol.parquet");
    write_parquet(
        &path,
        CANDLE_WITH_VOLUME,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![289.0]),
            Col::Dbl(vec![290.0]),
            Col::Dbl(vec![288.0]),
            Col::Dbl(vec![289.5]),
            Col::Dbl(vec![0.0]),
        ],
    );

    let export = export(
        path,
        LseDataset::Stocks,
        "AAPL",
        LseExportTimeframe::Candle(CandleInterval::Min1),
    );
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    let DataKind::Candle(candle) = &events[0].kind else {
        panic!("expected Candle");
    };
    assert_eq!(candle.volume, Some(dec!(0)));
}

// ── invariants ───────────────────────────────────────────────────────────────

#[test]
fn tied_timestamps_are_accepted_because_they_are_the_common_case() {
    // 68% of adjacent rows tie on an equity tape. A strict-ascent assert would reject valid files.
    let dir = dir();
    let path = dir.path().join("ties.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0, T0, T0, T0 + HOUR]),
            Col::Sym(vec!["AAPL"; 4]),
            Col::Dbl(vec![1.0, 2.0, 3.0, 4.0]),
            Col::Dbl(vec![1.0, 1.0, 1.0, 1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(events.len(), 4);
}

#[test]
fn a_backwards_timestamp_is_rejected_rather_than_replayed_out_of_order() {
    // An unsorted backtest feed produces a non-monotonic clock and wrong results, silently.
    let dir = dir();
    let path = dir.path().join("unsorted.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0 + HOUR, T0]),
            Col::Sym(vec!["AAPL", "AAPL"]),
            Col::Dbl(vec![1.0, 2.0]),
            Col::Dbl(vec![1.0, 1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    assert!(matches!(error, LseError::NonMonotonicTimestamps { .. }));
}

#[test]
fn the_iterator_ends_at_the_first_error_rather_than_resuming() {
    // The third row would otherwise decode cleanly: the ordering high-water mark is only advanced
    // on success, so it is still `T0 + HOUR` and `T0 + 2*HOUR` passes. Continuing would hand a
    // caller who discards errors a silently truncated view of a file already proven corrupt.
    let dir = dir();
    let path = dir.path().join("poisoned.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0 + HOUR, T0, T0 + 2 * HOUR]),
            Col::Sym(vec!["AAPL", "AAPL", "AAPL"]),
            Col::Dbl(vec![1.0, 2.0, 3.0]),
            Col::Dbl(vec![1.0, 1.0, 1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let mut events = read_export(&export, idx()).unwrap();

    assert!(matches!(events.next(), Some(Ok(_))));
    assert!(matches!(
        events.next(),
        Some(Err(LseError::NonMonotonicTimestamps { .. }))
    ));
    assert!(
        events.next().is_none(),
        "must not resume past a proven integrity violation"
    );
    // Fused: once ended, it stays ended.
    assert!(events.next().is_none());
}

#[test]
fn a_row_whose_symbol_contradicts_the_descriptor_is_rejected() {
    // The file and its descriptor disagree, so the artifact is not the one the caller thinks it
    // is. `BP` and `BP.L` are different instruments in different currencies -- a silent
    // misattribution here is a 100x pricing error.
    let dir = dir();
    let path = dir.path().join("wrong.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["BP"]),
            Col::Dbl(vec![43.9]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "BP.L", LseExportTimeframe::Tick);
    let error = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    let LseError::SymbolMismatch { expected, found } = error else {
        panic!("expected SymbolMismatch");
    };
    assert_eq!(expected, "BP.L");
    assert_eq!(found, "BP");
}

#[test]
fn an_unrecognised_schema_is_a_typed_error_not_a_mis_decode() {
    let dir = dir();
    let path = dir.path().join("odd.parquet");
    write_parquet(
        &path,
        "
        message schema {
          REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
          REQUIRED BYTE_ARRAY symbol (STRING);
          REQUIRED DOUBLE something_else;
        }",
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx()).unwrap_err();

    assert!(matches!(error, LseError::UnsupportedSchema { .. }));
    assert!(error.to_string().contains("something_else"));
}

#[test]
fn a_zero_row_artifact_decodes_to_no_events_without_erroring() {
    // The measured result of a `symbol: "all"` export: a well-formed file with the full schema and
    // nothing in it.
    let dir = dir();
    let path = dir.path().join("empty.parquet");
    write_parquet(
        &path,
        FX_TICK,
        vec![
            Col::Ts(vec![]),
            Col::Sym(vec![]),
            Col::Dbl(vec![]),
            Col::Dbl(vec![]),
        ],
    );

    let export = export(path, LseDataset::Fx, "EUR/USD", LseExportTimeframe::Tick);
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert!(events.is_empty());
}

// ── helpers ──────────────────────────────────────────────────────────────────

#[test]
fn symbols_in_export_lists_the_files_symbols_without_decoding_it() {
    let dir = dir();
    let path = dir.path().join("syms.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0, T0 + HOUR, T0 + DAY]),
            Col::Sym(vec!["AAPL", "AAPL", "AAPL"]),
            Col::Dbl(vec![1.0, 2.0, 3.0]),
            Col::Dbl(vec![1.0, 1.0, 1.0]),
        ],
    );

    assert_eq!(symbols_in_export(&path).unwrap(), vec!["AAPL".to_owned()]);
}

#[test]
fn instrument_index_is_derived_from_the_registry_so_a_typo_cannot_be_silent() {
    use rustrade_instrument::Underlying;
    use rustrade_instrument::instrument::name::{InstrumentNameExchange, InstrumentNameInternal};
    use rustrade_instrument::instrument::quote::InstrumentQuoteAsset;
    use rustrade_instrument::instrument::{Instrument, kind::InstrumentKind};
    use rustrade_instrument::test_utils::asset;

    let name = InstrumentNameExchange::from("BP.L");
    let instruments = IndexedInstrumentsBuilder::default()
        .add_instrument(Instrument::new(
            ExchangeId::LseEquities,
            InstrumentNameInternal::new_from_exchange(ExchangeId::LseEquities, name.clone()),
            name,
            Underlying::new(asset("bp.l"), asset("gbx")),
            InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Spot,
            None,
        ))
        .build();

    assert_eq!(
        instrument_index_for(&instruments, ExchangeId::LseEquities, "BP.L").unwrap(),
        InstrumentIndex::new(0)
    );

    // The typo that would otherwise leave one instrument silently receiving nothing.
    let error = instrument_index_for(&instruments, ExchangeId::LseEquities, "BP").unwrap_err();
    assert!(matches!(error, LseError::UnknownInstrument { .. }));
    // The message names what IS registered, so the fix is obvious.
    assert!(error.to_string().contains("BP.L"));
}
