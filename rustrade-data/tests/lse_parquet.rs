//! Decoder tests for London Strategic Edge bulk-export artifacts.
//!
//! Every fixture here is **written by this test file** using the `parquet` crate's own writer,
//! shaped from measurements of the live API. No provider data is committed — the provider
//! prohibits redistribution (<https://londonstrategicedge.com/terms>).
//!
//! What is measured is the *shape*: which columns each dataset publishes, their types and
//! nullability, and what `ts` means. **Every number is invented** — deliberately round and
//! obviously synthetic, so that no row here can be mistaken for a real quote or bar. Decoding is
//! value-independent, so nothing is lost by that: a fixture priced at `1.1` exercises exactly the
//! same code path as one priced at a real tick.
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
use rust_decimal::Decimal;
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

/// Write a Parquet file with the given schema and columns, as one row group.
fn write_parquet(path: &Path, message: &str, columns: Vec<Col>) {
    write_parquet_row_groups(path, message, vec![columns]);
}

/// Write a Parquet file whose rows are split across one row group per element of `groups`.
///
/// Every measured artifact holds exactly one, but that is a property of the provider's writer and
/// not a guarantee of the format, so the decoder walks groups — and something has to exercise the
/// walk.
fn write_parquet_row_groups(path: &Path, message: &str, groups: Vec<Vec<Col>>) {
    let schema = Arc::new(parse_message_type(message).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(File::create(path).unwrap(), schema, props).unwrap();

    for columns in groups {
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
    }

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
            Col::Dbl(vec![1.1, 1.3]),
            Col::Dbl(vec![1.2, 1.4]),
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
    assert_eq!(book.best_bid.unwrap().price, dec!(1.1));
    assert_eq!(book.best_ask.unwrap().price, dec!(1.2));
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
            Col::Dbl(vec![20.5]),
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
    assert_eq!(trade.price, dec!(20.5));
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
            Col::Dbl(vec![2.0, 2.5]),
            Col::OptDbl(vec![Some(0.0), None]),
            Col::OptDbl(vec![Some(2.1), None]),
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
    assert_eq!(book.best_bid.unwrap().price, dec!(2.0));
    assert_eq!(book.best_ask.unwrap().price, dec!(2.1));

    // A null ask yields a one-sided book rather than a fabricated price.
    let DataKind::OrderBookL1(book) = &events[1].kind else {
        panic!("expected OrderBookL1");
    };
    assert_eq!(book.best_bid.unwrap().price, dec!(2.5));
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
            Col::Dbl(vec![100.0]),
            Col::Dbl(vec![110.0]),
            Col::Dbl(vec![90.0]),
            Col::Dbl(vec![105.0]),
            Col::Dbl(vec![1000.0]),
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
    assert_eq!(candle.open, dec!(100));
    assert_eq!(candle.close, dec!(105));
    assert_eq!(candle.volume, Some(dec!(1000)));
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
            Col::Dbl(vec![1.1]),
            Col::Dbl(vec![1.3]),
            Col::Dbl(vec![0.9]),
            Col::Dbl(vec![1.2]),
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
            Col::Dbl(vec![100.0]),
            Col::Dbl(vec![110.0]),
            Col::Dbl(vec![90.0]),
            Col::Dbl(vec![105.0]),
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

// ── batching and row groups ──────────────────────────────────────────────────

/// Mirrors the decoder's private batch width.
///
/// The decoder reads a fixed number of rows per column-reader call rather than a whole row group,
/// which is what bounds its memory over a multi-gigabyte artifact. Nothing below asserts this exact
/// figure — only that crossing it changes nothing — so the tests stay correct if it is retuned;
/// they merely stop straddling the boundary, which is why it is worth keeping in step.
const BATCH_ROWS: usize = 8 * 1024;

#[test]
fn an_artifact_longer_than_one_read_batch_decodes_every_row_in_order() {
    // Deliberately not a round multiple: the final batch is partial, and the row after the last
    // one has to end the iterator rather than read off the end of the buffers.
    let rows = BATCH_ROWS * 2 + 7;

    let mut ts = Vec::with_capacity(rows);
    let mut price = Vec::with_capacity(rows);
    let mut ask = Vec::with_capacity(rows);
    for index in 0..rows {
        let index = i32::try_from(index).unwrap();

        ts.push(T0 + i64::from(index));
        // Distinct per row, so a value paired with the wrong timestamp is visible rather than
        // coincidentally equal to the right one.
        price.push(f64::from(index));
        // Alternating nulls: an OPTIONAL column's value buffer is shorter than its batch, so the
        // per-column cursor has to survive a batch boundary landing mid-pattern. A cursor reset one
        // row early or late shifts every subsequent ask onto the wrong row.
        ask.push((index % 2 == 0).then(|| f64::from(index) + 0.5));
    }

    let dir = dir();
    let path = dir.path().join("long.parquet");
    write_parquet(
        &path,
        SYNTH_TICK,
        vec![
            Col::Ts(ts),
            Col::Sym(vec!["VIX/USD"; rows]),
            Col::Dbl(price),
            Col::OptDbl(vec![None; rows]),
            Col::OptDbl(ask),
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
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(events.len(), rows);

    for (index, event) in events.iter().enumerate() {
        let expected = Decimal::from(i64::try_from(index).unwrap());

        assert_eq!(
            event.time_exchange.timestamp_micros(),
            T0 + i64::try_from(index).unwrap(),
            "row {index} decoded against the wrong timestamp"
        );

        let DataKind::OrderBookL1(book) = &event.kind else {
            panic!("expected OrderBookL1 on row {index}, got {:?}", event.kind);
        };
        assert_eq!(
            book.best_bid.unwrap().price,
            expected,
            "row {index} decoded the wrong bid"
        );
        assert_eq!(
            book.best_ask.map(|level| level.price),
            (index % 2 == 0).then(|| expected + dec!(0.5)),
            "row {index} decoded the wrong ask"
        );
    }
}

#[test]
fn an_artifact_written_as_several_row_groups_decodes_every_row_in_order() {
    let dir = dir();
    let path = dir.path().join("groups.parquet");
    write_parquet_row_groups(
        &path,
        FX_TICK,
        (0..3)
            .map(|group| {
                let base = T0 + i64::from(group) * DAY;

                vec![
                    Col::Ts(vec![base, base + HOUR]),
                    Col::Sym(vec!["EUR/USD"; 2]),
                    Col::Dbl(vec![f64::from(group), f64::from(group) + 0.5]),
                    Col::Dbl(vec![f64::from(group) + 0.1, f64::from(group) + 0.6]),
                ]
            })
            .collect(),
    );

    let export = export(path, LseDataset::Fx, "EUR/USD", LseExportTimeframe::Tick);
    let events: Vec<_> = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Every group is walked, in file order, and the boundary between two of them is not an end.
    assert_eq!(events.len(), 6);
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].time_exchange <= pair[1].time_exchange),
        "row groups must be decoded in file order"
    );

    let bids: Vec<_> = events
        .iter()
        .map(|event| {
            let DataKind::OrderBookL1(book) = &event.kind else {
                panic!("expected OrderBookL1, got {:?}", event.kind);
            };
            book.best_bid.unwrap().price
        })
        .collect();
    assert_eq!(
        bids,
        vec![dec!(0), dec!(0.5), dec!(1), dec!(1.5), dec!(2), dec!(2.5)]
    );
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
    //
    // The offending row is deliberately NOT the first: the check is documented as applying to
    // every row, and a single-row fixture is equally satisfied by an implementation that inspects
    // only row 0 and then trusts the rest of the file.
    let dir = dir();
    let path = dir.path().join("wrong.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0, T0 + HOUR, T0 + DAY]),
            Col::Sym(vec!["BP.L", "BP.L", "BP"]),
            Col::Dbl(vec![10.0, 11.0, 12.0]),
            Col::Dbl(vec![1.0, 1.0, 1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "BP.L", LseExportTimeframe::Tick);
    let mut events = read_export(&export, idx()).unwrap();

    // The matching rows decode; the contradiction is what stops the stream.
    assert!(events.next().unwrap().is_ok());
    assert!(events.next().unwrap().is_ok());

    let error = events.next().unwrap().unwrap_err();
    let LseError::SymbolMismatch { expected, found } = error else {
        panic!("expected SymbolMismatch");
    };
    assert_eq!(expected, "BP.L");
    assert_eq!(found, "BP");
}

// ── typed decode failures ────────────────────────────────────────────────────
//
// Each of these is a silent-corruption class rather than a crash: nothing downstream can tell a
// millisecond timestamp read as microseconds, or a zero substituted for a NaN, from real data. The
// decoder turns every one into a typed error, and these pin that so a refactor or a `parquet`
// upgrade cannot quietly reintroduce the guess.

/// `STOCK_TICK` with `ts` in milliseconds — a 1000x time error if it were read as microseconds.
const STOCK_TICK_TS_MILLIS: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MILLIS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE price;
  REQUIRED DOUBLE volume;
}";

/// `STOCK_TICK` with `ts` in local time — epoch microseconds against an unknown zone.
const STOCK_TICK_TS_NOT_UTC: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,false));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED DOUBLE price;
  REQUIRED DOUBLE volume;
}";

/// `STOCK_TICK` with an integer `price` rather than a `DOUBLE`.
const STOCK_TICK_INT_PRICE: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  REQUIRED INT64 price;
  REQUIRED DOUBLE volume;
}";

/// `STOCK_TICK` with a nullable `price`, which the tick layout has no substitute for.
const STOCK_TICK_NULLABLE_PRICE: &str = "
message schema {
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY symbol (STRING);
  OPTIONAL DOUBLE price;
  REQUIRED DOUBLE volume;
}";

#[test]
fn a_millisecond_timestamp_column_is_rejected_rather_than_read_as_microseconds() {
    let dir = dir();
    let path = dir.path().join("millis.parquet");
    write_parquet(
        &path,
        STOCK_TICK_TS_MILLIS,
        vec![
            Col::Ts(vec![T0 / 1000]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![1.0]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx()).unwrap_err();

    let LseError::UnsupportedColumnType { column, .. } = &error else {
        panic!("expected UnsupportedColumnType, got {error:?}");
    };
    assert_eq!(column, "ts");
}

#[test]
fn a_timestamp_column_not_adjusted_to_utc_is_rejected() {
    // The half of the check that has no downstream symptom at all: a local-time column is the right
    // physical type, the right unit, and off by the writer's UTC offset on every row.
    let dir = dir();
    let path = dir.path().join("local.parquet");
    write_parquet(
        &path,
        STOCK_TICK_TS_NOT_UTC,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![1.0]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx()).unwrap_err();

    let LseError::UnsupportedColumnType { column, .. } = &error else {
        panic!("expected UnsupportedColumnType, got {error:?}");
    };
    assert_eq!(column, "ts");
}

#[test]
fn a_value_column_of_the_wrong_physical_type_is_rejected() {
    let dir = dir();
    let path = dir.path().join("intprice.parquet");
    write_parquet(
        &path,
        STOCK_TICK_INT_PRICE,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Ts(vec![1]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx()).unwrap_err();

    let LseError::UnsupportedColumnType {
        column, required, ..
    } = &error
    else {
        panic!("expected UnsupportedColumnType, got {error:?}");
    };
    assert_eq!(column, "price");
    assert_eq!(*required, "DOUBLE");
}

#[test]
fn a_null_in_a_column_the_layout_has_no_substitute_for_is_an_error() {
    // Nullable `volume`/`ask` map to `None` by design; `price` does not — a tick with no price is
    // not a tick, and defaulting it would put a zero into the money path.
    let dir = dir();
    let path = dir.path().join("nullprice.parquet");
    write_parquet(
        &path,
        STOCK_TICK_NULLABLE_PRICE,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::OptDbl(vec![None]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    let LseError::NullValue { column } = error else {
        panic!("expected NullValue, got {error:?}");
    };
    assert_eq!(column, "price");
}

#[test]
fn a_price_with_no_decimal_representation_is_surfaced_not_zeroed() {
    // `f64::NAN` has no `Decimal`. Substituting zero would put a real-looking price into the money
    // path -- and a zero price is not obviously wrong to anything downstream.
    let dir = dir();
    let path = dir.path().join("nan.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![f64::NAN]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    let LseError::PriceNotRepresentable { value, .. } = error else {
        panic!("expected PriceNotRepresentable, got {error:?}");
    };
    assert!(value.is_nan());
}

#[test]
fn an_infinite_price_is_surfaced_too() {
    let dir = dir();
    let path = dir.path().join("inf.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![f64::INFINITY]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    assert!(matches!(error, LseError::PriceNotRepresentable { .. }));
}

#[test]
fn a_timestamp_outside_the_representable_range_is_an_error_not_a_wrapped_instant() {
    let dir = dir();
    let path = dir.path().join("farfuture.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![i64::MAX]),
            Col::Sym(vec!["AAPL"]),
            Col::Dbl(vec![1.0]),
            Col::Dbl(vec![1.0]),
        ],
    );

    let export = export(path, LseDataset::Stocks, "AAPL", LseExportTimeframe::Tick);
    let error = read_export(&export, idx())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    let LseError::TimestampNotRepresentable { micros } = error else {
        panic!("expected TimestampNotRepresentable, got {error:?}");
    };
    assert_eq!(micros, i64::MAX);
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
    // Deliberately multi-symbol, with the repeat in the middle. An all-one-symbol fixture is
    // satisfied by an implementation that reads row 0 and stops, and a caller using this to decide
    // which instruments to index would then silently drop every symbol but the first.
    let dir = dir();
    let path = dir.path().join("syms.parquet");
    write_parquet(
        &path,
        STOCK_TICK,
        vec![
            Col::Ts(vec![T0, T0 + HOUR, T0 + DAY]),
            Col::Sym(vec!["AAPL", "AAPL", "MSFT"]),
            Col::Dbl(vec![1.0, 2.0, 3.0]),
            Col::Dbl(vec![1.0, 1.0, 1.0]),
        ],
    );

    // Deduplicated, in first-seen order.
    assert_eq!(
        symbols_in_export(&path).unwrap(),
        vec!["AAPL".to_owned(), "MSFT".to_owned()]
    );
}

#[test]
fn symbols_in_export_walks_every_row_group_not_just_the_first() {
    // The provider's writer emits one row group per artifact, so a decoder that stopped after the
    // first would pass every other test in this file.
    let dir = dir();
    let path = dir.path().join("syms_groups.parquet");
    write_parquet_row_groups(
        &path,
        STOCK_TICK,
        vec![
            vec![
                Col::Ts(vec![T0]),
                Col::Sym(vec!["AAPL"]),
                Col::Dbl(vec![1.0]),
                Col::Dbl(vec![1.0]),
            ],
            vec![
                Col::Ts(vec![T0 + DAY]),
                Col::Sym(vec!["MSFT"]),
                Col::Dbl(vec![2.0]),
                Col::Dbl(vec![1.0]),
            ],
        ],
    );

    assert_eq!(
        symbols_in_export(&path).unwrap(),
        vec!["AAPL".to_owned(), "MSFT".to_owned()]
    );
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
