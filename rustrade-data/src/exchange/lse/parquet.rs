//! Decode a downloaded bulk-export artifact into [`MarketEvent`]s.
//!
//! Behind the `lse-parquet` feature: the core `lse` feature runs export jobs and produces files
//! without any Parquet dependency, so a consumer who only wants artifacts on disk pays nothing for
//! this.
//!
//! # The item type is decided by the file, not by the caller
//! The provider's tick schema **varies by dataset**, so there is no single "LSE tick" event. All
//! four layouts below are measured, and the decoder dispatches on the columns actually present:
//!
//! ```text
//! bid,   ask                      => DataKind::OrderBookL1   (fx)
//! price, ask   (+ volume)         => DataKind::OrderBookL1   (volatility, interest_rates, currency_index)
//! price, volume                   => DataKind::Trade         (stocks, and the other candle classes)
//! open, high, low, close (+ vol)  => DataKind::Candle
//! ```
//!
//! `price` is the **bid**: the provider's own price endpoint returns `price`, `bid` and `ask` side
//! by side and `price == bid` exactly, measured on ten symbols spanning every dataset family. That
//! is why the `price`/`ask` layout decodes to a quote rather than a trade.
//!
//! # ⚠️ A decoded [`DataKind::Trade`] may not be a trade
//! The `price`/`volume` layout carries no ask, so a quote is not constructible and a trade is the
//! only available mapping — but by the measurement above, that `price` is very likely a bid-side
//! observation rather than a print. Treat it as "the provider's price series", not as evidence a
//! trade occurred at that instant. The same caveat reaches candles built from it.
//!
//! # ⚠️ Candles are BID candles, at least for FX
//! Reconciling a day of `EUR/USD` 1-minute candles against the tick tape for the same day, open,
//! high, low and close matched the **bid** series on 1421 of 1421 minutes and matched the mid or
//! the ask on none. A backtest that fills at the candle close is filling at the bid — favourable
//! by a full spread on every buy. This is a property of the data, not of this decoder, and it
//! cannot be corrected here without inventing a spread.
//!
//! # ⚠️ Volume is not a dependable figure
//! The candle `volume` column is **absent entirely** for FX (the true `None`) and present for
//! equities — but measured unreliable there: a majority of one-minute bars reported `0` in minutes
//! where the tick tape shows real trades, and a daily series carried a contiguous band roughly
//! 2,000× too large. A literal `0` is decoded faithfully as `Some(0)` rather than being rewritten
//! to `None`: rewriting would be this library inventing a fact. Validate before using it.
//!
//! # One file, one symbol
//! Every artifact the provider will produce is single-symbol — `symbol` is mandatory on an export
//! and `"all"` silently matches nothing. So **combining instruments needs
//! [`merge_time_sorted`](crate::streams::merge::merge_time_sorted)**, as on the candle replay path;
//! there is no multi-symbol file to decode in one pass. See [`read_export`] for the adapter that
//! bridges a decoded artifact to that merge. The decoder still verifies the `symbol` column on
//! every row, which is what catches a mis-described file — including the `BP` versus `BP.L` case,
//! where the two are different instruments in different currencies.
//!
//! # ⚠️ Licensing
//! Decoded data is **not redistributable**. See the [module documentation](super) and
//! <https://londonstrategicedge.com/terms>.

use crate::books::Level;
use crate::event::{DataKind, MarketEvent};
use crate::exchange::lse::error::LseError;
use crate::exchange::lse::export::{LseExport, LseExportTimeframe};
use crate::subscription::book::OrderBookL1;
use crate::subscription::candle::{Candle, close_time_from_open};
use crate::subscription::trade::PublicTrade;
use chrono::{DateTime, Utc};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::reader::RowIter;
use parquet::record::{Row, RowAccessor};
use parquet::schema::types::Type as SchemaType;
use rust_decimal::Decimal;
use rustrade_instrument::instrument::name::InstrumentNameExchange;
use rustrade_instrument::{
    exchange::ExchangeId, index::IndexedInstruments, instrument::InstrumentIndex,
};
use smol_str::SmolStr;
use std::fs::File;
use std::path::Path;
use tracing::{info, warn};

/// Column name of the event timestamp, present on every measured layout.
const COL_TS: &str = "ts";
/// Column name of the instrument symbol, present on every measured layout — even single-symbol.
const COL_SYMBOL: &str = "symbol";

/// The row layout of an export artifact, resolved from the columns present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowLayout {
    /// `bid` + `ask` — an explicit two-sided quote.
    Quote { bid: usize, ask: usize },
    /// `price` + `ask`, where `price` is the bid. `volume` is present but always empty here.
    QuoteFromPrice { price: usize, ask: usize },
    /// `price` + `volume` with no ask.
    Trade { price: usize, volume: usize },
    /// OHLC, with `volume` present only on some datasets.
    Candle {
        open: usize,
        high: usize,
        low: usize,
        close: usize,
        volume: Option<usize>,
    },
}

impl RowLayout {
    /// Resolve the layout from the artifact's column names.
    ///
    /// # Errors
    /// Returns [`LseError::UnsupportedSchema`] when the columns match none of the measured
    /// layouts. Surfaced up front rather than mis-decoded: a wrong column guess would produce
    /// plausible numbers in the wrong field.
    fn resolve(columns: &[String], timeframe: LseExportTimeframe) -> Result<Self, LseError> {
        let index = |name: &str| columns.iter().position(|column| column == name);

        let unsupported = || LseError::UnsupportedSchema {
            columns: columns.join(", "),
        };

        // Required on every measured layout; their absence means this is not an export artifact.
        if index(COL_TS).is_none() || index(COL_SYMBOL).is_none() {
            return Err(unsupported());
        }

        match timeframe {
            LseExportTimeframe::Candle(_) => {
                match (index("open"), index("high"), index("low"), index("close")) {
                    (Some(open), Some(high), Some(low), Some(close)) => Ok(Self::Candle {
                        open,
                        high,
                        low,
                        close,
                        // Absent for FX, present for equities. Keyed on presence, never on arity.
                        volume: index("volume"),
                    }),
                    _ => Err(unsupported()),
                }
            }
            LseExportTimeframe::Tick => match (index("bid"), index("ask"), index("price")) {
                (Some(bid), Some(ask), _) => Ok(Self::Quote { bid, ask }),
                // `price` beside an `ask` is the bid, so this is a quote despite the column name.
                (None, Some(ask), Some(price)) => Ok(Self::QuoteFromPrice { price, ask }),
                (None, None, Some(price)) => match index("volume") {
                    Some(volume) => Ok(Self::Trade { price, volume }),
                    None => Err(unsupported()),
                },
                _ => Err(unsupported()),
            },
        }
    }
}

/// Resolve the [`InstrumentIndex`] a display symbol was registered under.
///
/// # Why derive it rather than accept one
/// [`InstrumentIndex`] is a public, unbounded `usize`, and engine state indexes positionally — a
/// fabricated index attributes this file's prices to a different instrument, or panics. Deriving
/// it from the registry the engine was actually built with makes both unrepresentable, and catches
/// the typo (`BP` for `BP.L`) that would otherwise yield an instrument that silently never marks.
///
/// # Errors
/// Returns [`LseError::UnknownInstrument`] if no instrument on `exchange` carries `symbol` as its
/// exchange-side name, listing what is registered there.
pub fn instrument_index_for(
    instruments: &IndexedInstruments,
    exchange: ExchangeId,
    symbol: &str,
) -> Result<InstrumentIndex, LseError> {
    let wanted = InstrumentNameExchange::new(symbol);

    instruments
        .instruments()
        .iter()
        .find(|keyed| keyed.value.exchange.value == exchange && keyed.value.name_exchange == wanted)
        .map(|keyed| keyed.key)
        .ok_or_else(|| LseError::UnknownInstrument {
            symbol: symbol.to_owned(),
            exchange,
            registered: instruments
                .instruments()
                .iter()
                .filter(|keyed| keyed.value.exchange.value == exchange)
                .map(|keyed| keyed.value.name_exchange.to_string())
                .collect::<Vec<_>>()
                .join(", "),
        })
}

/// List the distinct symbols an artifact contains, without decoding it.
///
/// Lets a caller validate a file against their instrument registry *before* a backtest. It is
/// deliberately **not** run implicitly: a streaming backtest source is already re-read once per
/// run, and an automatic pre-scan would double that.
///
/// Cheap in practice — `symbol` is dictionary-encoded with row-group statistics, and the read is
/// projected to that column alone — but it still walks it, so it is opt-in.
///
/// Blocking, like [`read_export`]: run it under [`tokio::task::spawn_blocking`] when something else
/// shares the runtime.
///
/// # Errors
/// See [`LseError::Parquet`] and [`LseError::UnsupportedSchema`].
pub fn symbols_in_export(path: impl AsRef<Path>) -> Result<Vec<String>, LseError> {
    let reader = open_reader(path.as_ref())?;
    let root = reader.metadata().file_metadata().schema();

    let field = root
        .get_fields()
        .iter()
        .find(|field| field.name() == COL_SYMBOL)
        .ok_or_else(|| LseError::UnsupportedSchema {
            columns: column_names(&reader).join(", "),
        })?
        .clone();

    // The record API materialises every column it is handed, so an unprojected scan would decode
    // the price and OHLC doubles this function never looks at.
    let projection = SchemaType::group_type_builder(root.name())
        .with_fields(vec![field])
        .build()?;

    let mut symbols: Vec<String> = Vec::new();
    for row in reader.get_row_iter(Some(projection))? {
        let row = row?;
        // The projected row carries exactly one column, wherever `symbol` sits in the file schema.
        let value = row.get_string(0)?;
        // Compare before cloning: every artifact is single-symbol, so all but the first row of a
        // file that can run to hundreds of thousands would otherwise allocate only to be discarded.
        if !symbols.iter().any(|symbol| symbol == value) {
            symbols.push(value.clone());
        }
    }

    Ok(symbols)
}

/// Decode an export artifact into market events for one instrument.
///
/// The returned iterator yields [`MarketEvent`]s in file order, which is ascending by
/// `time_exchange`.
///
/// # ⚠️ This decodes synchronously and can block for a long time
/// Both the file read and the Parquet decode are blocking, over artifacts that run to hundreds of
/// thousands of rows. Called directly from an async task, it stalls that runtime worker for the
/// whole decode. Drive it from [`tokio::task::spawn_blocking`] (or an equivalent) whenever anything
/// else shares the runtime.
///
/// # Combining artifacts
/// Every artifact is single-symbol, so a multi-instrument run merges several with
/// [`merge_time_sorted`](crate::streams::merge::merge_time_sorted). That merge takes *streams* of
/// [`MarketStreamEvent`](crate::streams::consumer::MarketStreamEvent), so a decoded artifact needs
/// two adapting steps — and only two, because these events are already instrument-tagged, unlike
/// the candle replay path's, which additionally need `tag_events`:
///
/// ```no_run
/// # use futures::StreamExt;
/// # use rustrade_data::exchange::lse::export::LseExport;
/// # use rustrade_data::exchange::lse::parquet::read_export;
/// # use rustrade_data::streams::consumer::MarketStreamEvent;
/// # use rustrade_data::streams::merge::merge_time_sorted;
/// # use rustrade_instrument::instrument::InstrumentIndex;
/// # fn merge(artifacts: &[(LseExport, InstrumentIndex)]) -> Result<(), Box<dyn std::error::Error>> {
/// let streams = artifacts
///     .iter()
///     .map(|(export, instrument)| {
///         // 1. iterator -> stream, 2. bare event -> the merge's reconnect-aware item.
///         read_export(export, *instrument).map(|events| {
///             futures::stream::iter(events).map(|event| event.map(MarketStreamEvent::Item))
///         })
///     })
///     .collect::<Result<Vec<_>, _>>()?;
///
/// let _merged = merge_time_sorted(streams);
/// # Ok(())
/// # }
/// ```
///
/// # `time_exchange` for a candle is the bar's `close_time`
/// The artifact's `ts` column is the bar's **open** instant. A bar enters the timeline when its
/// period *ends*, so `time_exchange` is the derived exclusive close — stamping the open would let a
/// strategy act on a completed bar at the instant its period began, which is lookahead and silent.
/// This matches the candle replay path, so the two producers are interchangeable in a merge. Tick
/// layouts have no such derivation: their `time_exchange` is the observation instant as published.
///
/// # What is verified, and why here
/// - **The schema**, up front, against the columns present — not assumed from the dataset.
/// - **The symbol on every row**, against [`LseExport::symbol`]. Cheap next to Parquet decode, and
///   the only thing that catches a file described by the wrong descriptor.
/// - **Ascending `time_exchange`**, because `MarketDataStreamed` explicitly delegates that
///   obligation to its source rather than checking it. **The comparison is `<=`**: timestamps are
///   non-decreasing, not strictly ascending — 68% of adjacent rows tie on an equity tape — so
///   requiring strict ascent would reject valid files.
///
/// # Errors
/// See [`LseError::UnsupportedSchema`], [`LseError::SymbolMismatch`],
/// [`LseError::NonMonotonicTimestamps`], [`LseError::PriceNotRepresentable`],
/// [`LseError::TimestampNotRepresentable`], [`LseError::TimestampOverflow`] and
/// [`LseError::Parquet`]. Decode errors are surfaced, never skipped, and the **first one ends the
/// iterator** — see [`LseExportEvents`].
pub fn read_export(
    export: &LseExport,
    instrument: InstrumentIndex,
) -> Result<LseExportEvents, LseError> {
    let reader = open_reader(export.path())?;
    let columns = column_names(&reader);
    let layout = RowLayout::resolve(&columns, export.timeframe())?;

    let symbol = columns
        .iter()
        .position(|column| column == COL_SYMBOL)
        .ok_or_else(|| LseError::UnsupportedSchema {
            columns: columns.join(", "),
        })?;
    let ts = columns
        .iter()
        .position(|column| column == COL_TS)
        .ok_or_else(|| LseError::UnsupportedSchema {
            columns: columns.join(", "),
        })?;

    let rows = reader.metadata().file_metadata().num_rows();
    info!(
        path = %export.path().display(),
        dataset = export.dataset().as_catalog_str(),
        symbol = export.symbol(),
        rows,
        ?layout,
        "decoding export artifact"
    );

    if rows == 0 {
        // A zero-row artifact is well-formed and carries the full schema, so nothing downstream
        // would complain. It is the measured result of an `"all"` export, and of a range that
        // matched nothing.
        warn!(
            path = %export.path().display(),
            symbol = export.symbol(),
            "export artifact contains no rows; a backtest fed from it will see no events"
        );
    }

    Ok(LseExportEvents {
        // Owns the reader, so the iterator is `'static` and can outlive this call.
        rows: RowIter::from_file_into(Box::new(reader)),
        layout,
        ts,
        symbol,
        expected_symbol: export.symbol().to_owned(),
        exchange: export.exchange_id(),
        timeframe: export.timeframe(),
        instrument,
        previous: None,
        failed: false,
        // Saturating rather than failing: the row count only feeds `size_hint`, which is a hint.
        remaining: usize::try_from(rows).unwrap_or(usize::MAX),
    })
}

/// Iterator over the market events decoded from one export artifact.
///
/// Created by [`read_export`].
///
/// # It ends at the first error
/// Any [`Err`] is terminal: the iterator yields it once and every later call returns [`None`]
/// ([`FusedIterator`](std::iter::FusedIterator)). The symbol and ordering checks are **whole-file**
/// invariants rather than per-row conditions, so once one is violated the artifact is known not to
/// hold the property the rest of the pipeline assumes without re-checking, and no later row can
/// restore it. Continuing would hand a caller who discards errors — `.filter_map(Result::ok)`, or a
/// log-and-continue loop — a self-consistent but silently truncated view of a file already proven
/// corrupt, which is the failure mode these checks exist to prevent.
///
/// This deliberately differs from the `databento` DBN iterators, which continue past a per-record
/// decode failure. Their errors are per-record and carry no cross-row meaning; these are verdicts
/// on the file. (Not linked: that module is behind its own feature, so the link would be dead in a
/// build without it.)
pub struct LseExportEvents {
    rows: parquet::record::reader::RowIter<'static>,
    layout: RowLayout,
    ts: usize,
    symbol: usize,
    expected_symbol: String,
    exchange: ExchangeId,
    timeframe: LseExportTimeframe,
    instrument: InstrumentIndex,
    previous: Option<DateTime<Utc>>,
    /// Set once any error has been yielded; see the type's documentation.
    ///
    /// Also guards against a non-termination hazard in the Parquet reader itself: its `RowIter`
    /// does not advance past a row group it failed to open, so without this latch a corrupt
    /// artifact can yield the same error forever instead of ending.
    failed: bool,
    /// Rows not yet consumed, from the file metadata — the `size_hint` upper bound.
    remaining: usize,
}

impl std::fmt::Debug for LseExportEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `RowIter` is not `Debug`, and the decoding position is the useful part anyway.
        f.debug_struct("LseExportEvents")
            .field("symbol", &self.expected_symbol)
            .field("exchange", &self.exchange)
            .field("layout", &self.layout)
            .field("previous", &self.previous)
            .finish_non_exhaustive()
    }
}

impl Iterator for LseExportEvents {
    type Item = Result<MarketEvent<InstrumentIndex, DataKind>, LseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }

        let row = match self.rows.next()? {
            Ok(row) => row,
            Err(error) => {
                self.failed = true;
                return Some(Err(error.into()));
            }
        };

        self.remaining = self.remaining.saturating_sub(1);

        let decoded = self.decode(&row);
        if decoded.is_err() {
            self.failed = true;
        }

        Some(decoded)
    }

    /// Upper bound only: one event per row, but an error can end the iterator early, so the lower
    /// bound stays zero. The row count comes from the file metadata, read once at construction.
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.failed {
            (0, Some(0))
        } else {
            (0, Some(self.remaining))
        }
    }
}

// Upheld by the `failed` latch: once `next` has returned `None` it can only return `None`, whether
// the file ran out or an error ended it.
impl std::iter::FusedIterator for LseExportEvents {}

impl LseExportEvents {
    /// Decode one row, enforcing the symbol and ordering invariants.
    fn decode(&mut self, row: &Row) -> Result<MarketEvent<InstrumentIndex, DataKind>, LseError> {
        let symbol = row.get_string(self.symbol)?;
        if symbol != &self.expected_symbol {
            return Err(LseError::SymbolMismatch {
                expected: self.expected_symbol.clone(),
                found: symbol.clone(),
            });
        }

        let micros = row.get_timestamp_micros(self.ts)?;
        let observed = DateTime::from_timestamp_micros(micros)
            .ok_or(LseError::TimestampNotRepresentable { micros })?;

        // For a candle this is the bar's OPEN instant, so the payload decides the event time.
        let (time_exchange, kind) = self.decode_kind(row, observed)?;

        // The invariant is `previous <= current`, not strict ascent: ties are the common case
        // rather than an edge case, so only a step BACKWARDS is a violation.
        if let Some(previous) = self.previous.filter(|previous| time_exchange < *previous) {
            return Err(LseError::NonMonotonicTimestamps {
                previous,
                found: time_exchange,
            });
        }
        self.previous = Some(time_exchange);

        Ok(MarketEvent {
            time_exchange,
            // A file replay has no genuine receipt instant; only `time_exchange` orders the
            // timeline, so mirroring it beats a synthetic `Utc::now()`.
            time_received: time_exchange,
            exchange: self.exchange,
            instrument: self.instrument,
            kind,
        })
    }

    /// Build the payload for one row according to the resolved layout.
    ///
    /// Returns the instant the event enters the timeline alongside the payload. For a tick that is
    /// the observation instant; for a candle it is the bar's **close**, not the `ts` it was read
    /// from — see [`read_export`].
    fn decode_kind(
        &self,
        row: &Row,
        time: DateTime<Utc>,
    ) -> Result<(DateTime<Utc>, DataKind), LseError> {
        match self.layout {
            RowLayout::Quote { bid, ask } => {
                Ok((time, quote(time, decimal(row, bid)?, decimal(row, ask)?)))
            }
            RowLayout::QuoteFromPrice { price, ask } => {
                // `price` is the bid; `ask` is nullable on these datasets, so a null row yields a
                // one-sided book rather than a fabricated ask.
                let bid = decimal(row, price)?;
                let kind = match optional_decimal(row, ask)? {
                    Some(ask) => quote(time, bid, ask),
                    None => DataKind::OrderBookL1(OrderBookL1 {
                        last_update_time: time,
                        best_bid: Some(Level::new(bid, Decimal::ZERO)),
                        best_ask: None,
                    }),
                };

                Ok((time, kind))
            }
            RowLayout::Trade { price, volume } => Ok((
                time,
                DataKind::Trade(PublicTrade {
                    // The tape carries no trade identifier; the timestamp is not unique (ties are
                    // the norm), so inventing one from it would be a fabricated, colliding id.
                    id: SmolStr::default(),
                    price: decimal(row, price)?,
                    amount: decimal(row, volume)?,
                    // No aggressor side is published, and it is not inferable from a bid-side price.
                    side: None,
                }),
            )),
            RowLayout::Candle {
                open,
                high,
                low,
                close,
                volume,
            } => {
                let LseExportTimeframe::Candle(interval) = self.timeframe else {
                    // Unreachable: `RowLayout::resolve` only produces `Candle` for a candle
                    // timeframe. Returned rather than panicked, per this crate's error policy.
                    return Err(LseError::UnsupportedSchema {
                        columns: "candle columns on a tick export".to_owned(),
                    });
                };

                // `ts` is the bar's OPEN time; the library's contract is an exclusive `close_time`.
                let close_time = close_time_from_open(time, interval.to_step()).ok_or(
                    LseError::TimestampOverflow {
                        open: time,
                        interval,
                    },
                )?;

                Ok((
                    // A bar enters the timeline when its period ENDS, matching the candle replay
                    // path. Stamping the open would let a strategy act on a completed bar at the
                    // instant its period began — silent lookahead.
                    close_time,
                    DataKind::Candle(Candle {
                        close_time,
                        open: decimal(row, open)?,
                        high: decimal(row, high)?,
                        low: decimal(row, low)?,
                        close: decimal(row, close)?,
                        // Absent column => `None` (FX). Present => faithful pass-through, including
                        // a literal zero: rewriting the provider's number would be inventing a fact.
                        volume: match volume {
                            Some(volume) => optional_decimal(row, volume)?,
                            None => None,
                        },
                        // Never published on any dataset.
                        trade_count: None,
                    }),
                ))
            }
        }
    }
}

/// Build a two-sided L1 book.
///
/// Both levels carry a zero size: the export publishes prices only, and a fabricated size would be
/// indistinguishable from a real one downstream.
fn quote(time: DateTime<Utc>, bid: Decimal, ask: Decimal) -> DataKind {
    DataKind::OrderBookL1(OrderBookL1 {
        last_update_time: time,
        best_bid: Some(Level::new(bid, Decimal::ZERO)),
        best_ask: Some(Level::new(ask, Decimal::ZERO)),
    })
}

/// Read a required `double` column as a [`Decimal`].
fn decimal(row: &Row, index: usize) -> Result<Decimal, LseError> {
    convert(row.get_double(index)?)
}

/// Read a nullable `double` column as a [`Decimal`], mapping SQL null to `None`.
fn optional_decimal(row: &Row, index: usize) -> Result<Option<Decimal>, LseError> {
    // `is_null` indexes the row's fields directly; walking the column iterator instead would cost
    // one step per preceding column, on every row of a file that can run to hundreds of thousands.
    if row.is_null(index)? {
        return Ok(None);
    }

    decimal(row, index).map(Some)
}

/// Convert a provider `f64` into a [`Decimal`], surfacing rather than swallowing a failure.
///
/// `f64::NAN` and the infinities have no [`Decimal`] representation. Substituting zero would put a
/// real-looking price into the money path.
fn convert(value: f64) -> Result<Decimal, LseError> {
    Decimal::try_from(value).map_err(|error| LseError::PriceNotRepresentable {
        value,
        message: error.to_string(),
    })
}

/// Open an artifact for reading.
fn open_reader(path: &Path) -> Result<SerializedFileReader<File>, LseError> {
    let file = File::open(path).map_err(|source| LseError::Io {
        message: format!("opening {}", path.display()),
        source,
    })?;

    Ok(SerializedFileReader::new(file)?)
}

/// The artifact's column names, in schema order.
fn column_names(reader: &SerializedFileReader<File>) -> Vec<String> {
    let schema = reader.metadata().file_metadata().schema_descr_ptr();

    (0..schema.num_columns())
        .map(|index| schema.column(index).name().to_owned())
        .collect()
}
