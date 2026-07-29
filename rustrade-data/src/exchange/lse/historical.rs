//! Paged historical candles from the London Strategic Edge vault.
//!
//! ```ignore
//! use rustrade_data::exchange::lse::vault::LseVaultClient;
//! use rustrade_data::subscription::candle::CandleInterval;
//! use chrono::{Duration, Utc};
//! use futures::StreamExt;
//!
//! let client = LseVaultClient::from_env()?;
//! let end = Utc::now();
//! let start = end - Duration::days(7);
//!
//! let mut stream = client.fetch_candles("EUR/USD", CandleInterval::Day1, start, end);
//! while let Some(candle) = stream.next().await {
//!     println!("{:?}", candle?);
//! }
//! ```
//!
//! # ⚠️ Licensing
//! Candles retrieved here are **not redistributable**. See the [module documentation](super) and
//! <https://londonstrategicedge.com/terms>.

use crate::exchange::lse::error::LseError;
use crate::exchange::lse::market::candle_interval_str;
use crate::exchange::lse::vault::LseVaultClient;
use crate::subscription::candle::{
    Candle, CandleInterval, close_time_from_open, open_time_from_close,
};
use async_stream::try_stream;
use chrono::{DateTime, Duration as TimeDelta, NaiveDateTime, Utc};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::debug;

/// Rows requested per page.
///
/// Matches the provider's [`max_rows_per_request`](super::quota::QuotaStatus::max_rows_per_request).
/// The cap is enforced **silently** — an over-large range returns exactly this many rows with a
/// `200` and no truncation marker — which is why pagination does not treat a short page as the end
/// of the data. See [`LseVaultClient::fetch_candles`].
const PAGE_LIMIT: u32 = 5000;

/// Format of the `ts` field in a candle row (`2024-01-02 09:09:00.000000`).
///
/// `%.f` makes the fractional part optional, so a response that drops the microseconds still
/// parses. The value carries no timezone and is UTC.
const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

/// Format accepted by the `start` / `end` query parameters.
///
/// **Second precision, and no finer.** The provider rejects both a sub-second value and the ISO
/// `2024-01-02T09:09:00Z` form with a `400` whose message (`use YYYY-MM-DD`) understates what is
/// actually accepted — the time-of-day *is* honoured.
const CURSOR_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// Amount the page cursor advances past the last bar received.
///
/// The cursor is inclusive, so resuming exactly at the last open time would re-yield that bar.
/// One second is the smallest step the parameter accepts.
///
/// # ⚠️ This is lossless only while the finest resolution is one second
/// Bars are at least one second apart, so stepping a full second past the last open can never skip
/// one. Were the provider to publish a sub-second resolution, this step would silently drop bars —
/// revisit it alongside any new [`CandleInterval`] variant below [`Sec1`](CandleInterval::Sec1).
const CURSOR_STEP_SECS: i64 = 1;

/// One candle as the vault serves it.
///
/// The `symbol` field is echoed by the provider and deliberately not captured: the caller already
/// knows what it asked for, and unknown fields are ignored rather than rejected so an added field
/// does not break decoding.
#[derive(Debug, Deserialize)]
struct LseCandleRow {
    /// Bar **open** time, UTC. See [`TIMESTAMP_FORMAT`].
    ts: String,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    /// Absent entirely for FX; an integer for equities.
    #[serde(default)]
    volume: Option<Decimal>,
}

impl LseCandleRow {
    /// Parse the row's `ts` as the bar's open instant.
    fn open_time(&self) -> Result<DateTime<Utc>, LseError> {
        NaiveDateTime::parse_from_str(&self.ts, TIMESTAMP_FORMAT)
            .map(|naive| naive.and_utc())
            .map_err(|error| LseError::Deserialize {
                message: format!("invalid candle timestamp {:?}: {error}", self.ts),
            })
    }

    /// Convert into the library [`Candle`] model, given the period-end boundary.
    ///
    /// `trade_count` is unconditionally `None`: the vault reports no trade count for any dataset.
    /// `volume` carries the provider's absence through as `None` rather than as a zero — for FX
    /// the field is omitted entirely, and a synthetic zero would aggregate into a
    /// legitimate-looking total at every derived resolution.
    fn into_candle(self, close_time: DateTime<Utc>) -> Candle {
        Candle {
            close_time,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            trade_count: None,
        }
    }
}

impl LseVaultClient {
    /// Fetch historical candles for `symbol` at `interval`, paginating automatically.
    ///
    /// Returns a [`Stream`] that processes each page as it arrives rather than buffering the whole
    /// range. For a convenience `Vec`, see [`collect_candles`](Self::collect_candles).
    ///
    /// # Symbol
    /// The vault keys candles on the **display symbol** — `EUR/USD`, `AAPL`, `BP.L`, `ES.F` — not
    /// on a dataset slug. Nothing here needs [`slug`](super::market::slug).
    ///
    /// # Range contract
    /// Yields exactly the candles whose [`close_time`](Candle::close_time) falls in `[start, end]`
    /// (**both inclusive**), matched on `close_time` — the field consumers receive — consistent
    /// with the library's other historical fetches.
    ///
    /// The vault's own range is expressed on the bar's **open** time with an **exclusive** upper
    /// bound, so this method maps both bounds (lower widens to capture the bar whose
    /// `close_time == start`; upper is extended past the final bar's open) and then trims exactly
    /// on `close_time`. The trim also absorbs the cursor's second-precision truncation of
    /// sub-second `start`/`end` values.
    ///
    /// `close_time` is computed library-side as the exclusive period-end boundary
    /// (`open_time + interval`) via the shared boundary helper; the provider reports only the open
    /// instant.
    ///
    /// # Zero-trade periods are absent, not gap-filled
    /// The vault omits periods with no activity entirely, so consecutive candles are **not**
    /// guaranteed to be one interval apart. This is the opposite of Binance's REST klines, which
    /// server-side gap-fill. Consumers that need a dense grid must fill it themselves.
    ///
    /// # Volume
    /// FX candles carry **no volume**: the vault omits the field, which surfaces as
    /// [`volume: None`](Candle::volume) rather than a zero. `trade_count` is `None` for every
    /// dataset — the vault reports none.
    ///
    /// # Arguments
    /// * `symbol` - Display symbol, e.g. `"EUR/USD"` or `"AAPL"`.
    /// * `interval` - Resolution. The provider serves 14 of [`CandleInterval`]'s variants;
    ///   an unserved one is rejected up front with [`LseError::UnsupportedInterval`] rather than
    ///   relayed as a `400`.
    /// * `start` / `end` - Inclusive `close_time` bounds.
    ///
    /// # Errors
    /// Each yielded item is a `Result`. On `429` the stream yields [`LseError::RateLimited`] and
    /// **ends** — resume by re-calling with `start` set past the last `close_time` received. An
    /// inverted range is [`LseError::InvalidInput`]. Other failures surface as
    /// [`LseError::Api`] / [`Http`](LseError::Http) / [`Deserialize`](LseError::Deserialize).
    ///
    /// # Request count
    /// Because the row cap is applied silently, a short page cannot be distinguished from the end
    /// of the data. Pagination therefore continues until a page comes back **empty** or the cursor
    /// passes the requested range, which costs at most one extra request per fetch. Treating a
    /// short page as terminal would be faster and would silently truncate the result if the
    /// provider ever lowered its cap below the 5,000 rows requested per page — the figure the
    /// provider itself reports as
    /// [`max_rows_per_request`](super::quota::QuotaStatus::max_rows_per_request).
    #[must_use = "fetch_candles returns a lazy Stream that does nothing unless polled"]
    pub fn fetch_candles<'a>(
        &'a self,
        symbol: &'a str,
        interval: CandleInterval,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> impl Stream<Item = Result<Candle, LseError>> + 'a {
        try_stream! {
            // An inverted range is a caller error, not an empty result: the vault would answer 200
            // with a confusing selection rather than complaining.
            if start > end {
                Err(LseError::InvalidInput {
                    message: format!("start ({start}) must not be after end ({end})"),
                })?;
            }

            // Rejected up front so the caller gets a typed answer instead of a relayed 400.
            let timeframe = candle_interval_str(interval)
                .ok_or(LseError::UnsupportedInterval { interval })?;

            let step = interval.to_step();

            // Map the close-time contract onto the vault's open-time filter. `None` (underflow near
            // `DateTime::MIN_UTC`) is not an error: the boundary bar would have an unrepresentable
            // open and so cannot exist, making the un-widened bound already correct.
            let request_start = open_time_from_close(start, step).unwrap_or(start);
            let request_end_open = open_time_from_close(end, step).unwrap_or(end);

            // The vault's `end` is EXCLUSIVE on open time, so extend past the final bar's open to
            // readmit it. Saturating near `DateTime::MAX_UTC` is safe -- the close-time trim below
            // is exact regardless, so an un-extended bound only risks omitting a bar that cannot
            // exist at that boundary.
            let range_end = request_end_open
                .checked_add_signed(TimeDelta::seconds(CURSOR_STEP_SECS))
                .unwrap_or(request_end_open);

            let mut cursor = request_start;
            let mut page = 0usize;

            loop {
                // Proactive courtesy between pages only; never on the first request.
                if page > 0 && !self.pace().is_zero() {
                    tokio::time::sleep(self.pace()).await;
                }

                let query = [
                    ("symbol", symbol.to_owned()),
                    // ⚠️ `timeframe`, NOT `resolution`. An unknown parameter is ignored silently
                    // and the vault defaults to 1-minute bars, returning a byte-identical shape.
                    ("timeframe", timeframe.to_owned()),
                    ("start", cursor.format(CURSOR_FORMAT).to_string()),
                    ("end", range_end.format(CURSOR_FORMAT).to_string()),
                    ("limit", PAGE_LIMIT.to_string()),
                ];

                let rows: Vec<LseCandleRow> = self.get_json("candles", &query).await?;
                page += 1;
                debug!(symbol, %cursor, rows = rows.len(), page, "vault candle page received");

                // The only reliable end-of-data signal; see the request-count note above.
                if rows.is_empty() {
                    break;
                }

                let mut max_open: Option<DateTime<Utc>> = None;
                let mut reached_end = false;

                for row in rows {
                    let open = row.open_time()?;
                    let close_time = close_time_from_open(open, step)
                        .ok_or(LseError::TimestampOverflow { open, interval })?;

                    // Track every row, including trimmed ones, so the cursor advances past bars
                    // that fell outside the contract rather than re-requesting them forever.
                    max_open = Some(max_open.map_or(open, |seen| seen.max(open)));

                    // Rows arrive ascending, so the first bar past the upper bound ends the fetch.
                    if close_time > end {
                        reached_end = true;
                        break;
                    }

                    // Below the lower bound only for the widened first page.
                    if close_time < start {
                        continue;
                    }

                    yield row.into_candle(close_time);
                }

                if reached_end {
                    break;
                }

                let Some(last_open) = max_open else {
                    break;
                };

                let next_cursor = last_open
                    .checked_add_signed(TimeDelta::seconds(CURSOR_STEP_SECS))
                    .ok_or(LseError::TimestampOverflow { open: last_open, interval })?;

                // A page consisting solely of bars at or before the cursor would leave the cursor
                // unmoved and loop forever. That requires the provider to ignore `start`, which is
                // exactly the silent-parameter failure this integration guards against elsewhere,
                // so it is surfaced rather than trusted.
                if next_cursor <= cursor {
                    Err(LseError::Api {
                        status: 200,
                        message: format!(
                            "page {page} did not advance past {cursor}: the range parameters \
                             appear to have been ignored"
                        ),
                    })?;
                }

                cursor = next_cursor;

                // `>=` rather than `>`: the vault's upper bound is exclusive, so a cursor sitting
                // on it selects an empty window. Stopping here saves a guaranteed-empty request
                // whenever the last bar received was the final one in range.
                if cursor >= range_end {
                    break;
                }
            }
        }
    }

    /// Fetch historical candles into a `Vec`.
    ///
    /// Convenience over [`fetch_candles`](Self::fetch_candles) with the same range contract, for
    /// slices small enough to hold in memory. **Buffers the whole range** — prefer the stream for
    /// long backfills at fine resolutions.
    ///
    /// # Errors
    /// Fails on the first error, discarding any candles already received. See
    /// [`fetch_candles`](Self::fetch_candles).
    pub async fn collect_candles(
        &self,
        symbol: &str,
        interval: CandleInterval,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Candle>, LseError> {
        let stream = self.fetch_candles(symbol, interval, start, end);
        futures::pin_mut!(stream);

        let mut candles = Vec::new();
        while let Some(candle) = stream.next().await {
            candles.push(candle?);
        }

        Ok(candles)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn row(ts: &str, volume: Option<Decimal>) -> LseCandleRow {
        LseCandleRow {
            ts: ts.to_owned(),
            open: dec!(1),
            high: dec!(2),
            low: dec!(0.5),
            close: dec!(1.5),
            volume,
        }
    }

    #[test]
    fn an_equity_row_deserializes_float_prices_and_an_integer_volume() {
        // The measured equity shape: prices are JSON floats, volume a JSON integer. Both must land
        // in `Decimal` without a custom helper.
        let json = r#"{"ts":"2003-09-10 00:00:00.000000","symbol":"AAPL","open":0.4,
                       "high":0.402321428571429,"low":0.4,"close":0.4,"volume":3428513}"#;

        let row: LseCandleRow = serde_json::from_str(json).unwrap();

        assert_eq!(row.open, dec!(0.4));
        assert_eq!(row.high, dec!(0.402321428571429));
        assert_eq!(row.volume, Some(dec!(3428513)));
    }

    #[test]
    fn an_fx_row_omitting_volume_deserializes_as_none() {
        // The measured FX shape: no `volume` key at all. This must be `None`, never `Some(0)`.
        let json = r#"{"ts":"2003-01-01 00:00:00.000000","symbol":"EUR/USD","open":1.0493,
                       "high":1.0493,"low":1.0493,"close":1.0493}"#;

        let row: LseCandleRow = serde_json::from_str(json).unwrap();

        assert_eq!(row.volume, None);
    }

    #[test]
    fn timestamps_parse_as_utc() {
        // The value carries no zone. Treating it as UTC is what makes the pre-market open land at
        // 09:00 in winter and 08:00 in summer, i.e. 04:00 ET across the DST boundary.
        let open = row("2024-01-02 09:09:00.000000", None).open_time().unwrap();

        assert_eq!(
            open,
            "2024-01-02T09:09:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn a_timestamp_without_a_fractional_part_still_parses() {
        assert!(row("2024-01-02 09:09:00", None).open_time().is_ok());
    }

    #[test]
    fn a_malformed_timestamp_is_a_typed_error() {
        let error = row("not-a-timestamp", None).open_time().unwrap_err();

        assert!(matches!(error, LseError::Deserialize { .. }));
        assert!(error.to_string().contains("not-a-timestamp"));
    }

    #[test]
    fn trade_count_is_always_none() {
        // The vault reports no trade count for any dataset, so it must be the explicit unknown.
        let candle = row("2024-01-02 09:09:00.000000", Some(dec!(5))).into_candle(Utc::now());

        assert_eq!(candle.trade_count, None);
    }

    #[test]
    fn a_daily_bars_close_is_the_next_midnight_not_its_own_label() {
        // `ts` is the bar's OPEN. Passing it through as `close_time` would shift every candle in
        // the integration by one bar.
        let open = row("2024-01-02 00:00:00.000000", None).open_time().unwrap();

        let close = close_time_from_open(open, CandleInterval::Day1.to_step()).unwrap();

        assert_eq!(
            close,
            "2024-01-03T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn a_monthly_bars_close_uses_calendar_arithmetic() {
        // Measured: monthly bars are labelled on the 1st. February's length rules out a fixed step.
        let open = row("2024-02-01 00:00:00.000000", None).open_time().unwrap();

        let close = close_time_from_open(open, CandleInterval::Month1.to_step()).unwrap();

        assert_eq!(
            close,
            "2024-03-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }
}
