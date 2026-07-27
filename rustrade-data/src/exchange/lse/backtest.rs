use crate::{
    error::DataError,
    event::DataKind,
    exchange::lse::vault::LseVaultClient,
    streams::{
        consumer::MarketStreamEvent,
        merge::{merge_time_sorted, tag_events},
    },
    subscription::candle::{Candle, CandleInterval},
};
use async_stream::try_stream;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use rustrade_instrument::{exchange::ExchangeId, instrument::InstrumentIndex};
use smol_str::SmolStr;
use std::sync::Arc;

/// One instrument's candle source: which vault symbol to fetch, and what to tag it as.
///
/// # The keys must match the instruments the engine was built with
/// `instrument` and `exchange` are **not** derived from the symbol, because only the caller knows
/// how they registered the instrument. Both must match that registration:
///
/// - `instrument` is the [`InstrumentIndex`] the instrument received in `IndexedInstruments`.
///   Positions and unrealised PnL are strictly index-scoped, so a wrong index attributes this
///   symbol's prices to a different instrument — silently.
/// - `exchange` must be the [`ExchangeId`] that instrument was registered under, since engine state
///   panics on a market event from an exchange it does not know. [`LseDataset::exchange_id`] gives
///   the variant a given dataset belongs to.
///
/// [`LseDataset::exchange_id`]: super::market::LseDataset::exchange_id
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LseCandleSource {
    /// Vault **display symbol** — `EUR/USD`, `AAPL`, `BP.L`, `ES.F`. Not a dataset slug.
    pub symbol: SmolStr,
    /// The engine-side key for this instrument.
    pub instrument: InstrumentIndex,
    /// The exchange this instrument was registered under.
    pub exchange: ExchangeId,
}

impl LseCandleSource {
    /// Create a candle source for one instrument.
    pub fn new(
        symbol: impl Into<SmolStr>,
        instrument: InstrumentIndex,
        exchange: ExchangeId,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            instrument,
            exchange,
        }
    }
}

/// Replay historical candles for N instruments as ONE time-ordered market stream.
///
/// This is the bridge between the per-symbol vault fetch and a consumer that wants a single feed —
/// a backtest harness above all, which exposes exactly one stream. Each source is fetched
/// independently, tagged with its own [`InstrumentIndex`], and k-way merged on `time_exchange`.
///
/// Nothing is buffered beyond one event per source, so memory is O(N) in the number of instruments
/// and O(1) in the length of the range. The whole thing is lazy: no request is issued until the
/// stream is polled.
///
/// # `time_exchange` is the candle's `close_time`
/// A bar enters the timeline when its period **ends**. Stamping the open would let a strategy act
/// on a completed bar at the instant its period began — lookahead, silently. The vault reports only
/// the open instant; `close_time` is derived library-side by
/// [`fetch_candles`](LseVaultClient::fetch_candles).
///
/// # Ordering
/// Each source is ascending by `close_time`, which is what the merge requires. Equal timestamps
/// across instruments resolve to the earlier entry in `sources`, so a given `sources` ordering
/// replays identically every time.
///
/// # Cost
/// One paged fetch per source, all in flight concurrently as the merge polls them. Against the
/// provider's shared allowance (`calls_per_minute`, `vault_concurrency`, both reported by
/// [`usage`](LseVaultClient::usage)) an N-instrument replay costs N concurrent paged fetches — and
/// re-running it re-fetches everything. For repeated runs over the same range, fetch once to local
/// storage and replay from that.
///
/// # Errors
/// A failed fetch on any source is forwarded immediately, ahead of buffered events from the others:
/// once one instrument's series is incomplete, the merged replay is no longer the dataset that was
/// asked for. [`LseError`](super::error::LseError) is flattened into
/// [`DataError::Lse`] — handle `LseError` at the [`fetch_candles`](LseVaultClient::fetch_candles)
/// level if you need to match on the cause.
///
/// # ⚠️ Licensing
/// The data this replays is **not redistributable**. See the [module docs](super).
pub fn replay_candles(
    client: Arc<LseVaultClient>,
    sources: Vec<LseCandleSource>,
    interval: CandleInterval,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> impl Stream<Item = Result<MarketStreamEvent<InstrumentIndex, DataKind>, DataError>> + Send + 'static
{
    merge_time_sorted(sources.into_iter().map(move |source| {
        tag_events(
            owned_candles(
                Arc::clone(&client),
                source.symbol.clone(),
                interval,
                start,
                end,
            ),
            source.exchange,
            source.instrument,
            |candle: &Candle| candle.close_time,
            DataKind::Candle,
        )
    }))
}

/// Drive [`fetch_candles`](LseVaultClient::fetch_candles) from an owned client and symbol.
///
/// `fetch_candles` borrows both, producing a stream tied to their lifetimes. Consumers such as
/// `BacktestMarketData` require a `'static` stream, so the borrow is moved inside the generator
/// here rather than pushed onto the caller.
fn owned_candles(
    client: Arc<LseVaultClient>,
    symbol: SmolStr,
    interval: CandleInterval,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> impl Stream<Item = Result<Candle, DataError>> + Send + 'static {
    try_stream! {
        let candles = client.fetch_candles(&symbol, interval, start, end);
        futures::pin_mut!(candles);

        while let Some(candle) = candles.next().await {
            yield candle.map_err(DataError::from)?;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panicking on a bad fixture is acceptable
mod tests {
    use super::*;
    use crate::event::MarketEvent;

    /// `(instrument index, time_exchange)` of each replayed candle.
    fn observed(
        events: &[Result<MarketStreamEvent<InstrumentIndex, DataKind>, DataError>],
    ) -> Vec<(usize, DateTime<Utc>)> {
        events
            .iter()
            .map(|event| match event {
                Ok(MarketStreamEvent::Item(MarketEvent {
                    time_exchange,
                    instrument,
                    kind: DataKind::Candle(_),
                    ..
                })) => (instrument.index(), *time_exchange),
                other => panic!("expected a candle Item, got {other:?}"),
            })
            .collect()
    }

    /// `count` one-minute candle rows on 2024-01-02, opening at `first_minute` and stepping by
    /// `step` minutes.
    fn rows(count: i64, first_minute: i64, step: i64) -> String {
        let rows: Vec<String> = (0..count)
            .map(|index| {
                let minute = first_minute + index * step;
                format!(
                    r#"{{"ts":"2024-01-02 {:02}:{:02}:00","open":1.0,"high":1.0,"low":1.0,"close":1.0,"volume":1}}"#,
                    minute / 60,
                    minute % 60
                )
            })
            .collect();

        format!("[{}]", rows.join(","))
    }

    /// Two symbols on interleaved minutes must come back strictly time-ordered and correctly
    /// attributed — the property a multi-instrument backtest depends on.
    #[tokio::test]
    async fn replay_merges_two_symbols_into_one_time_ordered_stream() {
        let server = wiremock::MockServer::start().await;

        // AAPL opens on even minutes, MSFT on odd, so a correct merge must alternate. Each symbol
        // serves one page; the fallback below then ends its pagination.
        for (symbol, first_minute) in [("AAPL", 0), ("MSFT", 1)] {
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .and(wiremock::matchers::path("/vault/candles"))
                .and(wiremock::matchers::query_param("symbol", symbol))
                .respond_with(
                    wiremock::ResponseTemplate::new(200)
                        .set_body_raw(rows(2, first_minute, 2), "application/json"),
                )
                .up_to_n_times(1)
                .mount(&server)
                .await;
        }

        // A short page is not a terminal signal (the row cap is applied silently), so pagination
        // continues until an empty page arrives.
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_raw("[]", "application/json"),
            )
            .mount(&server)
            .await;

        let client = Arc::new(
            LseVaultClient::new("key")
                .unwrap()
                .with_base_url(format!("{}/vault", server.uri())),
        );

        let events = replay_candles(
            client,
            vec![
                LseCandleSource::new("AAPL", InstrumentIndex::new(0), ExchangeId::LseEquities),
                LseCandleSource::new("MSFT", InstrumentIndex::new(1), ExchangeId::LseEquities),
            ],
            CandleInterval::Min1,
            "2024-01-01T00:00:00Z".parse().unwrap(),
            "2024-01-03T00:00:00Z".parse().unwrap(),
        )
        .collect::<Vec<_>>()
        .await;

        let observed = observed(&events);
        // Strictly ascending, and alternating between the two instruments.
        assert!(
            observed.windows(2).all(|pair| pair[0].1 <= pair[1].1),
            "merged replay must be time-ordered: {observed:?}"
        );
        assert_eq!(
            observed.iter().map(|(index, _)| *index).collect::<Vec<_>>(),
            vec![0, 1, 0, 1]
        );
    }

    #[tokio::test]
    async fn replay_of_no_sources_is_empty() {
        let client = Arc::new(LseVaultClient::new("key").unwrap());

        let events = replay_candles(
            client,
            vec![],
            CandleInterval::Min1,
            "2024-01-01T00:00:00Z".parse().unwrap(),
            "2024-01-03T00:00:00Z".parse().unwrap(),
        )
        .collect::<Vec<_>>()
        .await;

        assert!(events.is_empty());
    }

    /// A failed source must surface rather than silently shortening the replay.
    #[tokio::test]
    async fn replay_surfaces_a_source_failure() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(500)
                    .set_body_raw(r#"{"detail":"upstream unavailable"}"#, "application/json"),
            )
            .mount(&server)
            .await;

        let client = Arc::new(
            LseVaultClient::new("key")
                .unwrap()
                .with_base_url(format!("{}/vault", server.uri())),
        );

        let events = replay_candles(
            client,
            vec![LseCandleSource::new(
                "AAPL",
                InstrumentIndex::new(0),
                ExchangeId::LseEquities,
            )],
            CandleInterval::Min1,
            "2024-01-01T00:00:00Z".parse().unwrap(),
            "2024-01-03T00:00:00Z".parse().unwrap(),
        )
        .collect::<Vec<_>>()
        .await;

        assert!(matches!(events.first(), Some(Err(DataError::Lse(_)))));
    }
}
