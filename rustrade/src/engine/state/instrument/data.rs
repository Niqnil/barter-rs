use crate::{
    Timed,
    engine::{Processor, state::order::in_flight_recorder::InFlightRequestRecorder},
};
use derive_more::Constructor;
use rust_decimal::Decimal;
use rustrade_data::{
    event::{DataKind, MarketEvent},
    subscription::{book::OrderBookL1, candle::Candle},
};
use rustrade_execution::{
    AccountEvent,
    order::request::{OrderRequestCancel, OrderRequestOpen},
};
use rustrade_instrument::{
    asset::AssetIndex, exchange::ExchangeIndex, instrument::InstrumentIndex,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Defines a state object for tracking and managing custom instrument level data.
///
/// Implementations must handle market event & account event processing, as well as logic for
/// determining the latest instrument market price.
///
/// This trait enables users to define their own instrument level data, and specify the type of
/// [`MarketEvent`] that is required to update it. The custom instrument data could include
/// market data, strategy-specific data, risk-specific data, or any other instrument level data.
///
/// For an example, see the [`DefaultInstrumentMarketData`] implementation.
pub trait InstrumentDataState<
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
> where
    Self: Debug
        + Clone
        + for<'a> Processor<&'a MarketEvent<InstrumentKey, Self::MarketEventKind>>
        + for<'a> Processor<&'a AccountEvent<ExchangeKey, AssetKey, InstrumentKey>>
        + InFlightRequestRecorder<ExchangeKey, InstrumentKey>,
{
    /// [`MarketEvent<_, EventKind>`](MarketEvent) expected by this instrument data state.
    type MarketEventKind: Debug + Clone + Send;

    /// Latest price for an instrument, if available.
    ///
    /// Return the latest market price for an instrument, if available.
    ///
    /// An instrument price could be derived in many ways, but some common examples include:
    /// - Most recent `PublicTrade` price.
    /// - Volume-weighted mid-price from an `OrderBookL1`.
    /// - Close of the most recent `Candle`.
    /// - Volume-weighted mid-price from an `OrderBookL2`.
    fn price(&self) -> Option<Decimal>;
}

/// Basic [`InstrumentDataState`] implementation that tracks the [`OrderBookL1`], last traded price,
/// and last [`Candle`] for an instrument.
///
/// This is a simple example of instrument level data. Trading strategies typically maintain more
/// comprehensive data, such as rolling candle windows, technical indicators, market depth (L2 book),
/// volatility metrics, or strategy-specific state data.
///
/// The whole [`Candle`] is retained rather than just its close, since strategies consuming a
/// candle feed generally want OHLCV.
///
/// # Which [`DataKind`]s update this state
/// [`Trade`](DataKind::Trade), [`OrderBookL1`](DataKind::OrderBookL1) and
/// [`Candle`](DataKind::Candle) are price inputs. [`OrderBook`](DataKind::OrderBook) (L2),
/// [`Liquidation`](DataKind::Liquidation) and [`OptionGreeks`](DataKind::OptionGreeks) are
/// deliberately ignored — see the [`Processor`] impl, where each exclusion is stated with its
/// reason.
///
/// Does not implement `Ord`/`PartialOrd`: `last_traded_price` holds a [`Timed`] value, whose
/// whole-struct ordering is intentionally not provided (see [`Timed`] docs).
#[derive(Debug, Clone, Eq, PartialEq, Hash, Default, Deserialize, Serialize, Constructor)]
pub struct DefaultInstrumentMarketData {
    pub l1: OrderBookL1,
    pub last_traded_price: Option<Timed<Decimal>>,
    /// Most recent [`Candle`], ordered on its
    /// [`close_time`](rustrade_data::subscription::candle::Candle::close_time).
    pub candle: Option<Candle>,
}

impl InstrumentDataState for DefaultInstrumentMarketData {
    type MarketEventKind = DataKind;

    /// Latest price, resolved by an explicit precedence rule.
    ///
    /// 1. The [`OrderBookL1`] volume-weighted mid-price, whenever one is available. L1 is the
    ///    finest-grained view of the current market this state holds, and it wins
    ///    **unconditionally** — not by recency. This preserves the behaviour that predates candle
    ///    support.
    /// 2. Otherwise the **more recent** of the last [`Candle`] (by `close_time`) and the last
    ///    traded price (by its [`Timed::time`]). A trade wins an exact tie: `close_time` is the
    ///    *exclusive* period end, so a trade stamped at that instant belongs to the next period and
    ///    is therefore the fresher observation.
    /// 3. `None` if the instrument has received no price input at all.
    ///
    /// Recency — rather than a fixed "candle beats trade" — is what stops a coarse bar from
    /// shadowing fresher data on a mixed feed: a `1d` candle stamped this morning would otherwise
    /// silently outrank every trade tick received since. On a feed carrying candles *or* trades for
    /// a given instrument (the common case) the rule is inert.
    fn price(&self) -> Option<Decimal> {
        if let Some(l1_price) = self.l1.volume_weighed_mid_price() {
            return Some(l1_price);
        }

        match (&self.candle, &self.last_traded_price) {
            (Some(candle), Some(trade)) => Some(if candle.close_time > trade.time {
                candle.close
            } else {
                trade.value
            }),
            (Some(candle), None) => Some(candle.close),
            (None, Some(trade)) => Some(trade.value),
            (None, None) => None,
        }
    }
}

impl<InstrumentKey> Processor<&MarketEvent<InstrumentKey, DataKind>>
    for DefaultInstrumentMarketData
{
    type Audit = ();

    /// Apply a market event, ignoring any that is older than what is already held.
    ///
    /// The match is **exhaustive on purpose**: a catch-all is how `DataKind::Candle` went unhandled
    /// here for as long as it did — silently, with no compile error to prompt the edit. Each
    /// excluded variant states why it is not a price input, so a new variant forces a deliberate
    /// decision rather than inheriting a default of "ignore".
    fn process(&mut self, event: &MarketEvent<InstrumentKey, DataKind>) -> Self::Audit {
        match &event.kind {
            DataKind::Trade(trade) => {
                if self
                    .last_traded_price
                    .as_ref()
                    .is_none_or(|price| price.time < event.time_exchange)
                {
                    self.last_traded_price
                        .replace(Timed::new(trade.price, event.time_exchange));
                }
            }
            DataKind::OrderBookL1(l1) => {
                if self.l1.last_update_time < event.time_exchange {
                    self.l1 = l1.clone()
                }
            }
            // Ordered on `close_time`, not `event.time_exchange`, so the staleness guard and
            // `price()`'s precedence rule key on the same instant. A well-formed producer sets
            // `time_exchange` to `close_time` anyway (see `Candle`'s docs); keying on the payload
            // means a producer that does not cannot desynchronise the two.
            DataKind::Candle(candle) => {
                if self
                    .candle
                    .is_none_or(|current| current.close_time < candle.close_time)
                {
                    self.candle = Some(*candle);
                }
            }
            // L2 depth is excluded, not overlooked. Deriving a mid-price from a full book is a real
            // gap (named in `InstrumentDataState::price`'s docs), but satisfying this struct's
            // `Hash` derive would mean threading `Hash` through the entire order book type chain —
            // invasive, and hashing a depth book is close to meaningless. Tracked separately.
            DataKind::OrderBook(_) => {}
            // A liquidation is a FORCED fill at a potentially dislocated price, not a market
            // consensus. Feeding it to `price()` would corrupt `pnl_unrealised`, which is
            // recomputed from that price on every market event. "Handle every variant" is actively
            // wrong here.
            DataKind::Liquidation(_) => {}
            // Greeks are risk sensitivities, not a price, and belong in a dedicated option state
            // rather than a struct cloned per-instrument for every non-option user.
            DataKind::OptionGreeks(_) => {}
        }
    }
}

impl<ExchangeKey, AssetKey, InstrumentKey>
    Processor<&AccountEvent<ExchangeKey, AssetKey, InstrumentKey>> for DefaultInstrumentMarketData
{
    type Audit = ();

    fn process(&mut self, _: &AccountEvent<ExchangeKey, AssetKey, InstrumentKey>) -> Self::Audit {}
}

impl<ExchangeKey, InstrumentKey> InFlightRequestRecorder<ExchangeKey, InstrumentKey>
    for DefaultInstrumentMarketData
{
    fn record_in_flight_cancel(&mut self, _: &OrderRequestCancel<ExchangeKey, InstrumentKey>) {}

    fn record_in_flight_open(&mut self, _: &OrderRequestOpen<ExchangeKey, InstrumentKey>) {}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panicking on a bad fixture is acceptable
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_data::{
        books::Level,
        subscription::{liquidation::Liquidation, trade::PublicTrade},
    };
    use rustrade_instrument::{Side, instrument::InstrumentIndex};

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn event(time: DateTime<Utc>, kind: DataKind) -> MarketEvent<InstrumentIndex, DataKind> {
        MarketEvent {
            time_exchange: time,
            time_received: time,
            exchange: rustrade_instrument::exchange::ExchangeId::BinanceSpot,
            instrument: InstrumentIndex::new(0),
            kind,
        }
    }

    fn trade(price: Decimal) -> DataKind {
        DataKind::Trade(PublicTrade {
            id: "1".into(),
            price,
            amount: dec!(1),
            side: Some(Side::Buy),
        })
    }

    /// A candle closing at `close_time` with `close` as its close price.
    fn candle(close_time: DateTime<Utc>, close: Decimal) -> DataKind {
        DataKind::Candle(Candle {
            close_time,
            open: close,
            high: close,
            low: close,
            close,
            volume: Some(dec!(1)),
            trade_count: Some(1),
        })
    }

    /// An L1 with a symmetric book, so the volume-weighted mid-price is exactly `price`.
    fn l1(price: Decimal) -> DataKind {
        DataKind::OrderBookL1(OrderBookL1 {
            last_update_time: at(0),
            best_bid: Some(Level::new(price, dec!(1))),
            best_ask: Some(Level::new(price, dec!(1))),
        })
    }

    #[test]
    fn price_is_none_without_any_input() {
        assert_eq!(DefaultInstrumentMarketData::default().price(), None);
    }

    #[test]
    fn l1_wins_unconditionally_over_newer_candle_and_trade() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(10), l1(dec!(100))));
        data.process(&event(at(20), trade(dec!(200))));
        data.process(&event(at(30), candle(at(30), dec!(300))));

        assert_eq!(data.price(), Some(dec!(100)));
    }

    #[test]
    fn newer_candle_beats_older_trade() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(10), trade(dec!(200))));
        data.process(&event(at(20), candle(at(20), dec!(300))));

        assert_eq!(data.price(), Some(dec!(300)));
    }

    #[test]
    fn newer_trade_beats_older_candle() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(20), candle(at(20), dec!(300))));
        data.process(&event(at(30), trade(dec!(200))));

        assert_eq!(data.price(), Some(dec!(200)));
    }

    /// A coarse bar must not shadow a fresher tick — the reason precedence is recency-based rather
    /// than a fixed "candle beats trade".
    #[test]
    fn stale_coarse_candle_does_not_shadow_fresh_trade() {
        let mut data = DefaultInstrumentMarketData::default();
        // A daily bar that closed at t=100, then a tick a long while later.
        data.process(&event(at(100), candle(at(100), dec!(300))));
        data.process(&event(at(86_500), trade(dec!(200))));

        assert_eq!(data.price(), Some(dec!(200)));
    }

    /// `close_time` is the exclusive period end, so a trade stamped on it belongs to the next
    /// period and is the fresher observation.
    #[test]
    fn trade_wins_exact_tie_with_candle_close_time() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(20), candle(at(20), dec!(300))));
        data.process(&event(at(20), trade(dec!(200))));

        assert_eq!(data.price(), Some(dec!(200)));
    }

    #[test]
    fn stale_candle_does_not_replace_newer_stored_candle() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(30), candle(at(30), dec!(300))));
        data.process(&event(at(20), candle(at(20), dec!(999))));

        assert_eq!(data.candle.unwrap().close_time, at(30));
        assert_eq!(data.price(), Some(dec!(300)));
    }

    #[test]
    fn stale_trade_does_not_replace_newer_stored_trade() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(30), trade(dec!(300))));
        data.process(&event(at(20), trade(dec!(999))));

        assert_eq!(data.price(), Some(dec!(300)));
    }

    /// A liquidation is a forced fill at a potentially dislocated price; admitting it would corrupt
    /// `pnl_unrealised`.
    #[test]
    fn excluded_variants_do_not_move_price() {
        let mut data = DefaultInstrumentMarketData::default();
        data.process(&event(at(10), trade(dec!(200))));

        data.process(&event(
            at(20),
            DataKind::Liquidation(Liquidation {
                side: Side::Buy,
                price: dec!(999),
                quantity: dec!(1),
                time: at(20),
            }),
        ));

        assert_eq!(data.price(), Some(dec!(200)));
        assert_eq!(data.candle, None);
    }
}
