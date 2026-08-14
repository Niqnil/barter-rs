//! Decode a London Strategic Edge tick as an [`OrderBookL1`].

use super::tick::LseMessage;
use crate::{
    books::Level,
    event::{MarketEvent, MarketIter},
    subscription::book::OrderBookL1,
};
use chrono::Utc;
use rust_decimal::Decimal;
use rustrade_instrument::exchange::ExchangeId;

/// Decode a tick frame as a top-of-book quote.
///
/// This is the mapping the frame fits most honestly: the tick carries a bid and an ask, and its
/// `price` equals the bid on every sample taken. The trade mapping in
/// [`trade`](super::trade) exists alongside it and is documented as the approximation it is.
///
/// # Both levels carry a ZERO size
/// The feed publishes no bid or ask size — only prices — so the sizes here are placeholders, not
/// measurements. This is the same choice the provider's bulk-export decoder makes, for the same
/// reason: a fabricated size would be indistinguishable downstream from a real one. It is safe
/// because `DefaultInstrumentMarketData::price` in the `rustrade` crate falls back from the
/// volume-weighted mid to the plain mid when the sizes are absent, so a zero-size book prices
/// correctly rather than not at all. Consumers that genuinely need depth must not read these as
/// available quantity.
///
/// The tick's `volume` is deliberately **not** used for either level: it is a traded size, not
/// resting bid or ask liquidity, and on two of the five venues it is a fabricated constant. See
/// [`trade`](super::trade) for that warning in full.
impl<InstrumentKey> From<(ExchangeId, InstrumentKey, LseMessage)>
    for MarketIter<InstrumentKey, OrderBookL1>
{
    fn from((exchange, instrument, message): (ExchangeId, InstrumentKey, LseMessage)) -> Self {
        let LseMessage::Tick(tick) = message else {
            // See the matching arm in the trade decoder: unreachable through the transformer, and
            // an empty result is the only correct answer if it is ever reached anyway.
            return Self(vec![]);
        };

        Self(vec![Ok(MarketEvent {
            time_exchange: tick.time_exchange,
            time_received: Utc::now(),
            exchange,
            instrument,
            kind: OrderBookL1 {
                // Must equal the event's `time_exchange` -- downstream state orders L1 updates on
                // this field rather than on the event's, and a producer that lets the two diverge
                // gets no error and no log.
                last_update_time: tick.time_exchange,
                best_bid: Some(Level::new(tick.bid, Decimal::ZERO)),
                best_ask: Some(Level::new(tick.ask, Decimal::ZERO)),
            },
        })])
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    const TICK: &str = r#"{"type":"tick","symbol":"BTC/USD",
        "ts":"2026-01-02T09:37:24.760146+00:00","price":42000.5,"bid":42000.5,
        "ask":42001.0,"volume":0.00155}"#;

    fn decode(json: &str) -> Vec<MarketEvent<u8, OrderBookL1>> {
        let message: LseMessage = serde_json::from_str(json).unwrap();
        MarketIter::<u8, OrderBookL1>::from((ExchangeId::LseCrypto, 1_u8, message))
            .0
            .into_iter()
            .map(Result::unwrap)
            .collect()
    }

    #[test]
    fn a_tick_decodes_to_a_two_sided_book() {
        let events = decode(TICK);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind.best_bid.unwrap().price, dec!(42000.5));
        assert_eq!(events[0].kind.best_ask.unwrap().price, dec!(42001.0));
    }

    /// The producer obligation stated on the field: downstream state ranks L1 updates on
    /// `last_update_time`, so a divergence from the event's own instant silently freezes the book.
    #[test]
    fn the_book_instant_equals_the_events_instant() {
        let events = decode(TICK);
        assert_eq!(events[0].kind.last_update_time, events[0].time_exchange);
    }

    /// The feed publishes no sizes. A fabricated one would be indistinguishable from a real one.
    #[test]
    fn both_levels_carry_a_zero_size_because_none_is_published() {
        let events = decode(TICK);

        assert_eq!(events[0].kind.best_bid.unwrap().amount, Decimal::ZERO);
        assert_eq!(events[0].kind.best_ask.unwrap().amount, Decimal::ZERO);
    }

    /// The traded size is not resting liquidity, and on two venues it is a fabricated constant.
    #[test]
    fn the_tick_volume_is_not_used_as_a_level_size() {
        let events = decode(TICK);
        assert_ne!(events[0].kind.best_bid.unwrap().amount, dec!(0.00155));
    }

    #[test]
    fn a_control_frame_yields_no_books() {
        let events = decode(r#"{"type":"replay_complete","symbol":"BTC/USD","rows":41}"#);
        assert!(events.is_empty());
    }
}
