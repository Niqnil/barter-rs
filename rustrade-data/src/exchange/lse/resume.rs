//! Gap-free resumption of a London Strategic Edge subscription across a reconnect.
//!
//! The provider serves a historical window before going live when a subscription carries a
//! `start`, which makes it possible to close the gap a reconnect would otherwise leave. This
//! module holds the state that survives a reconnect; [`transformer`](super::transformer) applies
//! it, and [`live`](super::live) sends it.

use super::channel::LseChannel;
use crate::{Identifier, exchange::ExchangeSub};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use rustrade_integration::subscription::SubscriptionId;
use std::sync::{Mutex, PoisonError};

/// The newest instant delivered for one subscription, and how many events already delivered bear
/// exactly that instant.
///
/// The count is what makes resumption exact. `start` is **inclusive**, so resuming at the newest
/// instant re-delivers every event sharing it; the count says how many of those were already seen,
/// so they can be skipped by position. Resuming one tick *later* instead would be simpler and
/// would silently lose every event at that instant not yet consumed.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(super) struct LseWatermark {
    /// The newest `time_exchange` delivered for this subscription.
    pub time_exchange: DateTime<Utc>,

    /// How many events already delivered carry exactly [`time_exchange`](Self::time_exchange).
    ///
    /// # This is a real count, not an "almost always 1" optimisation
    /// Tie density is per-dataset and can be large. Crypto timestamps are quantised to the
    /// millisecond, and **141 ticks sharing a single instant** were measured on one symbol; FX and
    /// CFD timestamps carry microseconds and were distinct on every tick sampled, making the count
    /// `1` there. Both are properties of today's feed rather than contract, which is why the
    /// arithmetic is written to be correct at any granularity.
    pub count_at_time: usize,
}

/// The key a watermark is filed under: the subscription the provider ticks it on, **and** the
/// subscription kind the stream serves it as.
///
/// # Why the kind is part of the key
/// The provider publishes one data frame, so a subscribe names a symbol and nothing else and both
/// supported kinds resolve to the same wire identifier (`tick|BTC/USD`). Keying on that identifier
/// alone would let two streams carrying one symbol — the same instrument subscribed as trades and
/// as top-of-book — share a single watermark and advance it independently: whichever stream ran
/// ahead would set the resume point for both, and the one behind would resume past events it never
/// delivered. Since this integration is required to serve both kinds from every dataset, that is
/// the natural way to use the feature rather than an exotic one, so it is closed structurally here
/// instead of being documented as a caller obligation.
///
/// The kind never reaches the wire: it is a partition of this map, not part of the subscription.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct LseResumeKey {
    subscription: SubscriptionId,
    kind: &'static str,
}

impl LseResumeKey {
    /// File a watermark under `subscription`, as served for `kind`.
    ///
    /// `kind` is [`SubscriptionKind::as_str`](crate::subscription::SubscriptionKind::as_str) —
    /// taken as a string rather than a type parameter because the map holds every kind at once.
    pub(super) fn new(subscription: SubscriptionId, kind: &'static str) -> Self {
        Self { subscription, kind }
    }

    /// The kind this key partitions on.
    pub(super) fn kind(&self) -> &'static str {
        self.kind
    }

    /// Consume the key for the subscription half, which is what a transformer keys its own
    /// per-connection bookkeeping on once it has filtered the snapshot to its own kind.
    pub(super) fn into_subscription(self) -> SubscriptionId {
        self.subscription
    }
}

/// Resume state shared between a [`LseSubscriber`](super::live::LseSubscriber) and the transformer
/// of every stream it opens.
///
/// # An opaque handle, deliberately
/// Construct it, hand it to
/// [`LseSubscriber::with_resume`](super::live::LseSubscriber::with_resume), and let delivery drive
/// it; there is no supported way to read or advance a watermark from outside. Reading one means
/// exposing the key it is filed under, and this provider's ticks are filed under a wire identifier
/// rather than anything a caller holds — publishing that identifier would freeze its spelling into
/// this contract and still hand back a map no caller could interpret, because deriving it is
/// one-way. A read surface keyed on the symbol is the shape to add if a caller ever needs one, and
/// adding it later against a stated need is a smaller commitment than taking one back.
///
/// # Reconnect, not crash recovery
/// The watermark advances on **delivered** events and lives in memory. It closes the gap a
/// reconnect opens; it does not survive the process. Recovering across a restart would require the
/// consumer to acknowledge what it durably stored, which is consumer policy and deliberately not
/// modelled here.
///
/// # One state may be shared freely
/// Watermarks are keyed by subscription *and* subscription kind, so one state serves any set of
/// streams, including two that carry the same symbol as different kinds: this provider serves both
/// kinds from a single data frame, so keying on the wire identifier alone would let whichever
/// stream ran ahead set the resume point for both, and the one behind would resume past events it
/// never delivered. Nothing about sharing needs thinking about at the call site.
///
/// # Opting in
/// Resumption is **off** unless [`LseSubscriber::with_resume`](super::live::LseSubscriber::with_resume)
/// is called, because the replay it triggers is not free: one hour of a single busy crypto symbol
/// replayed **107,395 ticks** before the first live tick, and a 24-hour window had not drained
/// after 30 seconds. A consumer that must stay current is better served by a gap than by a
/// multi-minute burst of backdated events; a consumer recording a continuous tape wants the
/// opposite. That is the caller's call to make, so it is not made here.
///
/// # Example
/// ```rust,ignore
/// use rustrade_data::exchange::lse::{live::LseSubscriber, resume::LseResumeState};
/// use std::sync::Arc;
///
/// let subscriber = LseSubscriber::from_env()?.with_resume(Arc::new(LseResumeState::new()));
/// ```
#[derive(Debug, Default)]
pub struct LseResumeState {
    // A plain `Mutex` rather than an `RwLock`: there are no concurrent readers to shard. The
    // reconnect chain polls the outer stream (where the subscriber reads) only once the inner
    // stream (where the transformer writes) has fully drained, so the two never overlap. The lock
    // is never held across an `await`.
    marks: Mutex<FnvHashMap<LseResumeKey, LseWatermark>>,
}

impl LseResumeState {
    /// Construct empty resume state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that an event bearing `time_exchange` was delivered under `key`.
    ///
    /// Advancing to a newer instant resets the count to one. An instant older than the watermark
    /// is ignored rather than rolling it back — the watermark is the high-water mark of what was
    /// delivered, and moving it backwards would re-request events already seen.
    pub(super) fn record(&self, key: &LseResumeKey, time_exchange: DateTime<Utc>) {
        let mut marks = self.lock();

        match marks.get_mut(key) {
            Some(watermark) if time_exchange > watermark.time_exchange => {
                *watermark = LseWatermark {
                    time_exchange,
                    count_at_time: 1,
                };
            }
            Some(watermark) if time_exchange == watermark.time_exchange => {
                watermark.count_at_time += 1;
            }
            Some(_) => {}
            None => {
                marks.insert(
                    key.clone(),
                    LseWatermark {
                        time_exchange,
                        count_at_time: 1,
                    },
                );
            }
        }
    }

    /// The watermark filed under `key`, if anything has been delivered for it.
    pub(super) fn watermark(&self, key: &LseResumeKey) -> Option<LseWatermark> {
        self.lock().get(key).copied()
    }

    /// Every watermark recorded so far, across every kind.
    ///
    /// Taken as a snapshot so a transformer can carry its own drop counters without holding the
    /// lock, or consulting it, per tick. The caller filters to its own kind.
    pub(super) fn snapshot(&self) -> FnvHashMap<LseResumeKey, LseWatermark> {
        self.lock().clone()
    }

    // A poisoned lock means some other thread panicked mid-update. The data behind it is a
    // high-water mark whose worst case is a slightly stale resume point, so recovering the inner
    // value is strictly better than propagating the panic and taking the market stream down.
    fn lock(&self) -> std::sync::MutexGuard<'_, FnvHashMap<LseResumeKey, LseWatermark>> {
        self.marks.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The [`SubscriptionId`] the provider's ticks for `symbol` arrive under.
///
/// Shared with the tick decoder's own construction of the same identifier so the subscriber and
/// the stream cannot disagree about which subscription a watermark belongs to.
pub(super) fn subscription_id(symbol: &str) -> SubscriptionId {
    ExchangeSub::from((LseChannel::Tick, symbol)).id()
}

/// Render an instant as the epoch-seconds number the provider's `start` parameter expects.
///
/// # Why a float, and why microseconds
/// The server parses `start` with a float conversion first and only falls back to a datetime
/// parse, which accepts neither a fractional second nor a UTC offset — so the epoch form is the
/// only spelling that can express a sub-second resume point at all.
///
/// Microseconds are the provider's own resolution and the resolution it filters on: a `start`
/// placed 400 microseconds past an instant was measured to exclude every tick at that instant,
/// which neither a millisecond-flooring nor a millisecond-rounding server could do. An `f64`'s
/// spacing at present-day epochs is roughly a quarter of a microsecond, so this round-trips the
/// stored instant exactly rather than approximately.
pub(super) fn epoch_seconds(time: DateTime<Utc>) -> f64 {
    // The conversion is the point of this function, and its loss is bounded well below the
    // microsecond the value carries -- see the rustdoc above.
    #[allow(clippy::cast_precision_loss)]
    let micros = time.timestamp_micros() as f64;

    micros / 1_000_000.0
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::subscription::{SubscriptionKind, book::OrderBooksL1, trade::PublicTrades};

    fn at(spelling: &str) -> DateTime<Utc> {
        spelling.parse::<DateTime<Utc>>().unwrap()
    }

    fn id() -> SubscriptionId {
        subscription_id("BTC/USD")
    }

    /// The key one symbol's trades are filed under.
    ///
    /// The kind comes from [`SubscriptionKind::as_str`] rather than a spelled-out literal, so
    /// these tests partition on the same string production files watermarks under.
    fn key() -> LseResumeKey {
        LseResumeKey::new(id(), PublicTrades.as_str())
    }

    fn key_for(symbol: &str) -> LseResumeKey {
        LseResumeKey::new(subscription_id(symbol), PublicTrades.as_str())
    }

    #[test]
    fn the_first_event_seeds_the_watermark_with_a_count_of_one() {
        let state = LseResumeState::new();
        state.record(&key(), at("2026-01-02T09:37:24.760146Z"));

        assert_eq!(
            state.watermark(&key()),
            Some(LseWatermark {
                time_exchange: at("2026-01-02T09:37:24.760146Z"),
                count_at_time: 1,
            })
        );
    }

    /// The whole point of carrying a count: repeats at one instant are ordinary on this feed, and
    /// resuming needs to know how many of them were already delivered.
    #[test]
    fn events_sharing_an_instant_accumulate_a_count() {
        let state = LseResumeState::new();
        for _ in 0..141 {
            state.record(&key(), at("2026-01-02T09:37:24.760Z"));
        }

        assert_eq!(state.watermark(&key()).unwrap().count_at_time, 141);
    }

    #[test]
    fn a_newer_instant_advances_the_watermark_and_resets_the_count() {
        let state = LseResumeState::new();
        state.record(&key(), at("2026-01-02T09:37:24.760146Z"));
        state.record(&key(), at("2026-01-02T09:37:24.760146Z"));
        state.record(&key(), at("2026-01-02T09:37:24.760147Z"));

        assert_eq!(
            state.watermark(&key()),
            Some(LseWatermark {
                time_exchange: at("2026-01-02T09:37:24.760147Z"),
                count_at_time: 1,
            })
        );
    }

    /// Rolling the watermark backwards would re-request events already delivered.
    #[test]
    fn an_older_instant_leaves_the_watermark_untouched() {
        let state = LseResumeState::new();
        state.record(&key(), at("2026-01-02T09:37:24.760147Z"));
        state.record(&key(), at("2026-01-02T09:37:24.760146Z"));

        assert_eq!(
            state.watermark(&key()),
            Some(LseWatermark {
                time_exchange: at("2026-01-02T09:37:24.760147Z"),
                count_at_time: 1,
            })
        );
    }

    #[test]
    fn subscriptions_are_tracked_independently() {
        let state = LseResumeState::new();
        state.record(&key_for("BTC/USD"), at("2026-01-02T09:00:00Z"));
        state.record(&key_for("ETH/USD"), at("2026-01-02T10:00:00Z"));

        assert_eq!(
            state.watermark(&key_for("BTC/USD")).unwrap().time_exchange,
            at("2026-01-02T09:00:00Z")
        );
        assert_eq!(
            state.watermark(&key_for("ETH/USD")).unwrap().time_exchange,
            at("2026-01-02T10:00:00Z")
        );
    }

    /// The provider has one channel, so both kinds tick under one wire identifier. Sharing a
    /// watermark between them would let whichever stream ran ahead set the resume point for both,
    /// and the one behind would resume past events it never delivered.
    #[test]
    fn one_symbol_served_as_two_kinds_keeps_two_watermarks() {
        let state = LseResumeState::new();
        let trades = LseResumeKey::new(id(), PublicTrades.as_str());
        let books = LseResumeKey::new(id(), OrderBooksL1.as_str());

        state.record(&trades, at("2026-01-02T09:00:00Z"));
        state.record(&trades, at("2026-01-02T10:00:00Z"));
        state.record(&books, at("2026-01-02T09:00:00Z"));

        assert_eq!(
            state.watermark(&trades).unwrap().time_exchange,
            at("2026-01-02T10:00:00Z"),
        );
        assert_eq!(
            state.watermark(&books).unwrap().time_exchange,
            at("2026-01-02T09:00:00Z"),
            "the trailing kind must resume from its own last delivery, not the other kind's",
        );
    }

    #[test]
    fn an_untracked_subscription_has_no_watermark() {
        assert_eq!(LseResumeState::new().watermark(&key()), None);
    }

    /// The resume value must round-trip the stored instant exactly, because the server filters on
    /// it at microsecond resolution and the drop counter compares instants for equality.
    #[test]
    fn the_epoch_form_round_trips_an_instant_to_the_microsecond() {
        for spelling in [
            "2026-01-02T09:37:24.760146Z",
            "2026-08-14T10:16:55.161000Z",
            "2026-08-14T10:16:55.161999Z",
            "2026-01-02T09:37:24Z",
        ] {
            let instant = at(spelling);

            // Compared in the float domain rather than converting back, so the assertion needs no
            // truncating cast of its own.
            #[allow(clippy::cast_precision_loss)] // The bounded loss the function documents
            let expected = instant.timestamp_micros() as f64;
            let drift = epoch_seconds(instant) * 1_000_000.0 - expected;

            assert!(drift.abs() < 0.5, "{spelling} drifted {drift} microseconds");
        }
    }

    /// The tick decoder builds this identifier independently during deserialisation; if the two
    /// ever disagreed, every watermark would be filed under a subscription that never reads it.
    #[test]
    fn the_subscription_id_matches_the_one_the_tick_decoder_builds() {
        let tick: super::super::tick::LseMessage = serde_json::from_str(
            r#"{"type":"tick","symbol":"BTC/USD","ts":"2026-01-02T09:37:24.760146+00:00",
                "price":42000.5,"bid":42000.5,"ask":42001.0,"volume":0.00155}"#,
        )
        .unwrap();

        let super::super::tick::LseMessage::Tick(tick) = tick else {
            panic!("expected a tick");
        };

        assert_eq!(tick.subscription_id, subscription_id("BTC/USD"));
    }
}
