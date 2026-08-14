//! The London Strategic Edge stream transformer.
//!
//! Decoding is delegated wholesale to [`StatelessTransformer`] — one provider frame maps to one
//! event and nothing about it needs state. What this type adds is the consuming half of
//! [`resume`](super::resume): skipping the events a reconnect's replay re-delivers.

use super::{
    resume::{LseResumeState, LseWatermark},
    tick::LseMessage,
};
use crate::{
    Identifier,
    error::DataError,
    event::{MarketEvent, MarketIter},
    exchange::Connector,
    subscription::{Map, SubscriptionKind},
    transformer::{ExchangeTransformer, stateless::StatelessTransformer},
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use serde::Deserialize;
use std::{cmp::Ordering, sync::Arc};
use tokio::sync::mpsc;
use tracing::warn;

/// Transformer for every London Strategic Edge subscription kind.
///
/// Carries no resume state unless the caller opted in via
/// [`LseSubscriber::with_resume`](super::live::LseSubscriber::with_resume); without it this is a
/// direct pass-through to [`StatelessTransformer`].
#[derive(Debug)]
pub struct LseTransformer<Exchange, InstrumentKey, Kind> {
    inner: StatelessTransformer<Exchange, InstrumentKey, Kind, LseMessage>,
    resume: Option<ResumeTracker>,
}

/// Per-connection resume bookkeeping: the shared watermark, and what this connection still owes
/// the replay window.
#[derive(Debug)]
struct ResumeTracker {
    state: Arc<LseResumeState>,
    pending: FnvHashMap<SubscriptionId, PendingDrop>,
}

/// Events still expected to arrive twice for one subscription, and the instant they carry.
#[derive(Copy, Clone, Debug)]
struct PendingDrop {
    time_exchange: DateTime<Utc>,
    remaining: usize,
}

impl<Exchange, InstrumentKey, Kind> LseTransformer<Exchange, InstrumentKey, Kind>
where
    Exchange: Connector + Send,
    InstrumentKey: Clone + Send + Sync,
    Kind: SubscriptionKind + Send,
    Kind::Event: Sync,
    MarketIter<InstrumentKey, Kind::Event>: From<(ExchangeId, InstrumentKey, LseMessage)>,
{
    /// Construct a transformer, optionally resuming from previously delivered events.
    ///
    /// The drop counters are snapshotted here rather than consulted per tick, so the shared lock
    /// is taken once per connection on this path.
    pub(super) async fn new(
        instrument_map: Map<InstrumentKey>,
        ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
        resume: Option<Arc<LseResumeState>>,
    ) -> Result<Self, DataError> {
        let inner = StatelessTransformer::init(instrument_map, &[], ws_sink_tx).await?;

        let resume = resume.map(|state| ResumeTracker {
            pending: state
                .snapshot()
                .into_iter()
                .map(|(subscription_id, watermark)| (subscription_id, PendingDrop::from(watermark)))
                .collect(),
            state,
        });

        Ok(Self { inner, resume })
    }
}

impl From<LseWatermark> for PendingDrop {
    fn from(watermark: LseWatermark) -> Self {
        Self {
            time_exchange: watermark.time_exchange,
            remaining: watermark.count_at_time,
        }
    }
}

impl<Exchange, InstrumentKey, Kind> ExchangeTransformer<Exchange, InstrumentKey, Kind>
    for LseTransformer<Exchange, InstrumentKey, Kind>
where
    Exchange: Connector + Send,
    InstrumentKey: Clone + Send + Sync,
    Kind: SubscriptionKind + Send,
    Kind::Event: Sync,
    MarketIter<InstrumentKey, Kind::Event>: From<(ExchangeId, InstrumentKey, LseMessage)>,
{
    /// Initialise without resumption.
    ///
    /// This is the trait's only seam and it is a static function, so it cannot carry the caller's
    /// resume state; the stream that wants resumption bypasses it and constructs the transformer
    /// directly. Initial snapshots are ignored: this provider serves a tick stream with no book to
    /// synchronise against, so its streams fetch none.
    async fn init(
        instrument_map: Map<InstrumentKey>,
        _: &[MarketEvent<InstrumentKey, Kind::Event>],
        ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        Self::new(instrument_map, ws_sink_tx, None).await
    }
}

impl<Exchange, InstrumentKey, Kind> Transformer for LseTransformer<Exchange, InstrumentKey, Kind>
where
    Exchange: Connector,
    InstrumentKey: Clone,
    Kind: SubscriptionKind,
    MarketIter<InstrumentKey, Kind::Event>: From<(ExchangeId, InstrumentKey, LseMessage)>,
{
    type Error = DataError;
    type Input = LseMessage;
    type Output = MarketEvent<InstrumentKey, Kind::Event>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        // Nothing to track when the caller did not opt in, and a control frame carries no instant
        // to track. Both go straight through.
        let Some((subscription_id, time_exchange)) =
            self.resume.as_ref().and_then(|_| match &input {
                LseMessage::Tick(tick) => Some((tick.subscription_id.clone(), tick.time_exchange)),
                LseMessage::Other => None,
            })
        else {
            return self.inner.transform(input);
        };

        if let Some(resume) = self.resume.as_mut()
            && resume.should_drop(&subscription_id, time_exchange)
        {
            return vec![];
        }

        let output = self.inner.transform(input);

        // Record against what was actually emitted. An unidentifiable instrument yields an error
        // rather than an event, and a watermark advanced past an event nobody received would
        // leave a gap on the next reconnect.
        if let Some(resume) = self.resume.as_ref()
            && output.iter().any(Result::is_ok)
        {
            resume.state.record(&subscription_id, time_exchange);
        }

        output
    }
}

impl ResumeTracker {
    /// Whether this event was already delivered by the connection this one replaced.
    fn should_drop(
        &mut self,
        subscription_id: &SubscriptionId,
        time_exchange: DateTime<Utc>,
    ) -> bool {
        let Some(pending) = self.pending.get(subscription_id).copied() else {
            return false;
        };

        match time_exchange.cmp(&pending.time_exchange) {
            // Inside the instant the watermark names, and the previous connection is known to have
            // delivered this many events there. `start` is inclusive, so the provider replays them
            // all and they are skipped by position.
            Ordering::Equal if pending.remaining > 0 => {
                if let Some(pending) = self.pending.get_mut(subscription_id) {
                    pending.remaining -= 1;
                }
                true
            }
            // The instant is exhausted; anything further carrying it is genuinely new.
            Ordering::Equal => false,
            Ordering::Greater => {
                self.pending.remove(subscription_id);

                // The replay held fewer events at the watermark's instant than were delivered from
                // it. Positional skipping assumes the replay reproduces what went out live -- an
                // assumption that holds on every measurement taken, including one instant carrying
                // 141 events -- so a shortfall means the provider's history changed underneath the
                // subscription. No event is lost by it, but the assumption is load-bearing enough
                // that its failure must be visible rather than inferred from a gap much later.
                if pending.remaining > 0 {
                    warn!(
                        subscription = %subscription_id,
                        instant = %pending.time_exchange,
                        undelivered = pending.remaining,
                        "London Strategic Edge replay held fewer ticks at the resume instant than \
                         were delivered from it; the provider's history changed under the resume",
                    );
                }

                false
            }
            // Older than the watermark. `start` is inclusive, so the provider should never send
            // this; emitting is the safe answer, since dropping would discard a real event on the
            // strength of an assumption that has already been contradicted.
            Ordering::Less => false,
        }
    }
}

/// Bound required by [`StatelessTransformer`]'s decode path, restated here so this module's own
/// bounds stay legible.
const _: fn() = || {
    fn assert_identifiable<T: Identifier<Option<SubscriptionId>> + for<'de> Deserialize<'de>>() {}
    assert_identifiable::<LseMessage>();
};

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{exchange::lse::LseCrypto, subscription::trade::PublicTrades};
    use rustrade_integration::subscription::SubscriptionId;

    type Subject = LseTransformer<LseCrypto, u8, PublicTrades>;

    fn id() -> SubscriptionId {
        super::super::resume::subscription_id("BTC/USD")
    }

    fn tick(ts: &str, price: &str) -> LseMessage {
        serde_json::from_str(&format!(
            r#"{{"type":"tick","symbol":"BTC/USD","ts":"{ts}","price":{price},
                "bid":{price},"ask":42001.0,"volume":0.00155}}"#
        ))
        .unwrap()
    }

    async fn subject(resume: Option<Arc<LseResumeState>>) -> Subject {
        let map = Map([(id(), 1_u8)]
            .into_iter()
            .collect::<FnvHashMap<SubscriptionId, u8>>());
        let (tx, _rx) = mpsc::unbounded_channel();

        Subject::new(map, tx, resume).await.unwrap()
    }

    #[tokio::test]
    async fn without_resume_every_tick_is_emitted() {
        let mut subject = subject(None).await;

        assert_eq!(
            subject.transform(tick("2026-01-02T09:00:00Z", "1.0")).len(),
            1
        );
        assert_eq!(
            subject.transform(tick("2026-01-02T09:00:00Z", "1.0")).len(),
            1
        );
    }

    #[tokio::test]
    async fn with_resume_the_watermark_advances_on_emitted_ticks() {
        let state = Arc::new(LseResumeState::new());
        let mut subject = subject(Some(Arc::clone(&state))).await;

        subject.transform(tick("2026-01-02T09:00:00.000001Z", "1.0"));
        subject.transform(tick("2026-01-02T09:00:00.000001Z", "2.0"));

        let watermark = state.watermark(&id()).unwrap();
        assert_eq!(
            watermark.time_exchange,
            "2026-01-02T09:00:00.000001Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
        assert_eq!(watermark.count_at_time, 2);
    }

    /// The core of the mechanism: a reconnect replays the whole of the watermark's instant because
    /// `start` is inclusive, and exactly the already-delivered prefix is skipped.
    #[tokio::test]
    async fn a_replay_skips_exactly_the_ticks_already_delivered_at_the_resume_instant() {
        let state = Arc::new(LseResumeState::new());

        // First connection delivers three ticks sharing one instant.
        let mut first = subject(Some(Arc::clone(&state))).await;
        for price in ["1.0", "2.0", "3.0"] {
            first.transform(tick("2026-01-02T09:00:00.000001Z", price));
        }
        assert_eq!(state.watermark(&id()).unwrap().count_at_time, 3);

        // Reconnect: the provider replays all three, then a fourth that is genuinely new.
        let mut second = subject(Some(Arc::clone(&state))).await;
        for price in ["1.0", "2.0", "3.0"] {
            assert!(
                second
                    .transform(tick("2026-01-02T09:00:00.000001Z", price))
                    .is_empty(),
                "an already-delivered tick must not be emitted twice"
            );
        }

        assert_eq!(
            second
                .transform(tick("2026-01-02T09:00:00.000001Z", "4.0"))
                .len(),
            1,
            "a fourth tick at the same instant is new and must be emitted"
        );
        assert_eq!(state.watermark(&id()).unwrap().count_at_time, 4);
    }

    /// The gap half of the contract: everything after the resume instant is new, always.
    #[tokio::test]
    async fn a_replay_emits_every_tick_past_the_resume_instant() {
        let state = Arc::new(LseResumeState::new());

        let mut first = subject(Some(Arc::clone(&state))).await;
        first.transform(tick("2026-01-02T09:00:00.000001Z", "1.0"));

        let mut second = subject(Some(Arc::clone(&state))).await;
        assert!(
            second
                .transform(tick("2026-01-02T09:00:00.000001Z", "1.0"))
                .is_empty()
        );
        assert_eq!(
            second
                .transform(tick("2026-01-02T09:00:00.000002Z", "2.0"))
                .len(),
            1,
            "one microsecond later is past the resume instant and must be emitted"
        );
    }

    /// The drop window closes for good once the instant is passed, so a later tick that happens to
    /// carry the resume instant again cannot be swallowed.
    #[tokio::test]
    async fn the_drop_window_does_not_reopen_after_the_resume_instant_is_passed() {
        let state = Arc::new(LseResumeState::new());

        let mut first = subject(Some(Arc::clone(&state))).await;
        first.transform(tick("2026-01-02T09:00:00.000001Z", "1.0"));
        first.transform(tick("2026-01-02T09:00:00.000001Z", "2.0"));

        let mut second = subject(Some(Arc::clone(&state))).await;
        // Replay is short by one, then moves past the instant -- the window closes here.
        assert!(
            second
                .transform(tick("2026-01-02T09:00:00.000001Z", "1.0"))
                .is_empty()
        );
        assert_eq!(
            second
                .transform(tick("2026-01-02T09:00:00.000002Z", "2.0"))
                .len(),
            1
        );
        assert_eq!(
            second
                .transform(tick("2026-01-02T09:00:00.000001Z", "3.0"))
                .len(),
            1,
            "the window is closed; a tick at the old instant must be emitted, not dropped"
        );
    }

    /// A subscription with no history resumes as a fresh subscription would.
    #[tokio::test]
    async fn a_subscription_with_no_watermark_drops_nothing() {
        let state = Arc::new(LseResumeState::new());
        let mut subject = subject(Some(state)).await;

        assert_eq!(
            subject.transform(tick("2026-01-02T09:00:00Z", "1.0")).len(),
            1
        );
    }

    #[tokio::test]
    async fn a_control_frame_is_neither_emitted_nor_recorded() {
        let state = Arc::new(LseResumeState::new());
        let mut subject = subject(Some(Arc::clone(&state))).await;

        let frame: LseMessage =
            serde_json::from_str(r#"{"type":"replay_complete","symbol":"BTC/USD","rows":41}"#)
                .unwrap();

        assert!(subject.transform(frame).is_empty());
        assert_eq!(state.watermark(&id()), None);
    }

    /// The no-dedupe lock, restated at the transformer: identical repeats are genuine distinct
    /// prints and only the resume prefix may ever be skipped.
    #[tokio::test]
    async fn identical_live_ticks_are_never_deduplicated() {
        let state = Arc::new(LseResumeState::new());
        let mut subject = subject(Some(state)).await;

        let first = subject.transform(tick("2026-01-02T09:00:00.000001Z", "1.0"));
        let second = subject.transform(tick("2026-01-02T09:00:00.000001Z", "1.0"));

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1, "an identical repeat is a distinct print");
    }
}
