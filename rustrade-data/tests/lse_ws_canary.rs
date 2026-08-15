//! London Strategic Edge WebSocket shape canary (network + credential gated).
//!
//! # Why this exists
//!
//! `lse_ws_handshake.rs` drives this integration against a synthetic server, which proves the client
//! behaves correctly *given* the protocol as this repository understands it. Nothing in it can
//! notice that the provider's understanding has changed. That is this file's job, and it is the only
//! test in the crate that ever reaches the real socket — the provider prohibits redistributing its
//! data (<https://londonstrategicedge.com/terms>), so no recorded frame may be committed here and
//! every other test is necessarily synthetic.
//!
//! # ⚠️ What it deliberately does NOT assert
//!
//! **No counts.** The published symbol list moved from 7,940 to 8,516 entries inside a week, and
//! the subscription cap is a plan attribute the provider may revise. A canary that pinned either
//! would fail for a reason that is not a defect, and would then be muted — which costs more than it
//! ever caught. Support is asserted structurally instead: the guards either work against the real
//! list or they do not.
//!
//! # The three signals it does assert
//!
//! 1. **Every subscribed symbol actually ticks.** This surface's quietest failure is a subscription
//!    that is *confirmed and then silent* — the provider answers `subscribed` for a symbol it does
//!    not serve, never errors, and holds the slot for the life of the connection. A shape check on
//!    whichever frames happen to arrive cannot see it, because the frames that arrive are fine; it
//!    is the absent ones that matter. So delivery is required per symbol, not in aggregate.
//! 2. **A tick's decoded instant is plausible — and on a 24/7 venue, recent.** The provider spells
//!    `ts` three ways and changes spelling per dataset and between live and replayed frames. Every
//!    one of those spellings decodes to *a* `DateTime` — a seconds-vs-milliseconds epoch misread
//!    lands in 1970 or in the fifty-third century, and a dropped timezone lands hours out — so the
//!    decode cannot be checked by whether it succeeded, only by whether the answer is plausible.
//!    The *tight* recency window is held only against continuously-traded crypto, because a venue
//!    that keeps market hours serves a stale instant legitimately; see the note below.
//! 3. **A symbol the provider does not offer is rejected before subscribing.** This passes only if
//!    the `authenticated` frame still carries a usable symbol list: were the list to disappear or be
//!    renamed, the guard degrades to a warning by design, the provider confirms the bogus symbol,
//!    and the batch would succeed. That silent degradation is exactly what this catches.
//!
//! # Skip vs. fail contract
//!
//! - `LSE_API_KEY` **unset** → **SKIP** (logged, test passes), so CI without secrets stays green.
//! - `LSE_API_KEY` set but unusable → **FAIL**. A skip here would be indistinguishable from "no
//!   secrets configured", so a mistyped key would report green forever.
//! - Key present but an assertion fails → **FAIL** (the real signal).
//!
//! # ⚠️ A closed venue is not a silent one — measured
//!
//! One test covers the only venue that reaches the space-separated timestamp spelling, and that
//! venue keeps market hours. It was first written to **skip on silence**, on the assumption that a
//! closed market delivers nothing. That assumption is false: subscribing on a Saturday delivered a
//! tick stamped at the previous session's close — well-formed, correctly decoded, and five hours
//! old. Holding it to a ten-minute window failed the canary for a market simply being shut, which
//! is the failure-for-a-non-defect this file's own reasoning rules out.
//!
//! So that test holds the wide plausibility band unconditionally and reports whether the tight
//! recency signal was reached at all. A stale-but-plausible instant and a `CANARY_SKIP` line mean
//! the same thing — rerun while that market is open. Neither is a pass for the signal it names.
//!
//! # Running
//!
//! ```bash
//! set -a && . ./.env && set +a
//! cargo test --test lse_ws_canary --features lse -- --ignored --nocapture
//! ```
//!
//! Marked `#[ignore]` so a default test run never opens a connection or spends the shared
//! allowance.

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::{Stream, StreamExt};
use rustrade_data::{
    event::MarketEvent,
    exchange::lse::{LseCfd, LseCrypto, live::LseSubscriber},
    streams::{
        Streams,
        reconnect::{Event, stream::ReconnectingStream},
    },
    subscriber::Subscriber,
    subscription::{Subscription, trade::PublicTrade, trade::PublicTrades},
};
use rustrade_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
};
use std::time::Duration;
use tokio::time::Instant;

const KEY_ENV: &str = "LSE_API_KEY";

/// How long to wait for every subscribed symbol to tick.
///
/// Generous against the measured rates — one busy crypto symbol replayed six figures of ticks per
/// hour — so a timeout here means silence, not slowness.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(45);

/// How far behind now a tick may be stamped on a **continuously-traded** venue.
///
/// Wide enough to absorb clock skew and a quiet minute on a thin symbol, and far narrower than any
/// epoch or timezone misread: those miss by decades or by whole hours. Only crypto is held to this,
/// because only crypto never closes — see the module header.
const MAX_TICK_AGE: ChronoDuration = ChronoDuration::minutes(10);

/// How far behind now a tick may be stamped on a venue that **keeps market hours**.
///
/// A closed venue serves its last session print rather than going silent, so the age of a perfectly
/// decoded instant there is bounded by the closure, not by the feed: a Friday-evening close read on
/// a Sunday is two days old and entirely correct. This band contains any weekend or holiday closure
/// while still being narrower than an epoch-scale misread by decades, so it keeps the signal that
/// survives a closed market and drops the one that cannot.
const MAX_CLOSED_VENUE_TICK_AGE: ChronoDuration = ChronoDuration::days(7);

/// How far ahead of now a live tick may be stamped.
///
/// Non-zero only for clock skew — the provider stamps in the past by construction.
const MAX_TICK_LEAD: ChronoDuration = ChronoDuration::minutes(2);

/// Build a subscriber, or `None` when the key is **absent** (skip rather than fail).
///
/// Only an unset variable skips. A key that is *set but unusable* — a stray newline from a `.env`
/// edit, a mis-encoded paste — is a misconfiguration, and reporting it as a skip would let this
/// canary pass green while never once reaching the provider, which is exactly the state it exists
/// to detect. The error is safe to print: the credential type redacts the key from every message.
fn subscriber() -> Option<LseSubscriber> {
    if std::env::var_os(KEY_ENV).is_none() {
        println!("CANARY_SKIP: {KEY_ENV} is not set - skipping");
        return None;
    }

    Some(
        LseSubscriber::from_env()
            .unwrap_or_else(|error| panic!("{KEY_ENV} is set but unusable: {error}")),
    )
}

fn spot(base: &str) -> MarketDataInstrument {
    MarketDataInstrument::from((base, "usd", MarketDataInstrumentKind::Spot))
}

fn cfd(base: &str) -> MarketDataInstrument {
    MarketDataInstrument::from((base, "usd", MarketDataInstrumentKind::Cfd))
}

/// Fail if a tick's decoded instant is not plausible, where `max_age` is what "plausible" means for
/// the venue under test.
///
/// See signal 2 in the module header: every spelling the provider uses decodes to *some* instant,
/// so plausibility is the only available check on whether it decoded to the right one. How much
/// staleness is plausible is a property of the venue's trading hours, not of the decoder, which is
/// why the bound is a parameter rather than a constant.
fn assert_stamped_plausibly(
    instrument: &MarketDataInstrument,
    time_exchange: DateTime<Utc>,
    max_age: ChronoDuration,
) {
    let now = Utc::now();
    let age = now - time_exchange;

    assert!(
        age <= max_age,
        "{instrument} ticked at {time_exchange}, {age} behind now (limit {max_age}) - the timestamp \
         decode has probably misread the provider's spelling",
    );
    assert!(
        -age <= MAX_TICK_LEAD,
        "{instrument} ticked at {time_exchange}, {} ahead of now - a live tick cannot be stamped in \
         the future beyond clock skew, so the timestamp decode has probably misread the provider's \
         spelling",
        -age,
    );
}

/// Drive `stream` until every instrument in `expected` has delivered a tick, or the deadline.
///
/// Returns the instruments that never ticked **and** the newest instant seen. The caller decides
/// what both mean for its venue: whether silence is a failure (a 24/7 venue) or a skip (a venue that
/// closes), and whether the newest instant was recent enough to have exercised the tight window.
async fn collect_first_tick_per_instrument<S>(
    mut stream: S,
    expected: &[MarketDataInstrument],
    max_age: ChronoDuration,
) -> (Vec<MarketDataInstrument>, Option<DateTime<Utc>>)
where
    S: Stream<Item = Event<ExchangeId, MarketEvent<MarketDataInstrument, PublicTrade>>> + Unpin,
{
    let mut pending = expected.to_vec();
    let mut newest: Option<DateTime<Utc>> = None;
    let deadline = Instant::now() + DELIVERY_TIMEOUT;

    // One deadline governs the whole wait, so there is no per-iteration clock arithmetic and no
    // chance of spinning once the stream goes quiet.
    let _ = tokio::time::timeout_at(deadline, async {
        while !pending.is_empty() {
            match stream.next().await {
                Some(Event::Item(event)) => {
                    // Checked on every tick rather than only the first: the provider changes `ts`
                    // spelling between datasets, and a stream that begins well can still carry a
                    // frame this integration reads wrongly.
                    assert_stamped_plausibly(&event.instrument, event.time_exchange, max_age);

                    newest = newest.max(Some(event.time_exchange));

                    if let Some(index) = pending.iter().position(|i| *i == event.instrument) {
                        let delivered = pending.swap_remove(index);
                        println!("CANARY_OK: {delivered} delivered a live tick");
                    }
                }
                // Logged rather than ignored: a reconnect mid-window is the likeliest innocent
                // cause of a timeout, and distinguishing it from silence matters when reading a
                // failure.
                Some(Event::Reconnecting(origin)) => {
                    println!("CANARY: {origin} is reconnecting mid-test");
                }
                // The reconnecting stream should never exhaust; if it has, the consumer task died.
                None => panic!("the market stream terminated before delivering every symbol"),
            }
        }
    })
    .await;

    (pending, newest)
}

#[tokio::test]
#[ignore = "opens a live connection and spends the shared provider allowance; run on demand"]
async fn every_subscribed_crypto_symbol_delivers_a_live_tick() {
    let Some(subscriber) = subscriber() else {
        return;
    };

    // Crypto because it is the one dataset family that trades continuously: on any other, silence
    // is ambiguous between "closed" and "confirmed but never served", and this test exists to make
    // that distinction unambiguous.
    let expected = [spot("btc"), spot("eth")];

    let streams = Streams::<PublicTrades>::builder()
        .subscribe(
            subscriber,
            expected.clone().map(|instrument| {
                Subscription::<LseCrypto, MarketDataInstrument, PublicTrades>::new(
                    LseCrypto::default(),
                    instrument,
                    PublicTrades,
                )
            }),
        )
        .init()
        .await
        .expect("subscribing to continuously-traded crypto symbols should succeed");

    // Decode failures are reported rather than swallowed: they are themselves a shape-change
    // signal, and they explain a delivery timeout that would otherwise read as silence.
    let stream = streams
        .select_all()
        .with_error_handler(|error| println!("CANARY: market stream error: {error:?}"));

    // Crypto is the one venue whose staleness is unambiguous, so it carries the tight window.
    let (silent, _newest) =
        collect_first_tick_per_instrument(Box::pin(stream), &expected, MAX_TICK_AGE).await;

    assert!(
        silent.is_empty(),
        "{silent:?} were confirmed but delivered no tick in {DELIVERY_TIMEOUT:?} - on a \
         continuously-traded venue that is the confirmed-then-silent failure this integration's \
         pre-subscribe guard exists to prevent, which means the guard's symbol list no longer \
         reflects what the provider actually serves",
    );
}

/// Covers the space-separated timestamp spelling, which the crypto test above cannot reach: the
/// provider sends `2026-01-02 09:37:21.690159+00:00` on this venue and the `T`-separated form on
/// crypto, and [`DateTime::parse_from_rfc3339`] rejects the former outright.
#[tokio::test]
#[ignore = "opens a live connection and spends the shared provider allowance; run on demand"]
async fn a_cfd_tick_decodes_to_a_plausible_instant() {
    let Some(subscriber) = subscriber() else {
        return;
    };

    let expected = [cfd("xau")];

    let streams = Streams::<PublicTrades>::builder()
        .subscribe(
            subscriber,
            expected.clone().map(|instrument| {
                Subscription::<LseCfd, MarketDataInstrument, PublicTrades>::new(
                    LseCfd::default(),
                    instrument,
                    PublicTrades,
                )
            }),
        )
        .init()
        .await
        .expect("subscribing to a published CFD symbol should succeed");

    // Decode failures are reported rather than swallowed: they are themselves a shape-change
    // signal, and they explain a delivery timeout that would otherwise read as silence.
    let stream = streams
        .select_all()
        .with_error_handler(|error| println!("CANARY: market stream error: {error:?}"));

    // The wide band, because this venue closes. A tick arriving here is not evidence the market is
    // open: a closed session serves its last print, correctly stamped hours or days ago.
    let (silent, newest) =
        collect_first_tick_per_instrument(Box::pin(stream), &expected, MAX_CLOSED_VENUE_TICK_AGE)
            .await;

    // The plausibility assertion already ran inside the collector, on every tick that arrived. What
    // is left is to say which signal was actually reached, because neither outcome below is a pass
    // for the recency check, and a silent green here would be the muted canary this file warns
    // about.
    match newest {
        None => println!(
            "CANARY_SKIP: {silent:?} delivered no tick in {DELIVERY_TIMEOUT:?} - this venue keeps \
             market hours, so this is most likely closed rather than broken. The space-separated \
             timestamp spelling was NOT exercised; rerun while the market is open.",
        ),
        Some(newest) if Utc::now() - newest > MAX_TICK_AGE => println!(
            "CANARY_SKIP: the newest tick is stamped {newest}, older than {MAX_TICK_AGE} - the \
             session has closed and the provider is serving its last print. The spelling decoded \
             plausibly, but recency was NOT exercised; rerun while the market is open.",
        ),
        Some(newest) => println!(
            "CANARY_OK: the space-separated timestamp spelling decoded to {newest}, live and recent",
        ),
    }
}

/// The pre-subscribe guard is only as good as the list it checks against. If the provider stopped
/// publishing symbols in its `authenticated` frame, the guard would degrade to a warning by design,
/// the bogus symbol below would be *confirmed*, and this would return `Ok` — so a passing assertion
/// here is what proves the list is still both present and honoured.
#[tokio::test]
#[ignore = "opens a live connection; run on demand"]
async fn a_symbol_the_provider_does_not_offer_is_rejected_before_subscribing() {
    let Some(subscriber) = subscriber() else {
        return;
    };

    let subscription = Subscription::<LseCrypto, MarketDataInstrument, PublicTrades>::new(
        LseCrypto::default(),
        spot("nope-xyz"),
        PublicTrades,
    );

    let error = subscriber
        .subscribe(&[subscription])
        .await
        .expect_err(
            "the provider confirms symbols it does not serve, so a bogus symbol must be rejected \
             by the pre-subscribe guard rather than by the provider",
        )
        .to_string();

    assert!(error.contains("NOPE-XYZ/USD"), "{error}");
    println!("CANARY_OK: an unoffered symbol was rejected before any subscribe was sent");
}
