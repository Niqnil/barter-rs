//! Subscription-lifecycle frames: what the provider answers a `subscribe` with, and how a replay
//! window announces its own boundaries.

use super::tick::de_timestamp;
use chrono::{DateTime, Utc};
use rustrade_integration::{Validator, error::SocketError};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// The provider's answer to a `subscribe` request.
///
/// One frame arrives per request — `{"action":"subscribe","symbol":"EUR/USD"}` is answered by
/// `{"type":"subscribed","symbol":"EUR/USD","count":1,"max":16}` — so the default
/// [`expected_responses`](crate::exchange::Connector::expected_responses) of one-per-subscription
/// is correct.
///
/// # ⚠️ A confirmation is NOT evidence the symbol exists
/// Subscribing to a symbol the provider has never heard of is **confirmed, not rejected**: it
/// answers `subscribed`, never errors, never ticks, and permanently consumes one of the
/// connection's subscription slots. Validation here therefore cannot catch a typo, and does not
/// try to. That guard runs in the subscriber, against the symbol list the `authenticated` frame
/// supplies, *before* any subscribe is sent — which is also what keeps a bad batch from spending
/// slots it can never get back.
///
/// # Why replay frames are deliberately not modelled here
/// The subscription validator counts every frame that deserialises into this type and validates
/// `Ok` as one more subscription confirmed. A `replay_started` frame modelled as a success would
/// inflate that count and end validation before the last real confirmation arrived. Replay frames
/// are [`LseReplayFrame`] instead, and reach the subscriber through the buffered-events path — the
/// same route ticks that arrive mid-validation take. For the same reason this enum has no
/// catch-all variant: one would swallow ticks into the success count.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LseSubResponse {
    /// A subscription was accepted, and the connection now holds `count` of its `max` slots.
    Subscribed {
        /// The symbol, case-normalised by the provider (`eur/usd` is confirmed as `EUR/USD`).
        symbol: SmolStr,
        /// Slots held on this connection after the subscription.
        count: u32,
        /// Slots this connection may hold in total.
        max: u32,
    },

    /// A subscription was rejected.
    ///
    /// Both fields are optional so that a rejection missing one cannot fail to deserialise and be
    /// silently reclassified as stream data — an error frame must never be the thing that gets
    /// swallowed.
    Error {
        /// The provider's error code. `LIMIT_REACHED` and `INVALID_START` are the two observed;
        /// the field is kept as text rather than an enum so an unrecognised code reaches the
        /// caller intact instead of collapsing into a catch-all.
        #[serde(default)]
        code: Option<SmolStr>,
        /// The provider's own diagnostic.
        #[serde(default)]
        message: Option<SmolStr>,
    },
}

impl Validator for LseSubResponse {
    type Error = SocketError;

    fn validate(self) -> Result<Self, SocketError> {
        let Self::Error { code, message } = &self else {
            return Ok(self);
        };

        let code = code.as_deref().unwrap_or("unknown");
        let message = message.as_deref().unwrap_or("no message");

        // The rejection does not name the symbol it rejected -- measured: 20 subscriptions
        // requested against a cap of 16 produced 16 confirmations and 4 anonymous errors. There is
        // therefore no partial recovery available, and continuing would leave the caller holding a
        // subscription set the provider silently truncated.
        Err(SocketError::Subscribe(format!(
            "London Strategic Edge rejected a subscription ({code}): {message} - the rejection \
             does not name the symbol, so the whole batch fails"
        )))
    }
}

/// A replay window's boundary frames.
///
/// A subscription carrying a `start` is served the historical window before any live tick, framed
/// by these two. They are not subscription confirmations — see [`LseSubResponse`] for why keeping
/// them out of that type is load-bearing rather than tidy.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LseReplayFrame {
    /// The replay window the provider actually opened.
    ///
    /// # ⚠️ `from` is the only signal that a `start` was clamped
    /// A `start` further back than the provider's retention is **silently moved forward**, with no
    /// error: a window requested 48 hours back was answered with one beginning exactly 24 hours
    /// back, and streamed from there. Comparing `from` against what was asked for is the only way
    /// to detect it.
    ReplayStarted {
        /// The symbol whose replay is beginning.
        symbol: SmolStr,
        /// The instant the provider will actually replay from.
        #[serde(deserialize_with = "de_timestamp")]
        from: DateTime<Utc>,
    },

    /// The replay window is drained and the stream is now live for this symbol.
    ReplayComplete {
        /// The symbol whose replay has finished.
        symbol: SmolStr,
        /// Rows replayed. Trustworthy — it matched the delivered count exactly on every run
        /// measured.
        rows: u64,
        /// Live ticks that arrived during the replay and were held back to preserve ordering.
        #[serde(default)]
        buffered_drained: u64,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn a_subscription_confirmation_deserialises_and_validates() {
        let input = r#"{"type":"subscribed","symbol":"EUR/USD","count":1,"max":16}"#;
        let response: LseSubResponse = serde_json::from_str(input).unwrap();

        assert_eq!(
            response,
            LseSubResponse::Subscribed {
                symbol: "EUR/USD".into(),
                count: 1,
                max: 16,
            }
        );
        assert!(response.validate().is_ok());
    }

    #[test]
    fn a_subscription_rejection_fails_validation() {
        let input = r#"{"type":"error","code":"LIMIT_REACHED",
            "message":"Max 16 symbols on registered plan"}"#;
        let response: LseSubResponse = serde_json::from_str(input).unwrap();

        let error = response.validate().unwrap_err().to_string();
        assert!(error.contains("LIMIT_REACHED"), "{error}");
        assert!(error.contains("Max 16 symbols"), "{error}");
    }

    #[test]
    fn a_rejected_replay_start_fails_validation() {
        let input = r#"{"type":"error","code":"INVALID_START",
            "message":"Invalid start: could not convert string to float"}"#;
        let response: LseSubResponse = serde_json::from_str(input).unwrap();

        assert!(response.validate().is_err());
    }

    /// An error frame that fails to deserialise would be reclassified as stream data and silently
    /// dropped, turning a loud rejection into no signal at all.
    #[test]
    fn a_rejection_missing_its_fields_still_fails_validation() {
        let response: LseSubResponse = serde_json::from_str(r#"{"type":"error"}"#).unwrap();
        assert!(response.validate().is_err());
    }

    /// The subscription validator counts everything that deserialises into [`LseSubResponse`] as a
    /// confirmation. Ticks and replay frames must therefore NOT deserialise into it — they take
    /// the buffered-events path instead.
    #[test]
    fn stream_frames_do_not_deserialise_as_subscription_responses() {
        for input in [
            r#"{"type":"tick","symbol":"EUR/USD","ts":"2026-01-02T09:37:24.760146+00:00",
                "price":1.1,"bid":1.1,"ask":1.1001,"volume":1.0}"#,
            r#"{"type":"replay_started","symbol":"BTC/USD","from":"2026-01-02T09:39:31+00:00"}"#,
            r#"{"type":"replay_complete","symbol":"BTC/USD","rows":41,"buffered_drained":9}"#,
        ] {
            assert!(
                serde_json::from_str::<LseSubResponse>(input).is_err(),
                "deserialised as a subscription response: {input}"
            );
        }
    }

    #[test]
    fn a_replay_start_deserialises_with_its_window() {
        let input = r#"{"type":"replay_started","symbol":"BTC/USD","from":"2026-01-02T09:39:31.716622+00:00"}"#;
        let frame: LseReplayFrame = serde_json::from_str(input).unwrap();

        assert_eq!(
            frame,
            LseReplayFrame::ReplayStarted {
                symbol: "BTC/USD".into(),
                from: "2026-01-02T09:39:31.716622Z".parse().unwrap(),
            }
        );
    }

    /// The provider spells timestamps three ways and does not guarantee which one a given frame
    /// uses, so `from` goes through the same decoder a tick's `ts` does.
    #[test]
    fn a_replay_start_accepts_every_timestamp_spelling() {
        for raw in [
            r#""2026-01-02 09:39:31.716622+00:00""#,
            r#""2026-01-02T09:39:31.716622+00:00""#,
            "1767346771.716622",
        ] {
            let input = format!(r#"{{"type":"replay_started","symbol":"BTC/USD","from":{raw}}}"#);
            let frame: LseReplayFrame = serde_json::from_str(&input).unwrap();

            let LseReplayFrame::ReplayStarted { from, .. } = frame else {
                panic!("expected a replay start");
            };
            assert_eq!(
                from,
                "2026-01-02T09:39:31.716622Z"
                    .parse::<DateTime<Utc>>()
                    .unwrap()
            );
        }
    }

    #[test]
    fn a_replay_completion_deserialises_with_its_row_count() {
        let input = r#"{"type":"replay_complete","symbol":"BTC/USD","rows":41,
            "buffered_drained":9}"#;
        let frame: LseReplayFrame = serde_json::from_str(input).unwrap();

        assert_eq!(
            frame,
            LseReplayFrame::ReplayComplete {
                symbol: "BTC/USD".into(),
                rows: 41,
                buffered_drained: 9,
            }
        );
    }

    #[test]
    fn subscription_frames_do_not_deserialise_as_replay_frames() {
        let input = r#"{"type":"subscribed","symbol":"EUR/USD","count":1,"max":16}"#;
        assert!(serde_json::from_str::<LseReplayFrame>(input).is_err());
    }
}
