use crate::engine::{
    action::send_requests::{SendCancelsAndOpensOutput, SendRequestsOutput},
    error::UnrecoverableEngineError,
};
use derive_more::From;
use rustrade_execution::order::request::{RequestCancel, RequestOpen};
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};
use rustrade_integration::collection::one_or_many::OneOrMany;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Defines the `Engine` action for cancelling open order requests.
pub mod cancel_orders;

/// Defines the `Engine` action for generating and sending order requests for closing open positions.
pub mod close_positions;

/// Defines the `Engine` action for generating and sending algorithmic order requests.
pub mod generate_algo_orders;

/// Defines the `Engine` action for sending order `ExecutionRequests` to the execution manager.
pub mod send_requests;

/// Output of the `Engine` after actioning a [`Command`](super::command::Command).
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, From)]
// `ClosePositions` (a cancels+opens pair, ~608 B) dwarfs `CancelOrders` (~208 B); the >200 B spread
// trips `clippy::large_enum_variant`. This is a command-response type, not the per-tick hot path,
// and its largest member is already boxed one level up (`EngineOutput::Commanded(Box<ActionOutput>)`),
// so boxing a variant here would only add an allocation for no measured benefit. The root-cause
// shrink of `SendCancelsAndOpensOutput` itself is tracked separately in #195.
#[allow(clippy::large_enum_variant)]
pub enum ActionOutput<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    CancelOrders(SendRequestsOutput<RequestCancel, ExchangeKey, InstrumentKey>),
    OpenOrders(SendRequestsOutput<RequestOpen, ExchangeKey, InstrumentKey>),
    ClosePositions(SendCancelsAndOpensOutput<ExchangeKey, InstrumentKey>),
}

impl<ExchangeKey, InstrumentKey> ActionOutput<ExchangeKey, InstrumentKey> {
    /// Returns any unrecoverable errors that occurred during an `Engine` action.
    pub fn unrecoverable_errors(&self) -> Option<OneOrMany<UnrecoverableEngineError>> {
        match self {
            ActionOutput::CancelOrders(cancels) => cancels.unrecoverable_errors(),
            ActionOutput::OpenOrders(opens) => opens.unrecoverable_errors(),
            ActionOutput::ClosePositions(requests) => requests.unrecoverable_errors(),
        }
        .into_option()
    }
}
