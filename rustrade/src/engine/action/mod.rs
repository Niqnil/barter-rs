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
///
/// `ClosePositions` (a cancels+opens pair) previously dwarfed `CancelOrders` (~608 B vs ~208 B) — a
/// spread past 200 B that tripped `clippy::large_enum_variant`. With [`SendRequestsOutput`]'s order
/// payloads boxed at the root (#195) that spread is gone, so no `#[allow]` is needed here.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, From)]
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
