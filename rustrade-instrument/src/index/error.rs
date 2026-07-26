use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Represents all possible errors that can occur when building, or searching for indexes in, an
/// [`IndexedInstruments`](super::IndexedInstruments) collection.
///
/// `#[non_exhaustive]`: further failure causes can be added without breaking downstream exhaustive
/// matches.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Error)]
#[non_exhaustive]
pub enum IndexError {
    /// Indicates a failure to find an [`ExchangeIndex`](crate::exchange::ExchangeIndex) for a
    /// given exchange identifier.
    ///
    /// Contains a description of the failed lookup attempt.
    #[error("ExchangeIndex: {0}")]
    ExchangeIndex(String),

    /// Indicates a failure to find an [`AssetIndex`](crate::asset::AssetIndex) for a given
    /// asset identifier.
    ///
    /// Contains a description of the failed lookup attempt.
    #[error("AssetIndex: {0}")]
    AssetIndex(String),

    /// Indicates a failure to find an [`InstrumentIndex`](crate::instrument::InstrumentIndex)
    /// for a given instrument identifier.
    ///
    /// Contains a description of the failed lookup attempt.
    #[error("InstrumentIndex: {0}")]
    InstrumentIndex(String),

    /// Two or more [`Instrument`](crate::instrument::Instrument)s share one
    /// [`InstrumentNameInternal`](crate::instrument::name::InstrumentNameInternal), violating the
    /// uniqueness invariant that index-keyed state depends on.
    ///
    /// Downstream state maps are keyed on `InstrumentNameInternal` but read **positionally** by
    /// `InstrumentIndex`, so duplicates collapse the map and silently shift every index past the
    /// collision onto the wrong instrument. Detected while building rather than left to corrupt
    /// state at runtime.
    ///
    /// Contains a description naming the duplicated name and the instruments that share it.
    #[error("duplicate InstrumentNameInternal: {0}")]
    DuplicateInstrumentNameInternal(String),
}
