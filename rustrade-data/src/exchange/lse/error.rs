use thiserror::Error;

/// Errors produced by the London Strategic Edge integration.
///
/// `#[non_exhaustive]`: further variants are added alongside the endpoints that raise them.
#[derive(Debug, Clone, Eq, PartialEq, Error)]
#[non_exhaustive]
pub enum LseError {
    /// More than one distinct dataset series resolves to the requested slug, so no single dataset
    /// can be identified.
    ///
    /// Returned rather than picking one, because the provider's `/dataset/info` endpoint answers
    /// `200` for an ambiguous slug — silently serving whichever series it prefers. Two measured
    /// families produce this: eleven Eurex futures that publish both a bare and a `.F` series
    /// containing *different* data (the bare series is frequently the far larger one and has no
    /// slug of its own), and futures whose stripped symbol collides with an unrelated equity
    /// ticker.
    #[error(
        "ambiguous dataset slug {slug:?} for symbol {symbol:?}: more than one series resolves to \
         it, so it cannot identify a dataset - query the catalog and select explicitly"
    )]
    AmbiguousSlug { symbol: String, slug: String },

    /// The provided string does not name a known London Strategic Edge price dataset.
    #[error("unknown dataset {0:?}")]
    UnknownDataset(String),
}
