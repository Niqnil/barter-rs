use crate::instrument::name::InstrumentNameExchange;
use derive_more::Constructor;
use serde::{Deserialize, Serialize};

/// The venue an [`Instrument`](super::Instrument)s **market data** is sourced from, when that
/// differs from the venue it is **executed** on.
///
/// # Why this exists
/// Position and `pnl_unrealised` state is strictly `InstrumentIndex`-scoped: a position opened by a
/// fill only receives live PnL from market events tagged with *that* index. Modelling "priced by
/// venue A, traded on venue B" as two separate `Instrument`s would therefore leave the traded
/// instrument's PnL permanently stale, because the price updates would land on the other index.
/// A `DataVenue` keeps both roles on **one** instrument, and so on one ledger.
///
/// # Fallback
/// Both fields fall back to the execution venue's equivalents, so a `DataVenue` only has to state
/// what actually differs:
/// - no `DataVenue` at all ⇒ market data is sourced from `Instrument::exchange` under
///   `Instrument::name_exchange`, which is the behaviour of every single-venue instrument;
/// - a `DataVenue` with `name_exchange: None` ⇒ the data venue spells the symbol exactly as the
///   execution venue does.
///
/// Read them through [`Instrument::data_exchange`](super::Instrument::data_exchange) and
/// [`Instrument::data_name_exchange`](super::Instrument::data_name_exchange) rather than reaching
/// into the fields, so the fallback is applied in one place.
///
/// # `name_exchange` is not cosmetic
/// Two venues routinely spell one economic instrument differently — London Strategic Edge quotes BP
/// as `BP.L` while a US broker lists it as `BP` — so a data venue that could only carry an exchange
/// identifier would subscribe under the *execution* venue's symbol and silently receive nothing, or
/// worse, receive a different instrument's prices.
///
/// # Suitability (caller obligation)
/// Sourcing prices from one venue and filling on another means **the decision price is not the fill
/// price**: the two venues have separate books, and a data venue that publishes an aggregated or
/// synthetic series (a CFD or index proxy) has no book at all. That basis mismatch is sound for
/// research and paper trading, but for real capital it is a property the caller must opt into
/// knowingly — the library cannot detect it.
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct DataVenue<ExchangeKey> {
    /// Venue the market data is sourced from.
    pub exchange: ExchangeKey,

    /// Symbol the data venue uses for this instrument.
    ///
    /// `None` ⇒ the data venue spells it exactly as the execution venue does.
    #[serde(default)]
    pub name_exchange: Option<InstrumentNameExchange>,
}

impl<ExchangeKey> DataVenue<ExchangeKey> {
    /// Construct a `DataVenue` that shares the execution venue's symbol.
    pub fn new_same_name(exchange: ExchangeKey) -> Self {
        Self {
            exchange,
            name_exchange: None,
        }
    }

    /// Map this `DataVenue`s `ExchangeKey` to a new key, using the provided lookup closure.
    pub fn map_exchange_key<FnFindExchange, NewExchangeKey>(
        self,
        find_exchange: FnFindExchange,
    ) -> DataVenue<NewExchangeKey>
    where
        FnFindExchange: FnOnce(&ExchangeKey) -> NewExchangeKey,
    {
        let exchange = find_exchange(&self.exchange);

        DataVenue {
            exchange,
            name_exchange: self.name_exchange,
        }
    }
}
