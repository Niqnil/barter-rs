use indexmap::IndexMap;
use rustrade_instrument::{
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Maintains a global connection [`Health`], as well as the connection status of market data
/// and account connections for each exchange.
#[derive(Debug, Clone, Eq, PartialEq, Default, Deserialize, Serialize)]
pub struct ConnectivityStates {
    /// Global connection [`Health`].
    ///
    /// Global health is considered `Healthy` if all exchange market data and account
    /// connections are `Healthy`.
    pub global: Health,

    /// Connectivity `Health` of market data and account connections by exchange.
    pub exchanges: IndexMap<ExchangeId, ConnectivityState>,
}

impl ConnectivityStates {
    /// Updates from an exchange AccountStream disconnection.
    ///
    /// Sets the account `ConnectivityState` for the provided `ExchangeId`
    /// to [`Health::Reconnecting`].
    pub fn update_from_account_reconnecting(&mut self, exchange: &ExchangeId) {
        warn!(%exchange, "EngineState received AccountStream disconnected event");
        self.global = Health::Reconnecting;
        self.connectivity_mut(exchange).account = Health::Reconnecting;
    }

    /// Updates from an exchange AccountStream event, setting the `ConnectivityState` account
    /// connection to [`Health::Healthy`] if it was not previously.
    ///
    /// If after the update all `ConnectivityState`s are healthy, the global health is set to
    /// `Health::Healthy`.
    pub fn update_from_account_event(&mut self, exchange: &ExchangeIndex) {
        if self.global == Health::Healthy {
            return;
        }

        let state = self.connectivity_index_mut(exchange);
        if state.account == Health::Healthy {
            return;
        }

        info!(
            %exchange,
            "EngineState received AccountStream event - setting connection to Healthy"
        );
        state.account = Health::Healthy;

        if self.exchange_states().all(ConnectivityState::all_healthy) {
            info!("EngineState setting global connectivity to Healthy");
            self.global = Health::Healthy
        }
    }

    /// Updates from an exchange MarketStream disconnection.
    ///
    /// Sets the market data `ConnectivityState` for the provided `ExchangeId`
    /// to [`Health::Reconnecting`].
    pub fn update_from_market_reconnecting(&mut self, exchange: &ExchangeId) {
        warn!(%exchange, "EngineState received MarketStream disconnect event");
        self.global = Health::Reconnecting;
        self.connectivity_mut(exchange).market_data = Health::Reconnecting
    }

    /// Updates from an exchange MarketStream event, setting the `ConnectivityState` market data
    /// connection to [`Health::Healthy`] if it was not previously.
    ///
    /// If after the update all `ConnectivityState`s are healthy, the global health is set to
    /// `Health::Healthy`.
    pub fn update_from_market_event(&mut self, exchange: &ExchangeId) {
        if self.global == Health::Healthy {
            return;
        }

        let state = self.connectivity_mut(exchange);
        if state.market_data == Health::Healthy {
            return;
        }

        info!(
            %exchange,
            "EngineState received MarketStream event - setting connection to Healthy"
        );
        state.market_data = Health::Healthy;

        if self.exchange_states().all(ConnectivityState::all_healthy) {
            info!("EngineState setting global connectivity to Healthy");
            self.global = Health::Healthy
        }
    }

    /// Returns a reference to the `ConnectivityState` associated with the
    /// provided `ExchangeIndex`.
    ///
    /// Panics if the `ConnectivityState` associated with the `ExchangeIndex` is not found.
    pub fn connectivity_index(&self, key: &ExchangeIndex) -> &ConnectivityState {
        self.exchanges
            .get_index(key.index())
            .map(|(_key, state)| state)
            .unwrap_or_else(|| panic!("ConnectivityStates does not contain: {key}"))
    }

    /// Returns a mutable reference to the `ConnectivityState` associated with the
    /// provided `ExchangeIndex`.
    ///
    /// Panics if the `ConnectivityState` associated with the `ExchangeIndex` is not found.
    pub fn connectivity_index_mut(&mut self, key: &ExchangeIndex) -> &mut ConnectivityState {
        self.exchanges
            .get_index_mut(key.index())
            .map(|(_key, state)| state)
            .unwrap_or_else(|| panic!("ConnectivityStates does not contain: {key}"))
    }

    /// Returns a reference to the `ConnectivityState` associated with the
    /// provided `ExchangeId`.
    ///
    /// Panics if the `ConnectivityState` associated with the `ExchangeId` is not found.
    pub fn connectivity(&self, key: &ExchangeId) -> &ConnectivityState {
        self.exchanges
            .get(key)
            .unwrap_or_else(|| panic!("ConnectivityStates does not contain: {key}"))
    }

    /// Returns a mutable reference to the `ConnectivityState` associated with the
    /// provided `ExchangeId`.
    ///
    /// Panics if the `ConnectivityState` associated with the `ExchangeId` is not found.
    pub fn connectivity_mut(&mut self, key: &ExchangeId) -> &mut ConnectivityState {
        self.exchanges
            .get_mut(key)
            .unwrap_or_else(|| panic!("ConnectivityStates does not contain: {key}"))
    }

    /// Return an `Iterator` of the `ExchangeId`s being tracked.
    pub fn exchange_ids(&self) -> impl Iterator<Item = &ExchangeId> {
        self.exchanges.keys()
    }

    /// Return an `Iterator` of all `ConnectivityState`s being tracked.
    pub fn exchange_states(&self) -> impl Iterator<Item = &ConnectivityState> {
        self.exchanges.values()
    }
}

/// Represents the `Health` status of a component or connection to an exchange endpoint.
///
/// Used to track both market data and account connections in a [`ConnectivityState`].
///
/// Default implementation is [`Health::Reconnecting`].
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize,
)]
pub enum Health {
    /// Connection is established and functioning normally.
    Healthy,

    /// Connection is currently attempting to re-establish after a disconnect or failure.
    #[default]
    Reconnecting,
}

/// Which connection dimensions a venue actually provides.
///
/// A venue that only supplies market data has no account connection to establish, and a venue that
/// is only executed on has no market data subscription — so demanding [`Health::Healthy`] on both
/// dimensions would leave such a venue, and therefore
/// [`ConnectivityStates::global`], [`Health::Reconnecting`] forever.
///
/// # Roles are per dimension, not per provider
/// In a configuration where an instrument is priced on one venue and executed on another, **both**
/// venues are single-dimension. Marking only the data venue as `DataOnly` fixes nothing: the
/// execution venue would still be waiting on a market data connection that is never made. See
/// [`generate_empty_indexed_connectivity_states`] for how each dimension is derived independently.
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize,
)]
pub enum VenueRole {
    /// Market data is sourced from this venue, but nothing is executed on it — it has no account.
    DataOnly,

    /// Instruments are executed on this venue, but their market data comes from elsewhere.
    ExecutionOnly,

    /// The venue supplies market data *and* holds an account.
    ///
    /// The default, and the strictest interpretation: it is the behaviour that predates this type,
    /// so it is what a `ConnectivityState` deserialised from an older payload assumes.
    #[default]
    Both,
}

impl VenueRole {
    /// Returns true if market data is sourced from this venue.
    pub fn has_market_data(self) -> bool {
        matches!(self, Self::DataOnly | Self::Both)
    }

    /// Returns true if this venue holds an account and is executed on.
    pub fn has_account(self) -> bool {
        matches!(self, Self::ExecutionOnly | Self::Both)
    }

    /// Derive the role of a venue from the dimensions it was observed to provide.
    fn from_dimensions(has_market_data: bool, has_account: bool) -> Self {
        match (has_market_data, has_account) {
            (true, true) => Self::Both,
            (true, false) => Self::DataOnly,
            (false, true) => Self::ExecutionOnly,
            // Unreachable by construction: an `IndexedInstruments` exchange is only ever registered
            // as some `Instrument`s execution venue or as its data venue, so every indexed exchange
            // provides at least one dimension. `Both` is the conservative fallback — it withholds
            // `Healthy` until there is evidence, rather than declaring a venue with no known
            // connections healthy.
            (false, false) => Self::Both,
        }
    }
}

/// Represents the current connection state for both market data and account connections of an
/// exchange.
///
/// Connection health is monitored separately for market data and account connections since they
/// often use different endpoints and may have different health states. Which of the two a given
/// venue actually has is declared by [`Self::role`].
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize)]
pub struct ConnectivityState {
    /// Status of market data connection.
    ///
    /// Only meaningful when [`Self::role`] says market data is sourced from this venue. A
    /// [`VenueRole::ExecutionOnly`] venue has no market data connection to establish, so this stays
    /// at its [`Health::Reconnecting`] default indefinitely — read health via [`Self::all_healthy`],
    /// or consult the role first, rather than this field alone.
    pub market_data: Health,

    /// Status of the account and execution connection.
    ///
    /// Only meaningful when [`Self::role`] says this venue holds an account — see
    /// [`Self::market_data`] for the mirror case.
    pub account: Health,

    /// Which connection dimensions this venue provides.
    ///
    /// `serde` defaults it to [`VenueRole::Both`] — the semantics that predate the field — so a
    /// `ConnectivityState` persisted before it existed still deserialises unchanged.
    #[serde(default)]
    pub role: VenueRole,
}

impl ConnectivityState {
    /// Construct a `ConnectivityState` for a venue in the provided [`VenueRole`], with every
    /// connection [`Health::Reconnecting`].
    pub fn new(role: VenueRole) -> Self {
        Self {
            market_data: Health::Reconnecting,
            account: Health::Reconnecting,
            role,
        }
    }

    /// Returns true if every connection this venue actually has is [`Health::Healthy`].
    ///
    /// Dimensions the venue does not provide are not consulted — a [`VenueRole::DataOnly`] venue is
    /// healthy on a healthy market data connection alone, since it has no account to connect to.
    pub fn all_healthy(&self) -> bool {
        let market_data = !self.role.has_market_data() || self.market_data == Health::Healthy;
        let account = !self.role.has_account() || self.account == Health::Healthy;

        market_data && account
    }
}

/// Generates an indexed [`ConnectivityStates`] containing default connection states.
///
/// Creates a new connection state tracker for each exchange in the provided instruments, with all
/// connections initially set to [`Health::Reconnecting`].
///
/// # Venue roles
/// Each exchange's [`VenueRole`] is derived one dimension at a time, from the instruments
/// themselves:
/// - it has the **market data** dimension iff it is the effective data venue
///   ([`Instrument::data_exchange`](rustrade_instrument::instrument::Instrument::data_exchange)) of
///   at least one instrument;
/// - it has the **account** dimension iff it is the execution venue (`Instrument::exchange`) of at
///   least one instrument.
///
/// Deriving both independently is what makes a split configuration converge. Were the role instead
/// assigned per provider — "this one is the data venue, so it is `DataOnly`" — the *execution* venue
/// of the same instrument would keep its market data dimension and wait forever on a subscription
/// that is never made, and [`ConnectivityStates::global`] would never reach [`Health::Healthy`].
///
/// # Arguments
/// * `instruments` - Reference to [`IndexedInstruments`] containing what exchanges are being tracked.
pub fn generate_empty_indexed_connectivity_states(
    instruments: &IndexedInstruments,
) -> ConnectivityStates {
    // Scans the instruments once per exchange rather than building a lookup: this runs once at
    // startup, and the exchange count is a handful even for large instrument collections.
    let exchanges = instruments
        .exchanges()
        .iter()
        .map(|exchange| {
            let exchange = exchange.value;

            let has_market_data = instruments
                .instruments()
                .iter()
                .any(|instrument| instrument.value.data_exchange().value == exchange);

            let has_account = instruments
                .instruments()
                .iter()
                .any(|instrument| instrument.value.exchange.value == exchange);

            let role = VenueRole::from_dimensions(has_market_data, has_account);
            info!(%exchange, ?role, "EngineState tracking exchange connectivity");

            (exchange, ConnectivityState::new(role))
        })
        .collect();

    ConnectivityStates {
        global: Health::Reconnecting,
        exchanges,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rustrade_instrument::{instrument::data_venue::DataVenue, test_utils::instrument};

    const DATA: ExchangeId = ExchangeId::BinanceSpot;
    const EXECUTION: ExchangeId = ExchangeId::Coinbase;

    /// One instrument priced on `DATA` and executed on `EXECUTION` — the configuration every
    /// per-dimension derivation exists for.
    fn split_venue_instruments() -> IndexedInstruments {
        IndexedInstruments::new([
            instrument(EXECUTION, "btc", "usdt").with_data_venue(DataVenue::new_same_name(DATA))
        ])
    }

    #[test]
    fn a_split_configuration_gives_each_venue_only_the_dimension_it_provides() {
        let states = generate_empty_indexed_connectivity_states(&split_venue_instruments());

        assert_eq!(states.exchanges.len(), 2);
        assert_eq!(states.connectivity(&DATA).role, VenueRole::DataOnly);
        // The half a per-provider fix misses: the execution venue is single-dimension too.
        assert_eq!(
            states.connectivity(&EXECUTION).role,
            VenueRole::ExecutionOnly
        );
    }

    #[test]
    fn a_single_venue_instrument_leaves_its_venue_providing_both_dimensions() {
        let instruments = IndexedInstruments::new([instrument(EXECUTION, "btc", "usdt")]);
        let states = generate_empty_indexed_connectivity_states(&instruments);

        assert_eq!(states.exchanges.len(), 1);
        assert_eq!(states.connectivity(&EXECUTION).role, VenueRole::Both);
    }

    #[test]
    fn a_venue_that_prices_one_instrument_and_executes_another_provides_both_dimensions() {
        let instruments = IndexedInstruments::new([
            instrument(EXECUTION, "btc", "usdt").with_data_venue(DataVenue::new_same_name(DATA)),
            instrument(DATA, "eth", "usdt"),
        ]);
        let states = generate_empty_indexed_connectivity_states(&instruments);

        assert_eq!(states.connectivity(&DATA).role, VenueRole::Both);
        assert_eq!(
            states.connectivity(&EXECUTION).role,
            VenueRole::ExecutionOnly
        );
    }

    #[test]
    fn all_healthy_ignores_the_dimension_a_venue_does_not_provide() {
        assert!(
            ConnectivityState {
                market_data: Health::Healthy,
                account: Health::Reconnecting,
                role: VenueRole::DataOnly,
            }
            .all_healthy()
        );

        assert!(
            ConnectivityState {
                market_data: Health::Reconnecting,
                account: Health::Healthy,
                role: VenueRole::ExecutionOnly,
            }
            .all_healthy()
        );

        // ...but the dimension a venue does provide is still required.
        assert!(
            !ConnectivityState {
                market_data: Health::Reconnecting,
                account: Health::Healthy,
                role: VenueRole::DataOnly,
            }
            .all_healthy()
        );

        assert!(
            !ConnectivityState {
                market_data: Health::Healthy,
                account: Health::Reconnecting,
                role: VenueRole::Both,
            }
            .all_healthy()
        );
    }

    #[test]
    fn global_health_reaches_healthy_once_each_split_venue_reports_its_own_dimension() {
        // Both arrival orderings: whichever event lands last is the one that has to observe global
        // health, so a fix living on only one of the two update paths fails half of this test.
        for market_data_first in [true, false] {
            let instruments = split_venue_instruments();
            let execution = instruments.find_exchange_index(EXECUTION).unwrap();
            let mut states = generate_empty_indexed_connectivity_states(&instruments);

            assert_eq!(states.global, Health::Reconnecting);

            if market_data_first {
                states.update_from_market_event(&DATA);
                assert_eq!(
                    states.global,
                    Health::Reconnecting,
                    "the execution venue has not reported yet"
                );
                states.update_from_account_event(&execution);
            } else {
                states.update_from_account_event(&execution);
                assert_eq!(
                    states.global,
                    Health::Reconnecting,
                    "the data venue has not reported yet"
                );
                states.update_from_market_event(&DATA);
            }

            assert_eq!(states.global, Health::Healthy, "{market_data_first}");
        }
    }

    #[test]
    fn a_state_persisted_before_venue_roles_deserialises_demanding_both_dimensions() {
        let state: ConnectivityState =
            serde_json::from_str(r#"{"market_data":"Healthy","account":"Reconnecting"}"#).unwrap();

        assert_eq!(state.role, VenueRole::Both);
        assert!(!state.all_healthy());
    }
}
