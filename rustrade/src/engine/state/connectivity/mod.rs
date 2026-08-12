use derive_more::Constructor;
use fnv::FnvHashSet;
use rustrade_instrument::{
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
};
use rustrade_integration::collection::FnvIndexMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

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
    ///
    /// Fnv-hashed: this is read on every market event, and `ExchangeId` is a fieldless enum — a key
    /// small enough that SipHash's fixed per-call cost dominates. Insertion-ordered either way.
    pub exchanges: FnvIndexMap<ExchangeId, ConnectivityState>,
}

impl ConnectivityStates {
    /// Updates from an exchange AccountStream disconnection.
    ///
    /// Sets the account `ConnectivityState` for the provided `ExchangeId`
    /// to [`Health::Reconnecting`].
    ///
    /// # Errors
    /// Returns [`UntrackedExchange`] if the `ExchangeId` has no `ConnectivityState`, having mutated
    /// nothing — including [`Self::global`].
    pub fn update_from_account_reconnecting(
        &mut self,
        exchange: &ExchangeId,
    ) -> Result<(), UntrackedExchange> {
        let Some(state) = self.exchanges.get_mut(exchange) else {
            return Err(UntrackedExchange::new(
                *exchange,
                ConnectivityDimension::Account,
            ));
        };

        warn!(%exchange, "EngineState received AccountStream disconnected event");
        state.account = Health::Reconnecting;
        self.global = Health::Reconnecting;

        Ok(())
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
    ///
    /// # Errors
    /// Returns [`UntrackedExchange`] if the `ExchangeId` has no `ConnectivityState`, having mutated
    /// nothing — including [`Self::global`].
    pub fn update_from_market_reconnecting(
        &mut self,
        exchange: &ExchangeId,
    ) -> Result<(), UntrackedExchange> {
        let Some(state) = self.exchanges.get_mut(exchange) else {
            return Err(UntrackedExchange::new(
                *exchange,
                ConnectivityDimension::MarketData,
            ));
        };

        warn!(%exchange, "EngineState received MarketStream disconnect event");
        state.market_data = Health::Reconnecting;
        self.global = Health::Reconnecting;

        Ok(())
    }

    /// Updates from an exchange MarketStream event, setting the `ConnectivityState` market data
    /// connection to [`Health::Healthy`] if it was not previously.
    ///
    /// If after the update all `ConnectivityState`s are healthy, the global health is set to
    /// `Health::Healthy`.
    ///
    /// # Errors
    /// Returns [`UntrackedExchange`] if the `ExchangeId` has no `ConnectivityState`. The exchange is
    /// resolved on **every** call, including the already-globally-healthy fast path, so this is
    /// reported deterministically on the first event from that exchange — see the note at the
    /// lookup for why that matters.
    pub fn update_from_market_event(
        &mut self,
        exchange: &ExchangeId,
    ) -> Result<(), UntrackedExchange> {
        // Resolved BEFORE the `global == Healthy` short-circuit below, deliberately. Skipping the
        // lookup while global health held is what made an untracked exchange an *intermittent*
        // fault: its events were silently ignored for as long as everything else was healthy, and
        // the misconfiguration only surfaced once something dragged `global` back to `Reconnecting`
        // -- in practice a reconnect, hours into a run. Resolving unconditionally costs one
        // `IndexMap` get per market event and makes the report fire on the first event from that
        // exchange, in every run.
        let Some(state) = self.exchanges.get_mut(exchange) else {
            return Err(UntrackedExchange::new(
                *exchange,
                ConnectivityDimension::MarketData,
            ));
        };

        if self.global == Health::Healthy || state.market_data == Health::Healthy {
            return Ok(());
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

        Ok(())
    }

    /// Returns a reference to the `ConnectivityState` associated with the
    /// provided `ExchangeIndex`.
    ///
    /// Panics if the `ConnectivityState` associated with the `ExchangeIndex` is not found.
    ///
    /// Unlike the `ExchangeId`-keyed update path — which reports an [`UntrackedExchange`] rather
    /// than panicking — an `ExchangeIndex` is derived from the same [`IndexedInstruments`] this
    /// collection was built from, so an out-of-range one is a library bug rather than a
    /// misconfigured input, and is not something a caller could handle.
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
    /// Panics if the `ConnectivityState` associated with the `ExchangeId` is not found. This is an
    /// assertive accessor for a key the caller already knows is tracked; event-driven code takes
    /// the fallible path instead and reports an [`UntrackedExchange`].
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

/// One half of a venue's connectivity — the axis a [`VenueRole`] declares membership of, and the
/// one an [`UntrackedExchange`] report names.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub enum ConnectivityDimension {
    /// The market data subscription.
    MarketData,

    /// The account and execution connection.
    Account,
}

/// A stream event arrived tagged with an [`ExchangeId`] that has no [`ConnectivityState`] — an
/// exchange that is neither the execution venue nor the data venue of any instrument the engine was
/// built from.
///
/// # Why this is reported rather than panicked
/// The engine cannot rule it out in advance. A `SystemBuild`'s market stream is a caller-supplied
/// `Stream` forwarded verbatim, so the set of exchanges it will emit is unknowable at build time —
/// there is no seam at which to reject the mismatch early. The only honest options at the point of
/// discovery are to abort the engine or to report and continue, and a single stray event from a
/// venue nothing was configured against is not grounds for taking down a trading session.
///
/// # Nothing was mutated
/// In particular [`ConnectivityStates::global`] is left alone. An exchange the engine does not track
/// has no bearing on the health of the ones it does, so a stray disconnect notice must not degrade
/// global health — nor can it be repaired, since no later event for that exchange can restore a
/// state that does not exist.
///
/// Market events additionally stop **before** instrument state is touched. `InstrumentIndex` lookups
/// are positional, so continuing would either panic on an out-of-range index or, worse, silently
/// attribute the print to whichever instrument happens to occupy that slot.
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct UntrackedExchange {
    /// The exchange the event was tagged with.
    pub exchange: ExchangeId,

    /// Which connection the event would have updated.
    pub dimension: ConnectivityDimension,
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
    ///
    /// Also the conservative fallback for a venue observed to provide **neither** dimension: a
    /// misconfiguration, and health is withheld rather than granted to a venue with no known
    /// connection. The two cases are indistinguishable once constructed — this variant
    /// records what is demanded of a venue, not what it was found to have. Only a warning from
    /// whichever site derived the role — [`generate_empty_indexed_connectivity_states`] at startup,
    /// or [`reconcile_venue_roles`] once the execution clients are known — tells them apart, and
    /// neither is persisted.
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
            // Reachable only as a misconfiguration, and only once execution venues are declared: a
            // venue whose every instrument is priced elsewhere, and which was never given an
            // execution client, provides neither dimension. `Both` is the conservative fallback — it
            // withholds `Healthy` until there is evidence, rather than declaring a venue with no
            // known connections healthy. Every caller reports it; see
            // `generate_empty_indexed_connectivity_states` and `reconcile_venue_roles`.
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
    /// [`VenueRole::ExecutionOnly`] venue has no market data connection to establish, so nothing is
    /// expected to move this off its [`Health::Reconnecting`] default — read health via
    /// [`Self::all_healthy`], or consult the role first, rather than this field alone.
    ///
    /// That is an expectation, not an invariant: [`ConnectivityStates::update_from_market_event`]
    /// sets this `Healthy` for any *tracked* exchange, role notwithstanding. A market event arriving
    /// for a venue declared `ExecutionOnly` means the role is wrong, and this field will say so.
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
/// Each exchange's [`VenueRole`] is derived one dimension at a time. Deriving them independently is
/// what makes a split configuration converge. Were the role instead assigned per provider — "this
/// one is the data venue, so it is `DataOnly`" — the *execution* venue of the same instrument would
/// keep its market data dimension and wait forever on a subscription that is never made, and
/// [`ConnectivityStates::global`] would never reach [`Health::Healthy`].
///
/// The two dimensions have **different** sources of truth, and this asymmetry is the point:
/// - **Market data** is instrument-derived: a venue has the dimension iff it is the effective data
///   venue ([`Instrument::data_exchange`](rustrade_instrument::instrument::Instrument::data_exchange))
///   of at least one instrument. That is exact, because subscriptions are generated from the very
///   same field.
/// - **Account** is *execution-derived*: a venue has the dimension iff an execution client is
///   registered against it, since account events reach the engine only from a registered
///   `ExecutionManager`. The instrument model cannot answer this — `Instrument::exchange` names the
///   venue an instrument *would* be executed on, not whether anything is wired up to execute there.
///
/// # `execution_venues`, and what happens without it
/// Pass `Some` set of venues that have a registered execution client whenever it is known;
/// [`SystemBuilder`](crate::system::builder::SystemBuilder) does this automatically.
///
/// `None` falls back to approximating the account dimension as "is the execution venue of at least
/// one instrument". That is correct for the common configuration where every venue holding
/// instruments is also traded on, and it is the behaviour that predates this parameter — but it is
/// **wrong for a venue that prices instruments without executing any**. Such a venue is assigned
/// [`VenueRole::Both`], its account connection is then waited on forever, and
/// [`ConnectivityStates::global`] never reaches [`Health::Healthy`]. Declaring the venues closes
/// that gap; see [`EngineStateBuilder::execution_venues`](crate::engine::state::builder::EngineStateBuilder::execution_venues).
///
/// # Arguments
/// * `instruments` - Reference to [`IndexedInstruments`] containing what exchanges are being tracked.
/// * `execution_venues` - Venues with a registered execution client, if known. See above.
pub fn generate_empty_indexed_connectivity_states(
    instruments: &IndexedInstruments,
    execution_venues: Option<&FnvHashSet<ExchangeId>>,
) -> ConnectivityStates {
    // Scans the instruments once per exchange rather than building a lookup: this runs once at
    // startup, and the exchange count is a handful even for large instrument collections.
    let exchanges = instruments
        .exchanges()
        .iter()
        .map(|exchange| {
            let exchange = exchange.value;

            let (has_market_data, has_account) =
                derive_venue_dimensions(instruments, exchange, execution_venues);

            let role = VenueRole::from_dimensions(has_market_data, has_account);

            if has_market_data || has_account {
                info!(%exchange, ?role, "EngineState tracking exchange connectivity");
            } else {
                // Every instrument on this venue is priced elsewhere and nothing executes here, so
                // there is no connection it could ever report healthy. Said out loud rather than
                // left to be inferred from a `global` that never leaves `Reconnecting`.
                warn!(
                    %exchange,
                    ?role,
                    "EngineState tracking an exchange that provides neither market data nor \
                     execution - global connectivity cannot reach Healthy while it is tracked"
                );
            }

            (exchange, ConnectivityState::new(role))
        })
        .collect();

    ConnectivityStates {
        global: Health::Reconnecting,
        exchanges,
    }
}

/// Which connection dimensions a venue provides, as `(has_market_data, has_account)`.
///
/// Shared by [`generate_empty_indexed_connectivity_states`] and [`reconcile_venue_roles`] so both
/// answer the question the same way. See the former for what each dimension is derived from, and
/// what `execution_venues: None` approximates.
fn derive_venue_dimensions(
    instruments: &IndexedInstruments,
    exchange: ExchangeId,
    execution_venues: Option<&FnvHashSet<ExchangeId>>,
) -> (bool, bool) {
    let has_market_data = instruments
        .instruments()
        .iter()
        .any(|instrument| instrument.value.data_exchange().value == exchange);

    let has_account = match execution_venues {
        Some(venues) => venues.contains(&exchange),
        None => instruments
            .instruments()
            .iter()
            .any(|instrument| instrument.value.exchange.value == exchange),
    };

    (has_market_data, has_account)
}

/// Re-derives the [`VenueRole`] of every tracked venue from the venues that have a registered
/// execution client, leaving connection [`Health`] untouched.
///
/// For callers that must build their [`EngineState`](super::EngineState) *before* their execution
/// clients exist — [`backtest`](crate::backtest::backtest) does, since it builds one set of clients
/// per run from a state supplied once — this corrects the roles after the fact, rather than
/// obliging the caller to declare venues it cannot yet know. Callers that can declare them upfront
/// should keep using
/// [`EngineStateBuilder::execution_venues`](super::builder::EngineStateBuilder::execution_venues);
/// running both is harmless, since the two derive the same roles from the same inputs.
///
/// Only venues already present in `states` are touched — none are added or removed. The collection
/// is keyed positionally by [`ExchangeIndex`], so inserting one here would renumber every venue
/// after it and silently re-point the instruments indexed against them.
///
/// # Caller obligation
/// `instruments` must be the collection `states` was built from. A mismatched pair derives every
/// dimension against the wrong collection and assigns every venue a plausible but wrong
/// [`VenueRole`] — the `warn!` below fires only for a venue left with neither dimension, not for
/// one merely given the wrong role.
///
/// A `debug_assert!` catches the detectable half of that: `states` holding a different venue set,
/// or holding it in a different order. It cannot catch a collection carrying the *same* venues
/// with different instruments, since the venue keys are then identical — but such a pair also
/// misaligns every [`InstrumentIndex`](rustrade_instrument::index::InstrumentIndex) in the engine,
/// so venue roles are not the symptom that surfaces first.
///
/// # Arguments
/// * `states` - The [`ConnectivityStates`] to correct in place.
/// * `instruments` - The collection `states` was built from.
/// * `execution_venues` - Venues with a registered execution client.
pub fn reconcile_venue_roles(
    states: &mut ConnectivityStates,
    instruments: &IndexedInstruments,
    execution_venues: &FnvHashSet<ExchangeId>,
) {
    // Compared in `ExchangeIndex` order — the order `states` is indexed by — rather than as sets,
    // so a state whose venues were reordered or reindexed is caught too, not merely one holding a
    // different venue set. `debug_assert!` and not a `Result`: passing a mismatched pair is a
    // caller-contract violation (a library-usage bug) rather than a handleable input, and the
    // comparison then costs nothing in release.
    debug_assert!(
        states.exchanges.keys().eq(instruments
            .exchanges()
            .iter()
            .map(|exchange| &exchange.value)),
        "reconcile_venue_roles: `states` was not built from `instruments` — states: {:?}, \
         instruments: {:?}",
        states.exchanges.keys().collect::<Vec<_>>(),
        instruments
            .exchanges()
            .iter()
            .map(|exchange| exchange.value)
            .collect::<Vec<_>>(),
    );

    for (exchange, state) in &mut states.exchanges {
        let (has_market_data, has_account) =
            derive_venue_dimensions(instruments, *exchange, Some(execution_venues));

        if !has_market_data && !has_account {
            // See `generate_empty_indexed_connectivity_states` for why this is said out loud. It
            // is reported here too because this is the first point at which the execution clients
            // are known, and so the first point at which it is detectable at all.
            warn!(
                %exchange,
                "EngineState tracking an exchange that provides neither market data nor \
                 execution - global connectivity cannot reach Healthy while it is tracked"
            );
        }

        let role = VenueRole::from_dimensions(has_market_data, has_account);
        if role == state.role {
            continue;
        }

        // `debug!`, not `info!`: a backtest sweep shares one `BacktestArgsConstant` across every
        // run, so each run reconciles the same state from the same clients and this fires once per
        // run with identical content. The genuinely-wrong case above stays at `warn!`.
        debug!(
            %exchange,
            previous = ?state.role,
            ?role,
            "EngineState correcting venue role against the registered execution clients"
        );
        state.role = role;
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
        let states = generate_empty_indexed_connectivity_states(&split_venue_instruments(), None);

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
        let states = generate_empty_indexed_connectivity_states(&instruments, None);

        assert_eq!(states.exchanges.len(), 1);
        assert_eq!(states.connectivity(&EXECUTION).role, VenueRole::Both);
    }

    #[test]
    fn a_venue_that_prices_one_instrument_and_executes_another_provides_both_dimensions() {
        let instruments = IndexedInstruments::new([
            instrument(EXECUTION, "btc", "usdt").with_data_venue(DataVenue::new_same_name(DATA)),
            instrument(DATA, "eth", "usdt"),
        ]);
        let states = generate_empty_indexed_connectivity_states(&instruments, None);

        assert_eq!(states.connectivity(&DATA).role, VenueRole::Both);
        assert_eq!(
            states.connectivity(&EXECUTION).role,
            VenueRole::ExecutionOnly
        );
    }

    /// The two-instrument pattern: one instrument priced on `DATA` and never traded, a *different*
    /// instrument traded on `EXECUTION`. Nothing executes on `DATA`.
    fn two_instrument_pattern() -> IndexedInstruments {
        IndexedInstruments::new([
            instrument(DATA, "xau", "usd"),
            instrument(EXECUTION, "btc", "usdt"),
        ])
    }

    #[test]
    fn a_venue_that_prices_instruments_without_executing_any_is_data_only() {
        let instruments = two_instrument_pattern();
        let states = generate_empty_indexed_connectivity_states(
            &instruments,
            Some(&FnvHashSet::from_iter([EXECUTION])),
        );

        // `DATA` is the `Instrument::exchange` of the priced instrument, so the instrument model
        // alone claims it holds an account. Only the declared execution venues can say otherwise.
        assert_eq!(states.connectivity(&DATA).role, VenueRole::DataOnly);
        assert_eq!(states.connectivity(&EXECUTION).role, VenueRole::Both);
    }

    #[test]
    fn without_declared_execution_venues_a_pricing_only_venue_is_approximated_as_both() {
        // Pins the documented fallback, and is exactly the configuration it gets wrong: `DATA` is
        // handed an account dimension nothing will ever satisfy. Declaring the execution venues --
        // as `SystemBuilder` does -- is what closes it, per the test above.
        let instruments = two_instrument_pattern();
        let states = generate_empty_indexed_connectivity_states(&instruments, None);

        assert_eq!(states.connectivity(&DATA).role, VenueRole::Both);
    }

    #[test]
    fn global_health_reaches_healthy_when_the_pricing_only_venue_never_reports_an_account() {
        let instruments = two_instrument_pattern();
        let execution_index = instruments.find_exchange_index(EXECUTION).unwrap();
        let mut states = generate_empty_indexed_connectivity_states(
            &instruments,
            Some(&FnvHashSet::from_iter([EXECUTION])),
        );

        // Every connection that exists reports in. `DATA` has no account stream and never will.
        states.update_from_market_event(&DATA).unwrap();
        states.update_from_market_event(&EXECUTION).unwrap();
        assert_eq!(
            states.global,
            Health::Reconnecting,
            "the execution venue's account has not reported yet"
        );
        states.update_from_account_event(&execution_index);

        assert_eq!(states.global, Health::Healthy);
        assert_eq!(
            states.connectivity(&DATA).account,
            Health::Reconnecting,
            "the dimension it does not have stays at its default, and is simply not consulted"
        );
    }

    #[test]
    fn a_venue_providing_neither_dimension_conservatively_demands_both() {
        // Misconfiguration: `EXECUTION` holds an instrument priced elsewhere, and no execution
        // client was registered for it. Reachable only once execution venues are declared. Health
        // is withheld rather than granted to a venue with no known connection.
        let instruments = split_venue_instruments();
        let mut states =
            generate_empty_indexed_connectivity_states(&instruments, Some(&FnvHashSet::default()));

        assert_eq!(states.connectivity(&DATA).role, VenueRole::DataOnly);
        assert_eq!(states.connectivity(&EXECUTION).role, VenueRole::Both);

        // Satisfying one dimension is not enough: `Both` demands the account connection too, which
        // nothing will ever report for a venue with no execution client. Asserting on the state as
        // generated would pass for every role, since both dimensions start `Reconnecting`.
        states.connectivity_mut(&EXECUTION).market_data = Health::Healthy;

        assert!(!states.connectivity(&EXECUTION).all_healthy());
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

        // The mirror of the case above, and the one that pins `has_account` as actually consulted
        // for this role: without it, ignoring the account dimension for everything but `Both`
        // passes every other assertion here.
        assert!(
            !ConnectivityState {
                market_data: Health::Healthy,
                account: Health::Reconnecting,
                role: VenueRole::ExecutionOnly,
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
            let mut states = generate_empty_indexed_connectivity_states(&instruments, None);

            assert_eq!(states.global, Health::Reconnecting);

            if market_data_first {
                states.update_from_market_event(&DATA).unwrap();
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
                states.update_from_market_event(&DATA).unwrap();
            }

            assert_eq!(states.global, Health::Healthy, "{market_data_first}");
        }
    }

    #[test]
    fn an_untracked_exchange_is_reported_even_while_global_health_holds() {
        // The regression this locks: the lookup used to sit *after* a `global == Healthy`
        // short-circuit, so a misconfigured exchange was silently ignored for as long as everything
        // else was healthy, and only panicked once a reconnect dragged global back down. Reaching
        // `Healthy` first is therefore the whole point of this setup.
        let instruments = IndexedInstruments::new([instrument(EXECUTION, "btc", "usdt")]);
        let mut states = generate_empty_indexed_connectivity_states(&instruments, None);

        let execution = instruments.find_exchange_index(EXECUTION).unwrap();
        states.update_from_market_event(&EXECUTION).unwrap();
        states.update_from_account_event(&execution);
        assert_eq!(states.global, Health::Healthy);

        let error = states.update_from_market_event(&DATA).unwrap_err();

        assert_eq!(
            error,
            UntrackedExchange::new(DATA, ConnectivityDimension::MarketData)
        );
        assert_eq!(states.global, Health::Healthy, "the report must not mutate");
        assert_eq!(states.exchanges.len(), 1, "no state may be created for it");
    }

    #[test]
    fn an_untracked_exchange_disconnect_does_not_degrade_global_health() {
        // A disconnect notice from a venue the engine never tracked is not evidence about the
        // venues it does track -- and could never be repaired, since no later event for it can
        // restore a `ConnectivityState` that does not exist.
        let instruments = IndexedInstruments::new([instrument(EXECUTION, "btc", "usdt")]);
        let mut states = generate_empty_indexed_connectivity_states(&instruments, None);

        let execution = instruments.find_exchange_index(EXECUTION).unwrap();
        states.update_from_market_event(&EXECUTION).unwrap();
        states.update_from_account_event(&execution);
        assert_eq!(states.global, Health::Healthy);

        assert_eq!(
            states.update_from_market_reconnecting(&DATA).unwrap_err(),
            UntrackedExchange::new(DATA, ConnectivityDimension::MarketData)
        );
        assert_eq!(
            states.update_from_account_reconnecting(&DATA).unwrap_err(),
            UntrackedExchange::new(DATA, ConnectivityDimension::Account)
        );

        assert_eq!(states.global, Health::Healthy);
    }

    #[test]
    fn a_tracked_exchange_disconnect_still_degrades_global_health() {
        let instruments = IndexedInstruments::new([instrument(EXECUTION, "btc", "usdt")]);
        let mut states = generate_empty_indexed_connectivity_states(&instruments, None);

        let execution = instruments.find_exchange_index(EXECUTION).unwrap();
        states.update_from_market_event(&EXECUTION).unwrap();
        states.update_from_account_event(&execution);
        assert_eq!(states.global, Health::Healthy);

        states.update_from_market_reconnecting(&EXECUTION).unwrap();

        assert_eq!(states.global, Health::Reconnecting);
        assert_eq!(
            states.connectivity(&EXECUTION).market_data,
            Health::Reconnecting
        );
    }

    #[test]
    fn a_tracked_account_disconnect_degrades_the_account_dimension_and_global_health() {
        // The account twin of `a_tracked_exchange_disconnect_still_degrades_global_health`. The two
        // update functions are near-identical, so nothing but a test per dimension catches one
        // written to touch the other's field.
        let instruments = IndexedInstruments::new([instrument(EXECUTION, "btc", "usdt")]);
        let mut states = generate_empty_indexed_connectivity_states(&instruments, None);

        let execution = instruments.find_exchange_index(EXECUTION).unwrap();
        states.update_from_market_event(&EXECUTION).unwrap();
        states.update_from_account_event(&execution);
        assert_eq!(states.global, Health::Healthy);

        states.update_from_account_reconnecting(&EXECUTION).unwrap();

        assert_eq!(
            states.connectivity(&EXECUTION).account,
            Health::Reconnecting
        );
        assert_eq!(
            states.connectivity(&EXECUTION).market_data,
            Health::Healthy,
            "an account disconnect says nothing about the market data connection"
        );
        assert_eq!(states.global, Health::Reconnecting);
    }

    #[test]
    fn reconciling_roles_corrects_a_venue_that_prices_without_executing() {
        // The `backtest` path: the state is built before the execution clients exist, so `DATA` is
        // approximated as `Both` and waits forever on an account. Reconciling against the clients
        // that were ultimately registered is what lets global health converge.
        let instruments = two_instrument_pattern();
        let mut states = generate_empty_indexed_connectivity_states(&instruments, None);
        assert_eq!(states.connectivity(&DATA).role, VenueRole::Both);

        reconcile_venue_roles(
            &mut states,
            &instruments,
            &FnvHashSet::from_iter([EXECUTION]),
        );

        assert_eq!(states.connectivity(&DATA).role, VenueRole::DataOnly);
        assert_eq!(states.connectivity(&EXECUTION).role, VenueRole::Both);
    }

    #[test]
    fn reconciling_roles_preserves_health_and_the_indexed_venue_order() {
        // `ExchangeIndex` is positional into `exchanges`, so reconciliation must correct roles in
        // place -- never insert, remove or reorder -- and must not reset a connection that has
        // already reported in.
        let instruments = two_instrument_pattern();
        let mut states = generate_empty_indexed_connectivity_states(&instruments, None);
        states.update_from_market_event(&DATA).unwrap();

        let order_before = states.exchanges.keys().copied().collect::<Vec<_>>();

        reconcile_venue_roles(
            &mut states,
            &instruments,
            &FnvHashSet::from_iter([EXECUTION]),
        );

        assert_eq!(
            states.exchanges.keys().copied().collect::<Vec<_>>(),
            order_before
        );
        assert_eq!(states.connectivity(&DATA).market_data, Health::Healthy);
    }

    #[test]
    fn reconciling_roles_is_a_no_op_when_the_venues_were_declared_upfront() {
        // `SystemBuilder`-style construction already derives the roles, so running both paths must
        // agree -- otherwise the two entry points would disagree about the same configuration.
        let instruments = two_instrument_pattern();
        let venues = FnvHashSet::from_iter([EXECUTION]);
        let declared = generate_empty_indexed_connectivity_states(&instruments, Some(&venues));

        let mut reconciled = declared.clone();
        reconcile_venue_roles(&mut reconciled, &instruments, &venues);

        assert_eq!(reconciled, declared);
    }

    #[test]
    fn reconciling_roles_reports_a_venue_left_with_neither_dimension() {
        // Misconfiguration: `EXECUTION` holds an instrument priced on `DATA`, and no execution
        // client was registered anywhere. `Both` is the conservative answer, so health stays
        // withheld rather than being granted to a venue with no connection at all.
        let instruments = split_venue_instruments();
        let mut states = generate_empty_indexed_connectivity_states(&instruments, None);

        reconcile_venue_roles(&mut states, &instruments, &FnvHashSet::default());

        assert_eq!(states.connectivity(&DATA).role, VenueRole::DataOnly);
        assert_eq!(states.connectivity(&EXECUTION).role, VenueRole::Both);
    }

    #[test]
    #[cfg(debug_assertions)] // The guard under test compiles out in release
    #[should_panic(expected = "was not built from")]
    fn reconciling_roles_rejects_a_state_built_from_a_different_collection() {
        // The caller obligation, violated: `states` built from one instrument collection and
        // reconciled against another. Every venue would otherwise be handed a plausible role
        // derived from instruments it was never built from, with nothing reported.
        let mut states = generate_empty_indexed_connectivity_states(
            &IndexedInstruments::new([instrument(EXECUTION, "btc", "usdt")]),
            None,
        );

        reconcile_venue_roles(
            &mut states,
            &two_instrument_pattern(),
            &FnvHashSet::from_iter([EXECUTION]),
        );
    }

    #[test]
    fn a_state_persisted_before_venue_roles_deserialises_demanding_both_dimensions() {
        let state: ConnectivityState =
            serde_json::from_str(r#"{"market_data":"Healthy","account":"Reconnecting"}"#).unwrap();

        assert_eq!(state.role, VenueRole::Both);
        assert!(!state.all_healthy());
    }
}
