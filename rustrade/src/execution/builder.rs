use crate::{
    engine::{clock::EngineClock, execution_tx::MultiExchangeTxMap},
    error::BarterError,
    execution::{
        AccountStreamEvent, Execution, error::ExecutionError, manager::ExecutionManager,
        request::ExecutionRequest,
    },
    shutdown::AsyncShutdown,
};
use fnv::FnvHashMap;
use futures::{FutureExt, future::try_join_all};
use rustrade_data::streams::{
    consumer::STREAM_RECONNECTION_POLICY, reconnect::stream::ReconnectingStream,
};
use rustrade_execution::{
    UnindexedAccountEvent,
    client::{
        ExecutionClient,
        mock::{MockExecution, MockExecutionClientConfig, MockExecutionConfig},
    },
    exchange::mock::{MockExchange, request::MockExchangeRequest},
    indexer::AccountEventIndexer,
    map::generate_execution_instrument_map,
};
use rustrade_instrument::{
    Keyed, Underlying,
    asset::{AssetIndex, name::AssetNameExchange},
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{
        Instrument, InstrumentIndex,
        kind::{InstrumentKind, cfd::CfdContract},
        name::InstrumentNameExchange,
        spec::{InstrumentSpec, InstrumentSpecQuantity, OrderQuantityUnits},
    },
};
use rustrade_integration::channel::{Channel, UnboundedTx, mpsc_unbounded};
use std::{pin::Pin, sync::Arc, time::Duration};
use tokio::{
    sync::{broadcast, mpsc},
    task::{AbortHandle, JoinError, JoinHandle},
};

type ExecutionInitFuture =
    Pin<Box<dyn Future<Output = Result<(RunFuture, RunFuture), ExecutionError>> + Send>>;
type RunFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Full execution infrastructure builder.
///
/// Add Mock and Live [`ExecutionClient`] configurations and let the builder set up the required
/// infrastructure.
///
/// Once you have added all the configurations, call [`ExecutionBuilder::build`] to return the
/// full [`ExecutionBuild`]. Then calling [`ExecutionBuild::init`] will then initialise
/// the built infrastructure.
///
/// Handles:
/// - Building mock execution managers (mocks a specific exchange internally via the [`MockExchange`]).
/// - Building live execution managers, setting up an external connection to each exchange.
/// - Constructs a [`MultiExchangeTxMap`] with an entry for each mock/live execution manager.
/// - Combines all exchange account streams into a unified [`AccountStreamEvent`] `Stream`.
// `mock_exchange_futures` and `execution_init_futures` hold `Pin<Box<dyn Future>>`, which has no
// `Debug` to delegate to. A hand-written impl could only print a placeholder for the two fields
// that carry the builder's actual pending work, so it would say less than the type's name already does.
#[allow(missing_debug_implementations)]
pub struct ExecutionBuilder<'a> {
    instruments: &'a IndexedInstruments,
    execution_txs: FnvHashMap<ExchangeId, (ExchangeIndex, UnboundedTx<ExecutionRequest>)>,
    merged_channel: Channel<AccountStreamEvent<ExchangeIndex, AssetIndex, InstrumentIndex>>,
    mock_exchange_futures: Vec<RunFuture>,
    execution_init_futures: Vec<ExecutionInitFuture>,
}

impl<'a> ExecutionBuilder<'a> {
    /// Construct a new `ExecutionBuilder` using the provided `IndexedInstruments`.
    pub fn new(instruments: &'a IndexedInstruments) -> Self {
        Self {
            instruments,
            execution_txs: FnvHashMap::default(),
            merged_channel: Channel::default(),
            mock_exchange_futures: Vec::default(),
            execution_init_futures: Vec::default(),
        }
    }

    /// Adds an [`ExecutionManager`] for a mocked exchange, setting up a [`MockExchange`]
    /// internally.
    ///
    /// The provided [`MockExecutionConfig`] is used to configure the [`MockExchange`] and provide
    /// the initial account state.
    ///
    /// # Panics
    /// Despite returning a `Result`, this **panics** if any indexed instrument on `mocked_exchange`
    /// has an [`InstrumentKind`] other than `Spot` or `Cfd`. `MockExchange` models no expiry,
    /// funding or contract chain, so a `Perpetual`, `Future` or `Option` has no faithful
    /// projection. The panic happens here, at build time, rather than at first order — an
    /// unbacktestable instrument set is a configuration error, and deferring it would surface as a
    /// rejected order mid-run.
    ///
    /// It also panics if an instrument references a settlement asset absent from the index, which
    /// [`IndexedInstruments`] construction already rules out.
    ///
    /// [`InstrumentKind`]: rustrade_instrument::instrument::kind::InstrumentKind
    /// [`IndexedInstruments`]: rustrade_instrument::index::IndexedInstruments
    pub fn add_mock<Clock>(
        mut self,
        config: MockExecutionConfig,
        clock: Clock,
    ) -> Result<Self, BarterError>
    where
        Clock: EngineClock + Clone + Send + Sync + 'static,
    {
        const ACCOUNT_STREAM_CAPACITY: usize = 256;
        const DUMMY_EXECUTION_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = broadcast::channel(ACCOUNT_STREAM_CAPACITY);

        let mock_execution_client_config = MockExecutionClientConfig {
            mocked_exchange: config.mocked_exchange,
            clock: move || clock.time(),
            request_tx,
            event_rx,
        };

        // Register MockExchange init Future
        let mock_exchange_future = self.init_mock_exchange(config, request_rx, event_tx);
        self.mock_exchange_futures.push(mock_exchange_future);

        self.add_execution::<MockExecution<_>>(
            mock_execution_client_config.mocked_exchange,
            mock_execution_client_config,
            DUMMY_EXECUTION_REQUEST_TIMEOUT,
        )
    }

    fn init_mock_exchange(
        &self,
        config: MockExecutionConfig,
        request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
        event_tx: broadcast::Sender<UnindexedAccountEvent>,
    ) -> RunFuture {
        let instruments =
            generate_mock_exchange_instruments(self.instruments, config.mocked_exchange);
        Box::pin(MockExchange::new(config, request_rx, event_tx, instruments).run())
    }

    /// Adds an [`ExecutionManager`] for a live exchange.
    pub fn add_live<Client>(
        self,
        config: Client::Config,
        request_timeout: Duration,
    ) -> Result<Self, BarterError>
    where
        Client: ExecutionClient + Send + Sync + 'static,
        Client::AccountStream: Send,
        Client::Config: Send,
    {
        self.add_execution::<Client>(Client::EXCHANGE, config, request_timeout)
    }

    fn add_execution<Client>(
        mut self,
        exchange: ExchangeId,
        config: Client::Config,
        request_timeout: Duration,
    ) -> Result<Self, BarterError>
    where
        Client: ExecutionClient + Send + Sync + 'static,
        Client::AccountStream: Send,
        Client::Config: Send,
    {
        let instrument_map = generate_execution_instrument_map(self.instruments, exchange)?;

        let (execution_tx, execution_rx) = mpsc_unbounded();

        if self
            .execution_txs
            .insert(exchange, (instrument_map.exchange.key, execution_tx))
            .is_some()
        {
            return Err(BarterError::ExecutionBuilder(format!(
                "ExecutionBuilder does not support duplicate mocked ExecutionManagers: {exchange}"
            )));
        }

        let merged_tx = self.merged_channel.tx.clone();

        // Init ExecutionManager Future
        let future_result = ExecutionManager::init(
            execution_rx.into_stream(),
            request_timeout,
            Arc::new(Client::new(config)),
            AccountEventIndexer::new(Arc::new(instrument_map)),
            STREAM_RECONNECTION_POLICY,
        );

        let future_result = future_result.map(|result| {
            result.map(|(manager, account_stream)| {
                let manager_future: RunFuture = Box::pin(manager.run());
                let stream_future: RunFuture = Box::pin(account_stream.forward_to(merged_tx));

                (manager_future, stream_future)
            })
        });

        self.execution_init_futures.push(Box::pin(future_result));

        Ok(self)
    }

    /// Consume this `ExecutionBuilder` and build a full [`ExecutionBuild`] containing all the
    /// [`ExecutionManager`] (mock & live) and [`MockExchange`] futures.
    ///
    /// **For most users, calling [`ExecutionBuild::init`] after this is satisfactory.**
    ///
    /// If you want more control over what runtime drives the futures to completion, you can
    /// call [`ExecutionBuild::init_with_runtime`].
    pub fn build(mut self) -> ExecutionBuild {
        // Construct indexed ExecutionTx map
        let execution_tx_map = self
            .instruments
            .exchanges()
            .iter()
            .map(|exchange| {
                // If IndexedInstruments execution not used for execution, add None to map
                let Some((added_execution_exchange_index, added_execution_exchange_tx)) =
                    self.execution_txs.remove(&exchange.value)
                else {
                    return (exchange.value, None);
                };

                assert_eq!(
                    exchange.key, added_execution_exchange_index,
                    "execution ExchangeIndex != IndexedInstruments Keyed<ExchangeIndex, ExchangeId>"
                );

                // If execution has been added, add Some(ExecutionTx) to map
                (exchange.value, Some(added_execution_exchange_tx))
            })
            .collect();

        ExecutionBuild {
            execution_tx_map,
            account_channel: self.merged_channel,
            futures: ExecutionBuildFutures {
                mock_exchange_run_futures: self.mock_exchange_futures,
                execution_init_futures: self.execution_init_futures,
            },
        }
    }
}

/// Container holding execution infrastructure components ready to be initialised.
///
/// Call [`ExecutionBuild::init`] to run all the required execution component futures on tokio
/// tasks - returns the [`MultiExchangeTxMap`] and multi-exchange [`AccountStreamEvent`] stream.
// Holds an `ExecutionBuildFutures`, whose boxed futures have no `Debug` — see below.
#[allow(missing_debug_implementations)]
pub struct ExecutionBuild {
    pub execution_tx_map: MultiExchangeTxMap,
    pub account_channel: Channel<AccountStreamEvent>,
    pub futures: ExecutionBuildFutures,
}

impl ExecutionBuild {
    /// Initialises all execution components on the current tokio runtime.
    ///
    /// This method:
    /// - Spawns [`MockExchange`] runners tokio tasks.
    /// - Initialises all [`ExecutionManager`]s and their AccountStreams.
    /// - Returns the `MultiExchangeTxMap` and multi-exchange AccountStream.
    pub async fn init(self) -> Result<Execution, BarterError> {
        self.init_internal(tokio::runtime::Handle::current()).await
    }

    /// Initialises all execution components on the provided tokio runtime.
    ///
    /// Use this method if you want more control over which tokio runtime handles running
    /// execution components.
    ///
    /// This method:
    /// - Spawns [`MockExchange`] runners tokio tasks.
    /// - Initialises all [`ExecutionManager`]s and their AccountStreams.
    /// - Returns the `MultiExchangeTxMap` and multi-exchange AccountStream.
    pub async fn init_with_runtime(
        self,
        runtime: tokio::runtime::Handle,
    ) -> Result<Execution, BarterError> {
        self.init_internal(runtime).await
    }

    async fn init_internal(
        self,
        runtime: tokio::runtime::Handle,
    ) -> Result<Execution, BarterError> {
        let handles = self.futures.init_with_runtime(runtime).await?;

        Ok(Execution {
            execution_txs: self.execution_tx_map,
            account_channel: self.account_channel,
            handles,
        })
    }
}

// Both fields are collections of `Pin<Box<dyn Future>>`, which has no `Debug` to delegate to.
#[allow(missing_debug_implementations)]
pub struct ExecutionBuildFutures {
    pub mock_exchange_run_futures: Vec<RunFuture>,
    pub execution_init_futures: Vec<ExecutionInitFuture>,
}

impl ExecutionBuildFutures {
    /// Initialises all execution components on the current tokio runtime.
    ///
    /// This method:
    /// - Spawns [`MockExchange`] runner tokio tasks.
    /// - Initialises all [`ExecutionManager`]s and their AccountStreams.
    /// - Spawns tokio tasks to forward AccountStreams to multi-exchange AccountStream
    pub async fn init(self) -> Result<ExecutionHandles, BarterError> {
        self.init_internal(tokio::runtime::Handle::current()).await
    }

    /// Initialises all execution components on the provided tokio runtime.
    ///
    /// Use this method if you want more control over which tokio runtime handles running
    /// execution components.
    ///
    /// This method:
    /// - Spawns [`MockExchange`] runner tokio tasks.
    /// - Initialises all [`ExecutionManager`]s and their AccountStreams.
    /// - Spawns tokio tasks to forward AccountStreams to multi-exchange AccountStream
    pub async fn init_with_runtime(
        self,
        runtime: tokio::runtime::Handle,
    ) -> Result<ExecutionHandles, BarterError> {
        self.init_internal(runtime).await
    }

    async fn init_internal(
        self,
        runtime: tokio::runtime::Handle,
    ) -> Result<ExecutionHandles, BarterError> {
        let mock_exchanges = self
            .mock_exchange_run_futures
            .into_iter()
            .map(|mock_exchange_run_future| runtime.spawn(mock_exchange_run_future))
            .collect();

        // Await ExecutionManager build futures and ensure success
        let (managers, account_to_engines) =
            futures::future::try_join_all(self.execution_init_futures)
                .await?
                .into_iter()
                .map(|(manager_run_future, account_event_forward_future)| {
                    (
                        runtime.spawn(manager_run_future),
                        runtime.spawn(account_event_forward_future),
                    )
                })
                .unzip();

        Ok(ExecutionHandles {
            mock_exchanges,
            managers,
            account_to_engines,
        })
    }
}

#[derive(Debug)]
pub struct ExecutionHandles {
    pub mock_exchanges: Vec<JoinHandle<()>>,
    pub managers: Vec<JoinHandle<()>>,
    pub account_to_engines: Vec<JoinHandle<()>>,
}

impl AsyncShutdown for ExecutionHandles {
    type Result = Result<(), JoinError>;

    async fn shutdown(&mut self) -> Self::Result {
        let handles = self
            .mock_exchanges
            .drain(..)
            .chain(self.managers.drain(..))
            .chain(self.account_to_engines.drain(..));

        try_join_all(handles).await?;
        Ok(())
    }
}

impl ExecutionHandles {
    /// [`AbortHandle`]s for every execution task, without consuming the handles.
    ///
    /// Enumerates the same task set as [`IntoIterator`] — a task added to this struct must be added
    /// to both.
    pub(crate) fn abort_handles(&self) -> impl Iterator<Item = AbortHandle> + '_ {
        self.mock_exchanges
            .iter()
            .chain(&self.managers)
            .chain(&self.account_to_engines)
            .map(JoinHandle::abort_handle)
    }
}

impl IntoIterator for ExecutionHandles {
    type Item = JoinHandle<()>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.mock_exchanges
            .into_iter()
            .chain(self.managers)
            .chain(self.account_to_engines)
            .collect::<Vec<_>>()
            .into_iter()
    }
}

/// Project indexed instruments onto the `MockExchange`'s own instrument representation.
///
/// # Supported [`InstrumentKind`]s
/// `MockExchange` models `Spot` and `Cfd`, and **panics** on any other kind. This is a capability
/// limit of the mock — it fills at price × quantity with no expiry, settlement or contract chain —
/// not a statement about which kinds are executable in general.
///
/// `Cfd` is supported because the mock accounts for a cash-settled position directly: it applies
/// `contract_size` to the notional and the fee, and debits the **quote** asset in both directions.
/// Without it every CFD-quoted dataset would panic at execution-build time and be unbacktestable.
///
/// `settlement_asset` is re-resolved to its exchange name so the projected instrument describes
/// itself faithfully, but the mock does not settle in it — see [`MockExchange`]'s limitations for
/// why, and for the balances a caller must fund.
///
/// [`MockExchange`]: rustrade_execution::exchange::mock::MockExchange
#[allow(clippy::unwrap_used)] // Invariant: IndexedInstruments - all referenced assets exist; panics for unsupported InstrumentKind
fn generate_mock_exchange_instruments(
    instruments: &IndexedInstruments,
    exchange: ExchangeId,
) -> FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>> {
    instruments
        .instruments()
        .iter()
        .filter_map(
            |Keyed {
                 key: _,
                 value: instrument,
             }| {
                if instrument.exchange.value != exchange {
                    return None;
                }

                let Instrument {
                    exchange,
                    name_internal,
                    name_exchange,
                    underlying,
                    quote,
                    kind,
                    spec,
                } = instrument;

                let kind = match kind {
                    InstrumentKind::Spot => InstrumentKind::Spot,
                    // A CFD is cash-settled in an account currency that is routinely not the quote
                    // asset, so the settlement asset is re-resolved to its exchange name here
                    // rather than dropped.
                    InstrumentKind::Cfd(contract) => InstrumentKind::Cfd(CfdContract {
                        contract_size: contract.contract_size,
                        settlement_asset: instruments
                            .find_asset(contract.settlement_asset)
                            .unwrap()
                            .asset
                            .name_exchange
                            .clone(),
                    }),
                    unsupported => {
                        panic!("MockExchange does not support: {unsupported:?}")
                    }
                };

                let spec = match spec {
                    Some(spec) => {
                        let InstrumentSpec {
                            price,
                            quantity:
                                InstrumentSpecQuantity {
                                    unit,
                                    min,
                                    increment,
                                },
                            notional,
                        } = spec;

                        let unit = match unit {
                            OrderQuantityUnits::Asset(asset) => {
                                let quantity_asset = instruments
                                    .find_asset(*asset)
                                    .unwrap()
                                    .asset
                                    .name_exchange
                                    .clone();
                                OrderQuantityUnits::Asset(quantity_asset)
                            }
                            OrderQuantityUnits::Contract => OrderQuantityUnits::Contract,
                            OrderQuantityUnits::Quote => OrderQuantityUnits::Quote,
                        };

                        Some(InstrumentSpec {
                            price: *price,
                            quantity: InstrumentSpecQuantity {
                                unit,
                                min: *min,
                                increment: *increment,
                            },
                            notional: *notional,
                        })
                    }
                    None => None,
                };

                let underlying_base = instruments
                    .find_asset(underlying.base)
                    .unwrap()
                    .asset
                    .name_exchange
                    .clone();

                let underlying_quote = instruments
                    .find_asset(underlying.quote)
                    .unwrap()
                    .asset
                    .name_exchange
                    .clone();

                let instrument = Instrument {
                    exchange: exchange.value,
                    name_internal: name_internal.clone(),
                    name_exchange: name_exchange.clone(),
                    underlying: Underlying {
                        base: underlying_base,
                        quote: underlying_quote,
                    },
                    quote: *quote,
                    kind,
                    spec,
                };

                Some((instrument.name_exchange.clone(), instrument))
            },
        )
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panicking on a bad fixture is acceptable
mod tests {
    use super::*;
    use crate::engine::clock::HistoricalClock;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_execution::{AccountSnapshot, client::mock::MockExecutionConfig};
    use rustrade_instrument::{
        asset::Asset,
        index::builder::IndexedInstrumentsBuilder,
        instrument::{kind::future::FutureContract, quote::InstrumentQuoteAsset},
        test_utils::asset,
    };

    const EXCHANGE: ExchangeId = ExchangeId::LseCfd;

    fn at(raw: &str) -> DateTime<Utc> {
        raw.parse().unwrap()
    }

    /// A single `spx500_usd` instrument of the given kind, registered on [`EXCHANGE`].
    fn instruments(kind: InstrumentKind<Asset>) -> IndexedInstruments {
        IndexedInstrumentsBuilder::default()
            .add_instrument(Instrument::new(
                EXCHANGE,
                "spx500_usd",
                "spx500_usd",
                Underlying::new(asset("spx500"), asset("usd")),
                InstrumentQuoteAsset::UnderlyingQuote,
                kind,
                None,
            ))
            .build()
    }

    /// A CFD settling in an asset that is **not** the quote — a GBP-denominated account trading a
    /// USD-quoted index CFD, which is the case that makes re-resolving the settlement asset
    /// load-bearing rather than incidental.
    fn cfd() -> InstrumentKind<Asset> {
        InstrumentKind::Cfd(CfdContract {
            contract_size: dec!(25),
            settlement_asset: asset("gbp"),
        })
    }

    fn mock_config() -> MockExecutionConfig {
        MockExecutionConfig {
            mocked_exchange: EXCHANGE,
            initial_state: AccountSnapshot {
                exchange: EXCHANGE,
                balances: vec![],
                instruments: vec![],
            },
            latency_ms: 0,
            fee_model: Default::default(),
            fill_model: Default::default(),
        }
    }

    /// A CFD survives the projection with its multiplier intact and its settlement asset
    /// re-resolved to the exchange-facing name — not dropped, and not silently replaced by the
    /// quote asset.
    #[test]
    fn mock_instruments_map_a_cfd_preserving_contract_size_and_settlement_asset() {
        let instruments = instruments(cfd());

        let mocked = generate_mock_exchange_instruments(&instruments, EXCHANGE);

        let instrument = mocked
            .get(&InstrumentNameExchange::from("spx500_usd"))
            .expect("the CFD instrument must be projected onto the mock exchange");

        let InstrumentKind::Cfd(contract) = &instrument.kind else {
            panic!("expected a Cfd kind, got {:?}", instrument.kind)
        };
        assert_eq!(contract.contract_size, dec!(25));
        assert_eq!(
            contract.settlement_asset,
            AssetNameExchange::from("gbp"),
            "settlement is the account currency, not the quote asset"
        );
        assert_eq!(instrument.underlying.quote, AssetNameExchange::from("usd"));
    }

    /// The regression that made CFD-quoted datasets unbacktestable: the projection ran during
    /// `add_mock`, so a CFD instrument panicked at execution-build time — before a single event was
    /// replayed. Asserting on `add_mock` rather than the private projection is deliberate; that is
    /// the call site a backtest actually reaches.
    #[test]
    fn add_mock_accepts_a_cfd_instrument() {
        let instruments = instruments(cfd());

        ExecutionBuilder::new(&instruments)
            .add_mock(
                mock_config(),
                HistoricalClock::new(at("2025-03-24T22:00:00Z")),
            )
            .expect("building mock execution over a CFD instrument must succeed");
    }

    #[test]
    fn mock_instruments_map_spot_unchanged() {
        let instruments = instruments(InstrumentKind::Spot);

        let mocked = generate_mock_exchange_instruments(&instruments, EXCHANGE);

        let instrument = mocked
            .get(&InstrumentNameExchange::from("spx500_usd"))
            .unwrap();
        assert_eq!(instrument.kind, InstrumentKind::Spot);
    }

    /// The capability limit is real, not incidental: kinds the mock cannot fill still panic, so
    /// adding `Cfd` did not quietly widen the mock to everything.
    #[test]
    #[should_panic(expected = "MockExchange does not support")]
    fn mock_instruments_panic_on_an_unsupported_kind() {
        let instruments = instruments(InstrumentKind::Future(FutureContract {
            contract_size: dec!(1),
            settlement_asset: asset("usd"),
            expiry: at("2025-06-27T00:00:00Z"),
        }));

        let _ = generate_mock_exchange_instruments(&instruments, EXCHANGE);
    }

    /// Instruments on another venue are filtered out before the kind match, so an unsupported kind
    /// elsewhere in the registry cannot panic a mock that does not serve it.
    #[test]
    fn mock_instruments_exclude_other_exchanges() {
        let instruments = instruments(cfd());

        let mocked = generate_mock_exchange_instruments(&instruments, ExchangeId::BinanceSpot);

        assert!(mocked.is_empty());
    }
}
