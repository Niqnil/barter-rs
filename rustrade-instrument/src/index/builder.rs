use crate::{
    Keyed,
    asset::{Asset, AssetIndex, ExchangeAsset},
    exchange::{ExchangeId, ExchangeIndex},
    index::{
        IndexedInstruments, error::IndexError, find_asset_by_exchange_and_name_internal,
        find_exchange_by_exchange_id,
    },
    instrument::{Instrument, InstrumentIndex, spec::OrderQuantityUnits},
};
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct IndexedInstrumentsBuilder {
    exchanges: Vec<ExchangeId>,
    instruments: Vec<Instrument<ExchangeId, Asset>>,
    assets: Vec<ExchangeAsset<Asset>>,
}

impl IndexedInstrumentsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_instrument(mut self, instrument: Instrument<ExchangeId, Asset>) -> Self {
        // Add ExchangeId
        self.exchanges.push(instrument.exchange);

        // Add Underlying base
        self.assets.push(ExchangeAsset::new(
            instrument.exchange,
            instrument.underlying.base.clone(),
        ));

        // Add Underlying quote
        self.assets.push(ExchangeAsset::new(
            instrument.exchange,
            instrument.underlying.quote.clone(),
        ));

        // If Perpetual, Future, or Option, add settlement asset
        if let Some(settlement_asset) = instrument.kind.settlement_asset() {
            self.assets.push(ExchangeAsset::new(
                instrument.exchange,
                settlement_asset.clone(),
            ));
        }

        // Add Instrument OrderQuantityUnits if it's defined in asset units
        // --> likely a duplicate asset, but if so will be filtered during Self::build()
        if let Some(spec) = instrument.spec.as_ref()
            && let OrderQuantityUnits::Asset(asset) = &spec.quantity.unit
        {
            self.assets
                .push(ExchangeAsset::new(instrument.exchange, asset.clone()));
        }

        // Add Instrument
        self.instruments.push(instrument);

        self
    }

    /// Builds an [`IndexedInstruments`], panicking if the added [`Instrument`]s are not a valid
    /// collection.
    ///
    /// # Panics
    /// Panics if two added `Instrument`s share an
    /// [`InstrumentNameInternal`](crate::instrument::name::InstrumentNameInternal) — see
    /// [`Self::try_build`], which returns that as an [`IndexError`] instead.
    pub fn build(self) -> IndexedInstruments {
        // Deliberate panic: `build` is the infallible convenience over `try_build`, and the sole
        // failure is a caller error that must not be silently tolerated (see `try_build`'s
        // rustdoc). Panics with the error's `Display`, not `expect`'s `Debug`, so the operator
        // reads the duplicate rather than a quoted struct dump.
        #[allow(clippy::panic)] // Documented in this method's `# Panics` section.
        self.try_build()
            .unwrap_or_else(|error| panic!("failed to build IndexedInstruments: {error}"))
    }

    /// Builds an [`IndexedInstruments`], returning an [`IndexError`] if the added [`Instrument`]s
    /// are not a valid collection.
    ///
    /// # Errors
    /// Returns [`IndexError::DuplicateInstrumentNameInternal`] if two added `Instrument`s share an
    /// [`InstrumentNameInternal`](crate::instrument::name::InstrumentNameInternal).
    ///
    /// `InstrumentNameInternal` must be unique across the collection: downstream state maps are
    /// keyed on it while being read **positionally** by [`InstrumentIndex`], so a duplicate
    /// collapses two instruments into one map entry and silently shifts every index past the
    /// collision onto the wrong instrument — attaching positions, PnL and orders to the wrong
    /// place, with no error until the final index panics. Note that the `Instrument` dedup below
    /// does **not** cover this: it removes only instruments that are equal in *every* field, so
    /// two genuinely different instruments that merely share a name survive it.
    pub fn try_build(mut self) -> Result<IndexedInstruments, IndexError> {
        // Sort & dedup
        self.exchanges.sort();
        self.exchanges.dedup();
        self.instruments.sort();
        self.instruments.dedup();
        self.assets.sort();
        self.assets.dedup();

        // Enforce the InstrumentNameInternal uniqueness invariant that index-keyed state assumes.
        // Checked after the dedup above so exact duplicate `Instrument`s -- which are legitimate
        // input, already collapsed to one -- do not trip it.
        let mut names = HashMap::with_capacity(self.instruments.len());
        for instrument in &self.instruments {
            if let Some(previous) =
                names.insert(&instrument.name_internal, &instrument.name_exchange)
            {
                return Err(IndexError::DuplicateInstrumentNameInternal(format!(
                    "{} is shared by the distinct instruments {} and {} on {} - \
                     every Instrument requires a unique name_internal",
                    instrument.name_internal,
                    previous,
                    instrument.name_exchange,
                    // `as_str`, not `Display`: the canonical snake_case spelling users write in
                    // configs, rather than the bare variant name.
                    instrument.exchange.as_str(),
                )));
            }
        }

        // Index Exchanges
        let exchanges = self
            .exchanges
            .into_iter()
            .enumerate()
            .map(|(index, exchange)| Keyed::new(ExchangeIndex::new(index), exchange))
            .collect::<Vec<_>>();

        // Index Assets
        let assets = self
            .assets
            .into_iter()
            .enumerate()
            .map(|(index, exchange_asset)| Keyed::new(AssetIndex::new(index), exchange_asset))
            .collect::<Vec<_>>();

        // Index Instruments (also maps any Instrument AssetKeys -> AssetIndex)
        let instruments = self
            .instruments
            .into_iter()
            .enumerate()
            .map(|(index, instrument)| {
                let exchange_id = instrument.exchange;
                #[allow(clippy::expect_used)]
                // Invariant: add_instrument populates exchanges alongside each instrument
                let exchange_key = find_exchange_by_exchange_id(&exchanges, &exchange_id)
                    .expect("every exchange related to every instrument has been added");

                let instrument = instrument.map_exchange_key(Keyed::new(exchange_key, exchange_id));

                #[allow(clippy::expect_used)]
                // Invariant: add_instrument populates assets alongside each instrument
                let instrument = instrument
                    .map_asset_key_with_lookup(|asset: &Asset| {
                        find_asset_by_exchange_and_name_internal(
                            &assets,
                            exchange_id,
                            &asset.name_internal,
                        )
                    })
                    .expect("every asset related to every instrument has been added");

                Keyed::new(InstrumentIndex::new(index), instrument)
            })
            .collect();

        Ok(IndexedInstruments {
            exchanges,
            instruments,
            assets,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        Underlying,
        instrument::{
            kind::InstrumentKind,
            name::{InstrumentNameExchange, InstrumentNameInternal},
            quote::InstrumentQuoteAsset,
            spec::{
                InstrumentSpec, InstrumentSpecNotional, InstrumentSpecPrice, InstrumentSpecQuantity,
            },
        },
        test_utils::{exchange_asset, instrument},
    };
    use rust_decimal_macros::dec;

    #[test]
    fn test_builder_basic_spot() {
        // Add single spot instrument
        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(instrument(ExchangeId::BinanceSpot, "btc", "usdt"))
            .build();

        // Verify state
        assert_eq!(indexed.exchanges().len(), 1);
        assert_eq!(indexed.assets().len(), 2); // BTC and USDT
        assert_eq!(indexed.instruments().len(), 1);

        // Verify exchanges indexes
        assert_eq!(indexed.exchanges()[0].value, ExchangeId::BinanceSpot);

        // Verify asset indexes
        assert_eq!(
            indexed.assets()[0].value,
            exchange_asset(ExchangeId::BinanceSpot, "btc"),
        );
        assert_eq!(
            indexed.assets()[1].value,
            exchange_asset(ExchangeId::BinanceSpot, "usdt"),
        );

        // Very instrument indexes
        assert_eq!(
            indexed.instruments()[0].value,
            Instrument {
                exchange: Keyed::new(ExchangeIndex(0), ExchangeId::BinanceSpot),
                name_exchange: InstrumentNameExchange::new("btc_usdt"),
                name_internal: InstrumentNameInternal::new("binance_spot-btc_usdt"),
                underlying: Underlying {
                    base: AssetIndex(0),
                    quote: AssetIndex(1),
                },
                quote: InstrumentQuoteAsset::UnderlyingQuote,
                kind: InstrumentKind::Spot,
                spec: None
            }
        );
    }

    #[test]
    fn test_builder_deduplication() {
        // Add same spot instrument twice
        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(instrument(ExchangeId::BinanceSpot, "BTC", "USDT"))
            .add_instrument(instrument(ExchangeId::BinanceSpot, "BTC", "USDT"))
            .build();

        // Should deduplicate exchanges and assets, but not instruments
        assert_eq!(indexed.exchanges().len(), 1); // Exchange are de-duped
        assert_eq!(indexed.assets().len(), 2); // BTC and USDT and de-duped
        assert_eq!(indexed.instruments().len(), 1); // Instruments are de-duped
    }

    #[test]
    fn test_builder_rejects_duplicate_name_internal() {
        // Two genuinely DIFFERENT instruments that share a `name_internal`. The full-equality
        // `Instrument` dedup does not collapse these, so without the guard they would reach
        // `InstrumentStates` as one map entry, silently shifting every later `InstrumentIndex`
        // onto the wrong instrument.
        let shared_name = "binance_spot-btc_usdt";

        let build = |name_exchange| {
            Instrument::new(
                ExchangeId::BinanceSpot,
                shared_name,
                name_exchange,
                Underlying::new(
                    Asset::new_from_exchange("btc"),
                    Asset::new_from_exchange("usdt"),
                ),
                InstrumentQuoteAsset::UnderlyingQuote,
                InstrumentKind::Spot,
                None,
            )
        };

        let error = IndexedInstrumentsBuilder::default()
            .add_instrument(build("BTCUSDT"))
            .add_instrument(build("BTC-USDT"))
            .try_build()
            .expect_err("duplicate name_internal must be rejected");

        let IndexError::DuplicateInstrumentNameInternal(message) = &error else {
            panic!("unexpected error variant: {error:?}")
        };

        // The message must name the duplicate and both instruments, or it cannot be acted on.
        assert!(message.contains(shared_name), "{message}");
        assert!(message.contains("BTCUSDT"), "{message}");
        assert!(message.contains("BTC-USDT"), "{message}");
        assert!(message.contains("binance_spot"), "{message}");
    }

    #[test]
    #[should_panic(expected = "duplicate InstrumentNameInternal")]
    fn test_builder_build_panics_on_duplicate_name_internal() {
        let build = |name_exchange| {
            Instrument::new(
                ExchangeId::BinanceSpot,
                "binance_spot-btc_usdt",
                name_exchange,
                Underlying::new(
                    Asset::new_from_exchange("btc"),
                    Asset::new_from_exchange("usdt"),
                ),
                InstrumentQuoteAsset::UnderlyingQuote,
                InstrumentKind::Spot,
                None,
            )
        };

        let _ = IndexedInstrumentsBuilder::default()
            .add_instrument(build("BTCUSDT"))
            .add_instrument(build("BTC-USDT"))
            .build();
    }

    #[test]
    fn test_builder_exact_duplicate_instruments_are_not_a_name_collision() {
        // The same instrument added twice is legitimate input -- the pre-existing dedup collapses
        // it to one -- so the uniqueness guard must not fire. Pins the guard's position after the
        // dedup rather than before it.
        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(instrument(ExchangeId::BinanceSpot, "btc", "usdt"))
            .add_instrument(instrument(ExchangeId::BinanceSpot, "btc", "usdt"))
            .try_build()
            .expect("exact duplicates are deduped, not a name collision");

        assert_eq!(indexed.instruments().len(), 1);
    }

    #[test]
    fn test_builder_multiple_exchanges() {
        // Add instruments from different exchanges
        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(instrument(ExchangeId::BinanceSpot, "BTC", "USDT"))
            .add_instrument(instrument(ExchangeId::Coinbase, "BTC", "USD"))
            .build();

        // Should maintain separate indices for same asset on different exchanges
        assert_eq!(indexed.exchanges().len(), 2);
        assert_eq!(indexed.assets().len(), 4); // BTC on both exchanges, USDT and USD
        assert_eq!(indexed.instruments().len(), 2);
    }

    #[test]
    fn test_builder_asset_unit_handling() {
        // Create instrument with asset-based order quantity
        let base_asset = Asset::new_from_exchange("BTC");
        let quote_asset = Asset::new_from_exchange("USDT");

        let instrument = Instrument::new(
            ExchangeId::BinanceSpot,
            "binance_spot_btc_usdt",
            "BTC-USDT",
            Underlying::new(base_asset.clone(), quote_asset.clone()),
            InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Spot,
            Some(InstrumentSpec {
                price: InstrumentSpecPrice {
                    min: dec!(0.1),
                    tick_size: dec!(0.1),
                },
                quantity: InstrumentSpecQuantity {
                    unit: OrderQuantityUnits::Asset(base_asset.clone()),
                    min: dec!(0.001),
                    increment: dec!(0.001),
                },
                notional: InstrumentSpecNotional { min: dec!(10) },
            }),
        );

        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(instrument)
            .build();

        // Should index the asset used in OrderQuantityUnits
        assert_eq!(indexed.assets().len(), 2);
        assert_eq!(
            indexed.assets()[0].value,
            exchange_asset(ExchangeId::BinanceSpot, "BTC")
        );
    }

    #[test]
    fn test_builder_ordering() {
        // Add instruments in any order
        let indexed = IndexedInstrumentsBuilder::default()
            .add_instrument(instrument(ExchangeId::BinanceSpot, "BTC", "USDT"))
            .add_instrument(instrument(ExchangeId::Coinbase, "ETH", "USD"))
            .build();

        // Verify exchanges are ordered by input sequence
        assert_eq!(indexed.exchanges()[0].value, ExchangeId::BinanceSpot);
        assert_eq!(indexed.exchanges()[1].value, ExchangeId::Coinbase);

        // Verify exchanges are ordered by input sequence
        assert_eq!(
            indexed.assets()[0].value,
            exchange_asset(ExchangeId::BinanceSpot, "BTC")
        );
        assert_eq!(
            indexed.assets()[1].value,
            exchange_asset(ExchangeId::BinanceSpot, "USDT")
        );
        assert_eq!(
            indexed.assets()[2].value,
            exchange_asset(ExchangeId::Coinbase, "ETH")
        );
        assert_eq!(
            indexed.assets()[3].value,
            exchange_asset(ExchangeId::Coinbase, "USD")
        );

        // Verify instruments are ordered by input sequence
        assert_eq!(
            indexed.instruments()[0].value,
            Instrument {
                exchange: Keyed::new(ExchangeIndex(0), ExchangeId::BinanceSpot),
                name_exchange: InstrumentNameExchange::new("BTC_USDT"),
                name_internal: InstrumentNameInternal::new("binance_spot-btc_usdt"),
                underlying: Underlying {
                    base: AssetIndex(0),
                    quote: AssetIndex(1),
                },
                quote: InstrumentQuoteAsset::UnderlyingQuote,
                kind: InstrumentKind::Spot,
                spec: None
            }
        );

        assert_eq!(
            indexed.instruments()[1].value,
            Instrument {
                exchange: Keyed::new(ExchangeIndex(1), ExchangeId::Coinbase),
                name_exchange: InstrumentNameExchange::new("ETH_USD"),
                name_internal: InstrumentNameInternal::new("coinbase-eth_usd"),
                underlying: Underlying {
                    base: AssetIndex(2),
                    quote: AssetIndex(3),
                },
                quote: InstrumentQuoteAsset::UnderlyingQuote,
                kind: InstrumentKind::Spot,
                spec: None
            }
        );
    }
}
