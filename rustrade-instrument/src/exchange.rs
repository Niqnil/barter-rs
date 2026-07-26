use derive_more::{Constructor, Display};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct ExchangeIndex(pub usize);

impl ExchangeIndex {
    pub fn index(&self) -> usize {
        self.0
    }
}

impl std::fmt::Display for ExchangeIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ExchangeIndex({})", self.0)
    }
}

/// Unique identifier for an execution server.
///
/// ### Notes
/// An execution may have a distinct server for different
/// [`InstrumentKinds`](super::instrument::kind::InstrumentKind).
///
/// For example, BinanceSpot and BinanceFuturesUsd have distinct APIs, and are therefore
/// represented as unique variants.
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Display,
)]
#[serde(rename = "execution", rename_all = "snake_case")]
pub enum ExchangeId {
    Other,
    Simulated,
    Mock,
    BinanceFuturesCoin,
    BinanceFuturesUsd,
    BinanceMargin,
    BinanceOptions,
    BinancePortfolioMargin,
    BinanceSpot,
    BinanceUs,
    Bitazza,
    Bitfinex,
    Bitflyer,
    Bitget,
    Bitmart,
    BitmartFuturesUsd,
    Bitmex,
    Bitso,
    Bitstamp,
    Bitvavo,
    Bithumb,
    BybitPerpetualsUsd,
    BybitSpot,
    Cexio,
    Coinbase,
    CoinbaseInternational,
    Cryptocom,
    /// Databento DBEQ.MAX — Composite US equities (all venues)
    DatabentoDbeq,
    /// Databento GLBX.MDP3 — CME Globex futures
    DatabentoGlbx,
    /// Databento OPRA.PILLAR — US options consolidated
    DatabentoOpra,
    /// Databento XNAS.ITCH — Nasdaq equities
    DatabentoXnas,
    /// Databento XNYS.PILLAR — NYSE equities
    DatabentoXnys,
    Deribit,
    GateioFuturesBtc,
    GateioFuturesUsd,
    GateioOptions,
    GateioPerpetualsBtc,
    GateioPerpetualsUsd,
    GateioSpot,
    Gemini,
    Hitbtc,
    #[serde(alias = "huobi")]
    Htx,
    /// Hyperliquid perpetual futures (decentralized, EVM-based)
    HyperliquidPerp,
    /// Hyperliquid spot trading (decentralized, EVM-based)
    HyperliquidSpot,
    /// Alpaca Broker API (execution for equities, options, and crypto)
    AlpacaBroker,
    /// Alpaca crypto market data (wss://stream.data.alpaca.markets/v1beta3/crypto/us)
    AlpacaCrypto,
    /// Alpaca IEX equities market data (free feed)
    AlpacaIex,
    /// Alpaca SIP equities market data (paid consolidated feed)
    AlpacaSip,
    /// Interactive Brokers — equities, futures, options, forex
    Ibkr,
    Kraken,
    Kucoin,
    Liquid,
    /// Massive (formerly Polygon.io) — consolidated market data across all asset classes
    Massive,
    Mexc,
    Okx,
    Poloniex,
    // ---------------------------------------------------------------------------------------
    // NOTE: append new variants HERE, at the end.
    //
    // `ExchangeId` derives `Ord` from declaration order, and `IndexedInstrumentsBuilder::build`
    // sorts exchanges and instruments by it -- `Instrument`'s own `Ord` leads with `exchange`.
    // Inserting a variant mid-enum therefore renumbers `ExchangeIndex` and `InstrumentIndex` for
    // every existing configuration containing an exchange that sorts after the insertion point,
    // and those indices are serialized into engine state, the audit replica and backtest replay
    // streams. Appending renumbers nothing.
    // ---------------------------------------------------------------------------------------
    /// London Strategic Edge FX (spot currency pairs)
    LseFx,
    /// London Strategic Edge crypto (aggregated spot tape; no venue, funding or liquidations)
    LseCrypto,
    /// London Strategic Edge equities & ETFs
    LseEquities,
    /// London Strategic Edge futures (continuous front-month proxies; no contract chain or roll)
    LseFutures,
    /// London Strategic Edge CFDs (index, commodity, interest rates, currency index, volatility)
    LseCfd,
}

impl ExchangeId {
    /// Return the &str representation of this [`ExchangeId`]
    pub fn as_str(&self) -> &'static str {
        match self {
            ExchangeId::Other => "other",
            ExchangeId::Simulated => "simulated",
            ExchangeId::Mock => "mock",
            ExchangeId::AlpacaBroker => "alpaca_broker",
            ExchangeId::AlpacaCrypto => "alpaca_crypto",
            ExchangeId::AlpacaIex => "alpaca_iex",
            ExchangeId::AlpacaSip => "alpaca_sip",
            ExchangeId::BinanceFuturesCoin => "binance_futures_coin",
            ExchangeId::BinanceFuturesUsd => "binance_futures_usd",
            ExchangeId::BinanceMargin => "binance_margin",
            ExchangeId::BinanceOptions => "binance_options",
            ExchangeId::BinancePortfolioMargin => "binance_portfolio_margin",
            ExchangeId::BinanceSpot => "binance_spot",
            ExchangeId::BinanceUs => "binance_us",
            ExchangeId::Bitazza => "bitazza",
            ExchangeId::Bitfinex => "bitfinex",
            ExchangeId::Bitflyer => "bitflyer",
            ExchangeId::Bitget => "bitget",
            ExchangeId::Bitmart => "bitmart",
            ExchangeId::BitmartFuturesUsd => "bitmart_futures_usd",
            ExchangeId::Bitmex => "bitmex",
            ExchangeId::Bitso => "bitso",
            ExchangeId::Bitstamp => "bitstamp",
            ExchangeId::Bitvavo => "bitvavo",
            ExchangeId::Bithumb => "bithumb",
            ExchangeId::BybitPerpetualsUsd => "bybit_perpetuals_usd",
            ExchangeId::BybitSpot => "bybit_spot",
            ExchangeId::Cexio => "cexio",
            ExchangeId::Coinbase => "coinbase",
            ExchangeId::CoinbaseInternational => "coinbase_international",
            ExchangeId::Cryptocom => "cryptocom",
            ExchangeId::DatabentoDbeq => "databento_dbeq",
            ExchangeId::DatabentoGlbx => "databento_glbx",
            ExchangeId::DatabentoOpra => "databento_opra",
            ExchangeId::DatabentoXnas => "databento_xnas",
            ExchangeId::DatabentoXnys => "databento_xnys",
            ExchangeId::Deribit => "deribit",
            ExchangeId::GateioFuturesBtc => "gateio_futures_btc",
            ExchangeId::GateioFuturesUsd => "gateio_futures_usd",
            ExchangeId::GateioOptions => "gateio_options",
            ExchangeId::GateioPerpetualsBtc => "gateio_perpetuals_btc",
            ExchangeId::GateioPerpetualsUsd => "gateio_perpetuals_usd",
            ExchangeId::GateioSpot => "gateio_spot",
            ExchangeId::Gemini => "gemini",
            ExchangeId::Hitbtc => "hitbtc",
            ExchangeId::Htx => "htx", // huobi alias
            ExchangeId::HyperliquidPerp => "hyperliquid_perp",
            ExchangeId::HyperliquidSpot => "hyperliquid_spot",
            ExchangeId::Ibkr => "ibkr",
            ExchangeId::Kraken => "kraken",
            ExchangeId::LseCfd => "lse_cfd",
            ExchangeId::LseCrypto => "lse_crypto",
            ExchangeId::LseEquities => "lse_equities",
            ExchangeId::LseFutures => "lse_futures",
            ExchangeId::LseFx => "lse_fx",
            ExchangeId::Kucoin => "kucoin",
            ExchangeId::Liquid => "liquid",
            ExchangeId::Massive => "massive",
            ExchangeId::Mexc => "mexc",
            ExchangeId::Okx => "okx",
            ExchangeId::Poloniex => "poloniex",
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn test_de_exchange_id() {
        assert_eq!(
            serde_json::from_str::<ExchangeId>(r#""htx""#).unwrap(),
            ExchangeId::Htx
        );
        assert_eq!(
            serde_json::from_str::<ExchangeId>(r#""huobi""#).unwrap(),
            ExchangeId::Htx
        );
    }

    #[test]
    fn test_serde_lse_variants() {
        // `as_str` and the serde representation must agree: `as_str` is what
        // `InstrumentNameInternal` embeds, and the serde string is what users write in configs.
        // A mismatch splits one exchange into two spellings across that boundary.
        for (exchange, expected) in [
            (ExchangeId::LseFx, "lse_fx"),
            (ExchangeId::LseCrypto, "lse_crypto"),
            (ExchangeId::LseEquities, "lse_equities"),
            (ExchangeId::LseFutures, "lse_futures"),
            (ExchangeId::LseCfd, "lse_cfd"),
        ] {
            assert_eq!(exchange.as_str(), expected);
            assert_eq!(
                serde_json::to_string(&exchange).unwrap(),
                format!(r#""{expected}""#)
            );
            assert_eq!(
                serde_json::from_str::<ExchangeId>(&format!(r#""{expected}""#)).unwrap(),
                exchange
            );
        }
    }

    #[test]
    fn test_lse_variants_are_appended_last() {
        // `ExchangeId` derives `Ord` from declaration order, and that ordering determines
        // `ExchangeIndex`/`InstrumentIndex` assignment in `IndexedInstrumentsBuilder::build`.
        // Appending renumbers nothing for existing configurations; inserting mid-enum silently
        // renumbers indices that are serialized into engine state and replay streams.
        for exchange in [
            ExchangeId::LseFx,
            ExchangeId::LseCrypto,
            ExchangeId::LseEquities,
            ExchangeId::LseFutures,
            ExchangeId::LseCfd,
        ] {
            assert!(
                exchange > ExchangeId::Poloniex,
                "{exchange:?} must sort after every pre-existing variant"
            );
        }
    }

    #[test]
    fn test_serde_binance_margin() {
        assert_eq!(ExchangeId::BinanceMargin.as_str(), "binance_margin");
        assert_eq!(
            serde_json::to_string(&ExchangeId::BinanceMargin).unwrap(),
            r#""binance_margin""#
        );
        assert_eq!(
            serde_json::from_str::<ExchangeId>(r#""binance_margin""#).unwrap(),
            ExchangeId::BinanceMargin
        );
    }
}
