//! London Strategic Edge market data.
//!
//! A free, no-account market-data provider covering FX, equities, ETFs, crypto, commodities,
//! indices, futures and options, plus reference and macroeconomic series.
//!
//! # ⚠️ Licensing — the data is NOT redistributable
//! This integration's **code** is MIT-licensed like the rest of this repository. **The data it
//! retrieves is not.** London Strategic Edge permits use for your own research, trading and model
//! training — including commercially — but **prohibits redistributing, reselling, or otherwise
//! making the data available to third parties**, in bulk or through any competing feed, download
//! service or interface. Their own client library being MIT-licensed covers *that client only* and
//! confers no rights in the data; the same split applies here.
//!
//! In practice: do not commit retrieved data to a public repository, do not publish it as fixtures
//! or example datasets, and do not re-serve it. Terms: <https://londonstrategicedge.com/terms>
//!
//! # Data characteristics
//! Properties that will silently mislead if assumed away:
//!
//! - **The vault omits volume for FX candles entirely.** FX bars carry OHLC and no volume field,
//!   which is modelled as `None` rather than a zero. A zero would aggregate into a
//!   legitimate-looking total at every derived resolution. (The provider's other host reports a
//!   volume for the same bar; this integration uses the vault, which does not.)
//! - **These are CFD and aggregated-spot series, not exchange instruments.** `XAU/USD` is spot
//!   gold rather than a COMEX contract, `SPX500/USD` is a CFD rather than an index or its future,
//!   and `ES.F` is a continuous front-month proxy with **no contract chain, expiry or roll**.
//!   There is no venue attribution anywhere in the feed.
//! - **Crypto is an aggregated tape**: no funding rates, no liquidations, no venue. It is not a
//!   substitute for a native exchange connector.
//! - **London (`.L`) listings are quoted in PENCE**, and the catalog reports no unit. They are
//!   quoted in GBX, an asset distinct from GBP; see [`market::quote_asset`].
//! - **Dataset slugs are not instrument identities** and do not uniquely identify a series; see
//!   [`market::slug`].

/// Replay historical candles for N instruments as one time-ordered market stream.
pub mod backtest;

/// Errors produced by the London Strategic Edge integration.
pub mod error;

/// Paged historical candles from the vault.
pub mod historical;

/// London Strategic Edge symbology: datasets, underlying assets, quote currencies and slugs.
pub mod market;

/// The shared streaming + export allowance, as the provider reports it.
pub mod quota;

/// Authenticated REST transport for the vault data plane.
pub mod vault;
