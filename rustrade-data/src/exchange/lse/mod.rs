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
//! - **FX candles are BID candles — not mid, and not last.** Reconciling a day of `EUR/USD`
//!   one-minute bars against the tick tape for the same day, open/high/low/close matched the
//!   **bid** series on 1421 of 1421 minutes and matched the mid or the ask on none. So a backtest
//!   that fills at the candle close is filling at the bid — systematically favourable by a full
//!   spread on every buy, in the provider's deepest dataset. This cannot be corrected here without
//!   inventing a spread. Equity candles, by contrast, track the trade tape; the asymmetry is real
//!   and per-dataset.
//! - **Candle volume is not a dependable figure.** For FX the vault omits the field entirely, which
//!   is modelled as `None` rather than a zero — a zero would aggregate into a legitimate-looking
//!   total at every derived resolution. (The provider's other host reports a volume for the same
//!   bar; this integration uses the vault, which does not.) Where the field *is* published it is
//!   still unreliable: a majority of sampled one-minute equity bars reported `0` in minutes the
//!   tick tape shows real trades, and one daily series carried a contiguous band roughly 2,000×
//!   too large. A literal `0` is passed through as `Some(0)`; rewriting it to `None` would be this
//!   library inventing a fact, and `None` is reserved for a column the provider does not publish.
//!   Validate before trading on it.
//! - **Non-trading days are emitted as FLAT bars, not omitted — daily series are not sparse.**
//!   Every sampled Saturday and the US Independence Day observance returned a bar with
//!   `open == high == low == close`; Sundays are absent. A backtest therefore sees a tradeable
//!   price on a closed market, and the only signal is the flat OHLC. Intraday bars *are* sparse
//!   (no-trade minutes are absent rather than zero-filled), so the two resolutions differ.
//! - **These are CFD and aggregated-spot series, not exchange instruments.** `XAU/USD` is spot
//!   gold rather than a COMEX contract, `SPX500/USD` is a CFD rather than an index or its future,
//!   and `ES.F` is a continuous front-month proxy with **no contract chain, expiry or roll**.
//!   There is no venue attribution anywhere in the feed.
//! - **Crypto is an aggregated tape**: no funding rates, no liquidations, no venue. It is not a
//!   substitute for a native exchange connector.
//! - **London (`.L`) listings are quoted in PENCE**, and the catalog reports no unit. They are
//!   quoted in GBX, an asset distinct from GBP; see
//!   [`market::quote_asset`](crate::exchange::lse::market::quote_asset).
//! - **Dataset slugs are not instrument identities** and do not uniquely identify a series; see
//!   [`market::slug`](crate::exchange::lse::market::slug).

// A module carries an outer `///` here only when its own file has no `//!` documentation.
// Supplying both makes rustdoc resolve the file's inner links in THIS module's scope rather than
// the child's, so every `[`SomeType`]` written inside the child silently renders as dead text.

/// Replay historical candles for N instruments as one time-ordered market stream.
pub mod backtest;

/// Errors produced by the London Strategic Edge integration.
pub mod error;

pub mod export;

pub mod historical;

/// London Strategic Edge symbology: datasets, underlying assets, quote currencies and slugs.
pub mod market;

#[cfg(feature = "lse-parquet")]
pub mod parquet;

/// The shared streaming + export allowance, as the provider reports it.
pub mod quota;

pub mod vault;
