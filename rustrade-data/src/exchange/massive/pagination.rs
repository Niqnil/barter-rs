//! Bounded, cycle-safe pagination for Massive REST streams.
//!
//! Every Massive REST endpoint follows the same `next_url` cursor protocol: a
//! page carries an optional `next_url` pointing at the following page, and the
//! stream terminates when a page omits it. Two failure modes make that loop
//! unsafe if followed unconditionally:
//!
//! - a server (or proxy) that never stops returning a `next_url` would page
//!   forever, and
//! - a `next_url` that points back to an already-fetched page would loop over
//!   the same responses indefinitely.
//!
//! [`PaginationGuard`] makes both observable: it caps the page count at
//! [`MAX_PAGES`] and records visited URLs to detect cycles, returning a terminal
//! [`MassiveError`] instead of looping. It intentionally does **not** truncate
//! silently — for a market-data client a short `Vec` is indistinguishable from a
//! genuinely small result set, so incomplete pagination must fail loudly.

use super::error::MassiveError;
use std::collections::HashSet;

/// Maximum number of pages a single paginated fetch will follow.
///
/// This is a runaway backstop, not a business limit: it is set well above the
/// largest legitimate paginated response (e.g. the full reference-ticker
/// universe), so reaching it signals a pathological query or a misbehaving
/// server rather than normal operation.
pub(super) const MAX_PAGES: usize = 10_000;

/// Tracks pagination progress across a single Massive REST stream, enforcing a
/// page-count cap and detecting `next_url` cycles.
///
/// Construct once per stream (via [`Default`]) and call [`observe`] once per page
/// with the URL about to be fetched. The guard borrows nothing and is reset by
/// simply creating a new instance.
///
/// [`observe`]: PaginationGuard::observe
#[derive(Debug, Default)]
pub(super) struct PaginationGuard {
    pages: usize,
    /// URLs already fetched, for cycle detection. Bounded by [`MAX_PAGES`]
    /// entries: [`observe`] checks the page cap before inserting, so growth
    /// cannot exceed the cap before the stream terminates.
    ///
    /// [`observe`]: PaginationGuard::observe
    visited: HashSet<String>,
}

impl PaginationGuard {
    /// Record that `url` is about to be fetched as the next page.
    ///
    /// Returns:
    /// - [`MassiveError::PaginationLimitExceeded`] once more than [`MAX_PAGES`]
    ///   pages have been observed, and
    /// - [`MassiveError::CyclicPagination`] if `url` was already observed.
    ///
    /// Both are terminal: propagate them to end the stream rather than continuing
    /// to page.
    pub(super) fn observe(&mut self, url: &str) -> Result<(), MassiveError> {
        self.pages += 1;
        if self.pages > MAX_PAGES {
            return Err(MassiveError::PaginationLimitExceeded {
                pages: self.pages,
                limit: MAX_PAGES,
            });
        }
        if !self.visited.insert(url.to_owned()) {
            return Err(MassiveError::CyclicPagination {
                url: url.to_owned(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)] // Test code: panics on unexpected values are acceptable
mod tests {
    use super::*;

    #[test]
    fn distinct_urls_under_the_cap_are_accepted() {
        let mut guard = PaginationGuard::default();
        for i in 0..MAX_PAGES {
            assert!(
                guard
                    .observe(&format!("https://api.massive.com/page/{i}"))
                    .is_ok(),
                "page {i} within the cap should be accepted"
            );
        }
    }

    #[test]
    fn exceeding_the_page_cap_is_a_terminal_error() {
        let mut guard = PaginationGuard::default();
        // Exactly MAX_PAGES distinct pages are allowed.
        for i in 0..MAX_PAGES {
            guard
                .observe(&format!("https://api.massive.com/page/{i}"))
                .expect("within cap");
        }
        // The next distinct page trips the backstop.
        let err = guard
            .observe("https://api.massive.com/page/over")
            .expect_err("page MAX_PAGES + 1 must be rejected");
        assert_eq!(
            err,
            MassiveError::PaginationLimitExceeded {
                pages: MAX_PAGES + 1,
                limit: MAX_PAGES,
            }
        );
    }

    #[test]
    fn revisiting_a_url_is_detected_as_a_cycle() {
        let mut guard = PaginationGuard::default();
        let url = "https://api.massive.com/v2/aggs/ticker/X:BTCUSD/range/1/minute/0/1?cursor=abc";
        guard.observe(url).expect("first visit is fine");
        let err = guard.observe(url).expect_err("second visit is a cycle");
        assert_eq!(
            err,
            MassiveError::CyclicPagination {
                url: url.to_owned()
            }
        );
    }

    #[test]
    fn cycle_detection_is_url_exact() {
        // URLs differing by cursor are distinct pages, not a cycle.
        let mut guard = PaginationGuard::default();
        guard
            .observe("https://api.massive.com/x?cursor=a")
            .expect("page 1");
        guard
            .observe("https://api.massive.com/x?cursor=b")
            .expect("page 2 is a different URL");
    }
}
