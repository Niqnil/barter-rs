use futures::Stream;
use tokio_stream::wrappers::ReceiverStream;

/// Channel depth [`stream_blocking_iter`] is documented for, and a sensible default for a decoder
/// feeding a backtest.
///
/// Large enough that the producer is not woken per item, small enough that the buffered events are
/// an accounting rounding error next to the dataset. At ~160 bytes per market event this is ~160 KiB
/// in flight.
pub const DEFAULT_BLOCKING_CHANNEL_CAPACITY: usize = 1024;

/// Bridge a **blocking**, fallible iterator into a bounded [`Stream`], decoding on a blocking thread.
///
/// Historical data usually arrives as something that blocks: a Parquet artifact, a compressed
/// archive, a CSV on a slow disk. Two things then go wrong if such an iterator is simply wrapped in
/// [`futures::stream::iter`]:
///
/// 1. **It stalls a runtime worker for the whole decode.** Nothing else scheduled on that worker —
///    the engine, another instrument's decode, a heartbeat — makes progress until the file ends.
/// 2. **It never yields `Pending`, so nothing downstream can pace it.** A consumer that forwards
///    into an unbounded channel (the engine feed is one) therefore takes the *entire* artifact into
///    memory before processing the first event, which is the opposite of the streaming the caller
///    asked for. Laziness alone does not bound memory; the producer has to be made to wait.
///
/// This helper fixes both: `init` runs on a [`tokio::task::spawn_blocking`] thread — so opening the
/// source is blocking too, as it usually is — and each item is handed over a channel of `capacity`
/// items. When the channel is full the blocking thread parks, which is the back-pressure: the decoder
/// runs at most `capacity` items ahead of whatever polls this stream, whatever the dataset's size,
/// and decoding overlaps consumption instead of preceding it.
///
/// **That bound is local to this hand-off.** It constrains nothing further downstream: a caller that
/// forwards this stream into another *unbounded* queue — a backtest engine feed is exactly that shape
/// — reintroduces unbounded growth there whenever the far end is the slower side. What this helper
/// guarantees is "the decoder will not get more than `capacity` items ahead of its own consumer", not
/// an end-to-end memory bound for whatever pipeline it is embedded in.
///
/// # Failure model
/// The item type is `Result`, because a source that reads incrementally can fail *after* it opened
/// successfully. An `init` that fails yields exactly one `Err` and ends the stream, so a caller
/// handles open and mid-stream failures on one path.
///
/// # Cancellation
/// Dropping the returned stream drops the receiver, the next hand-off fails, and the blocking task
/// returns at its next item boundary. A blocking task cannot be pre-empted, so an iterator that
/// blocks for a long time between items keeps its thread until it comes back — bounded by the
/// iterator's own step, not by the dataset.
///
/// # Panics
/// Panics if `capacity` is zero (a zero-capacity channel cannot deliver), and requires a Tokio
/// runtime with the `rt` feature, like any [`tokio::task::spawn_blocking`] caller.
///
/// # Examples
/// ```no_run
/// # use rustrade_data::streams::blocking::{stream_blocking_iter, DEFAULT_BLOCKING_CHANNEL_CAPACITY};
/// # fn decode() -> Result<std::vec::IntoIter<Result<u64, std::io::Error>>, std::io::Error> {
/// #     unimplemented!()
/// # }
/// let _stream = stream_blocking_iter(DEFAULT_BLOCKING_CHANNEL_CAPACITY, decode);
/// ```
pub fn stream_blocking_iter<Init, Iter, Item, Error>(
    capacity: usize,
    init: Init,
) -> impl Stream<Item = Result<Item, Error>> + Send + 'static
where
    Init: FnOnce() -> Result<Iter, Error> + Send + 'static,
    // No `Send`/`'static` on `Iter`: it is built and drained entirely inside the `spawn_blocking`
    // closure, never captured by it and never returned, so it does not cross a thread boundary. A
    // decoder with `!Send` internals is therefore usable here.
    Iter: Iterator<Item = Result<Item, Error>>,
    Item: Send + 'static,
    Error: Send + 'static,
{
    assert!(
        capacity > 0,
        "stream_blocking_iter requires a non-zero channel capacity"
    );

    let (tx, rx) = tokio::sync::mpsc::channel(capacity);

    // Detached deliberately: the handle carries no result worth awaiting, and the receiver being
    // dropped is what ends the task (see `# Cancellation`). `spawn_blocking` tasks are not
    // abortable, so retaining the handle would buy nothing.
    tokio::task::spawn_blocking(move || {
        let iter = match init() {
            Ok(iter) => iter,
            // The consumer is gone; there is nobody to report the failure to.
            Err(error) => {
                let _ = tx.blocking_send(Err(error));
                return;
            }
        };

        for item in iter {
            // Parks this thread while the channel is full -- the back-pressure the bound exists for.
            // An `Err` means the receiver was dropped, so the remaining items have no destination.
            if tx.blocking_send(item).is_err() {
                break;
            }
        }
    });

    ReceiverStream::new(rx)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::{
        io::{Error, ErrorKind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    fn ok_iter(count: usize) -> Result<std::vec::IntoIter<Result<usize, Error>>, Error> {
        Ok((0..count).map(Ok).collect::<Vec<_>>().into_iter())
    }

    #[tokio::test]
    async fn forwards_every_item_in_order() {
        let stream = stream_blocking_iter(4, || ok_iter(50));

        let items = stream.map(|item| item.unwrap()).collect::<Vec<_>>().await;

        assert_eq!(items, (0..50).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn an_init_failure_becomes_a_single_err_item() {
        let stream = stream_blocking_iter(4, || {
            Err::<std::vec::IntoIter<Result<usize, Error>>, _>(Error::new(
                ErrorKind::NotFound,
                "no such artifact",
            ))
        });

        let items = stream.collect::<Vec<_>>().await;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].as_ref().unwrap_err().kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn a_mid_stream_failure_is_surfaced_not_skipped() {
        let stream = stream_blocking_iter(4, || {
            Ok(vec![
                Ok(1_usize),
                Err(Error::new(ErrorKind::InvalidData, "bad row")),
                Ok(3),
            ]
            .into_iter())
        });

        let items = stream.collect::<Vec<_>>().await;
        assert_eq!(items.len(), 3);
        assert!(items[1].is_err());
    }

    /// The property the type exists for: the producer must not run ahead of the consumer without
    /// bound. With a capacity of 2 and nothing drained, no more than a handful of items can have
    /// been produced -- emphatically not all 10,000.
    #[tokio::test]
    async fn the_producer_is_bounded_by_the_channel_capacity() {
        const CAPACITY: usize = 2;
        let produced = Arc::new(AtomicUsize::new(0));

        let counter = Arc::clone(&produced);
        let mut stream = Box::pin(stream_blocking_iter(CAPACITY, move || {
            Ok((0..10_000).map(move |index| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<usize, Error>(index)
            }))
        }));

        // Take one item, then let the blocking thread run as far as it can.
        assert_eq!(stream.next().await.unwrap().unwrap(), 0);
        tokio::task::yield_now().await;

        // One consumed, `CAPACITY` buffered, and at most one more parked mid-hand-off.
        let produced = produced.load(Ordering::SeqCst);
        assert!(
            produced <= CAPACITY + 2,
            "producer ran {produced} items ahead of a {CAPACITY}-item channel"
        );
    }

    #[tokio::test]
    async fn dropping_the_stream_stops_the_producer() {
        let produced = Arc::new(AtomicUsize::new(0));

        let counter = Arc::clone(&produced);
        let mut stream = Box::pin(stream_blocking_iter(2, move || {
            Ok((0..10_000).map(move |index| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<usize, Error>(index)
            }))
        }));

        assert!(stream.next().await.is_some());
        drop(stream);

        // The producer stops at its next hand-off, so the count stops climbing. Sampled twice
        // rather than compared against a bound, since the exact stopping point is a race.
        tokio::task::yield_now().await;
        let first = produced.load(Ordering::SeqCst);
        tokio::task::yield_now().await;
        assert_eq!(produced.load(Ordering::SeqCst), first);
    }
}
