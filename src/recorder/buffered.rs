//! Buffered recorder: wraps an inner recorder with asynchronous, batched
//! writes on a background tokio task.
//!
//! Events enqueue on a bounded channel and are flushed to the inner recorder
//! when the pending batch reaches `flush_size` or `flush_interval` elapses,
//! whichever comes first. `record` only enqueues, so it stays effectively
//! non-blocking (it awaits only under [`OverflowPolicy::Block`] when full).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;

use super::error::{DrainTimeout, RecordError};
use super::{BatchRecorder, Recorder, RecorderOptions};
use crate::event::Event;

/// Controls [`BufferedRecorder::record`] when the buffer is full.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Drop the incoming event and increment the dropped counter (default).
    #[default]
    DropNewest,
    /// Wait for space (awaits until the buffer drains or the recorder closes).
    Block,
    /// Return [`RecordError::BufferFull`].
    Error,
}

/// Runtime counters for observability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferedStats {
    pub dropped: u64,
    pub flushed: u64,
    pub flush_errs: u64,
    pub pending: usize,
    pub buffer_size: usize,
}

#[derive(Default)]
struct Counters {
    dropped: AtomicU64,
    flushed: AtomicU64,
    flush_errs: AtomicU64,
}

type FlushAck = oneshot::Sender<Result<(), RecordError>>;

/// Wraps an inner recorder with asynchronous batched writes. Spawns a
/// background tokio task that runs until [`BufferedRecorder::close`].
pub struct BufferedRecorder {
    tx: mpsc::Sender<Event>,
    flush_tx: mpsc::Sender<FlushAck>,
    shutdown: Arc<Notify>,
    handle: Mutex<Option<JoinHandle<()>>>,
    counters: Arc<Counters>,
    overflow: OverflowPolicy,
    buffer_size: usize,
    drain_timeout: Duration,
    closed: AtomicBool,
}

impl BufferedRecorder {
    /// Wrap `inner` with asynchronous batched writes. Must be called from
    /// within a tokio runtime (it spawns a background task).
    pub fn new<R: BatchRecorder>(inner: R, opts: &RecorderOptions) -> Self {
        let (tx, rx) = mpsc::channel(opts.buffer_size.max(1));
        let (flush_tx, flush_rx) = mpsc::channel(16);
        let shutdown = Arc::new(Notify::new());
        let counters = Arc::new(Counters::default());

        let handle = tokio::spawn(run(
            rx,
            flush_rx,
            shutdown.clone(),
            inner,
            FlushCfg {
                flush_size: opts.flush_size.max(1),
                flush_interval: opts.flush_interval,
                flush_timeout: opts.flush_timeout,
            },
            counters.clone(),
        ));

        BufferedRecorder {
            tx,
            flush_tx,
            shutdown,
            handle: Mutex::new(Some(handle)),
            counters,
            overflow: opts.overflow,
            buffer_size: opts.buffer_size.max(1),
            drain_timeout: opts.drain_timeout,
            closed: AtomicBool::new(false),
        }
    }

    /// Enqueue an event for asynchronous delivery. Empty-`action` events are
    /// no-ops. Behavior when the buffer is full follows the configured
    /// [`OverflowPolicy`]. Calls after [`BufferedRecorder::close`] are dropped.
    pub async fn record(&self, event: Event) -> Result<(), RecordError> {
        if event.action.is_empty() || self.closed.load(Relaxed) {
            return Ok(());
        }
        match self.overflow {
            OverflowPolicy::Block => {
                let _ = self.tx.send(event).await; // Err only if the task is gone
                Ok(())
            }
            OverflowPolicy::Error => match self.tx.try_send(event) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => Err(RecordError::BufferFull),
                Err(mpsc::error::TrySendError::Closed(_)) => Ok(()),
            },
            OverflowPolicy::DropNewest => match self.tx.try_send(event) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(ev)) => {
                    let n = self.counters.dropped.fetch_add(1, Relaxed) + 1;
                    if n == 1 || n % 1000 == 0 {
                        log::warn!(
                            "everscribe: recorder buffer full, event dropped (action={} dropped_total={})",
                            ev.action,
                            n
                        );
                    }
                    Ok(())
                }
                Err(mpsc::error::TrySendError::Closed(_)) => Ok(()),
            },
        }
    }

    /// Force an immediate flush of everything buffered at call time and wait
    /// for it to be persisted. A no-op after [`BufferedRecorder::close`].
    pub async fn flush(&self) -> Result<(), RecordError> {
        if self.closed.load(Relaxed) {
            return Ok(());
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.flush_tx.send(ack_tx).await.is_err() {
            return Ok(());
        }
        ack_rx.await.unwrap_or(Ok(()))
    }

    /// Drain pending events and stop the background task. Idempotent. Returns
    /// [`DrainTimeout`] if the drain exceeds `drain_timeout`.
    pub async fn close(&self) -> Result<(), DrainTimeout> {
        if self.closed.swap(true, Relaxed) {
            return Ok(());
        }
        self.shutdown.notify_one();
        let handle = self.handle.lock().unwrap().take();
        let Some(handle) = handle else { return Ok(()) };
        match tokio::time::timeout(self.drain_timeout, handle).await {
            Ok(_) => Ok(()),
            Err(_) => Err(DrainTimeout),
        }
    }

    /// Cumulative counters for observability.
    pub fn stats(&self) -> BufferedStats {
        BufferedStats {
            dropped: self.counters.dropped.load(Relaxed),
            flushed: self.counters.flushed.load(Relaxed),
            flush_errs: self.counters.flush_errs.load(Relaxed),
            pending: self.tx.max_capacity() - self.tx.capacity(),
            buffer_size: self.buffer_size,
        }
    }
}

impl Recorder for BufferedRecorder {
    async fn record(&self, event: Event) -> Result<(), RecordError> {
        BufferedRecorder::record(self, event).await
    }
}

struct FlushCfg {
    flush_size: usize,
    flush_interval: Duration,
    flush_timeout: Duration,
}

async fn run<R: BatchRecorder>(
    mut rx: mpsc::Receiver<Event>,
    mut flush_rx: mpsc::Receiver<FlushAck>,
    shutdown: Arc<Notify>,
    inner: R,
    cfg: FlushCfg,
    counters: Arc<Counters>,
) {
    let mut batch: Vec<Event> = Vec::with_capacity(cfg.flush_size);
    let mut ticker = tokio::time::interval(cfg.flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(e) => {
                    batch.push(e);
                    if batch.len() >= cfg.flush_size {
                        let _ = flush_batch(&inner, &mut batch, cfg.flush_timeout, &counters).await;
                    }
                }
                None => {
                    // All senders dropped (recorder dropped without close).
                    let _ = flush_batch(&inner, &mut batch, cfg.flush_timeout, &counters).await;
                    break;
                }
            },
            _ = ticker.tick() => {
                let _ = flush_batch(&inner, &mut batch, cfg.flush_timeout, &counters).await;
            }
            Some(ack) = flush_rx.recv() => {
                while let Ok(e) = rx.try_recv() {
                    batch.push(e);
                }
                let res = flush_batch(&inner, &mut batch, cfg.flush_timeout, &counters).await;
                let _ = ack.send(res);
            }
            _ = shutdown.notified() => {
                while let Ok(e) = rx.try_recv() {
                    batch.push(e);
                }
                let _ = flush_batch(&inner, &mut batch, cfg.flush_timeout, &counters).await;
                break;
            }
        }
    }
}

async fn flush_batch<R: BatchRecorder>(
    inner: &R,
    batch: &mut Vec<Event>,
    flush_timeout: Duration,
    counters: &Counters,
) -> Result<(), RecordError> {
    if batch.is_empty() {
        return Ok(());
    }
    let n = batch.len();
    let events = std::mem::take(batch);
    let res = match tokio::time::timeout(flush_timeout, inner.record_batch(events)).await {
        Ok(r) => r,
        Err(_) => Err(RecordError::Timeout),
    };
    match &res {
        Ok(()) => {
            counters.flushed.fetch_add(n as u64, Relaxed);
        }
        Err(e) => {
            counters.flush_errs.fetch_add(1, Relaxed);
            log::error!("everscribe: recorder flush failed (error={e} batch_size={n})");
        }
    }
    res
}
