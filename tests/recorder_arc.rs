//! An Arc-wrapped recorder must satisfy Recorder, so one recorder can
//! be both mounted on an adapter and closed on shutdown.
//!
//! Without the Arc forward this file does not compile, which is the
//! point: the adapters take the recorder by value into a private Arc
//! with no accessor, so a mounted recorder was unreachable and
//! close() could never be called. Buffered events were then dropped
//! at shutdown, losing exactly the events immediately before a deploy
//! or a crash.

use std::sync::{Arc, Mutex};

use everscribe::event::Event;
use everscribe::recorder::{RecordError, Recorder};

#[derive(Default)]
struct CountingRecorder {
    seen: Mutex<Vec<String>>,
}

impl Recorder for CountingRecorder {
    fn record(
        &self,
        event: Event,
    ) -> impl std::future::Future<Output = Result<(), RecordError>> + Send {
        self.seen.lock().unwrap().push(event.action.clone());
        async { Ok(()) }
    }
}

/// Takes anything the adapters would take, by value, exactly as
/// EverscribeLayer::new does.
async fn mount_and_record<R>(recorder: R, action: &str) -> Result<(), RecordError>
where
    R: Recorder + Send + Sync + 'static,
{
    let ev = Event {
        action: action.to_string(),
        ..Default::default()
    };
    recorder.record(ev).await
}

#[tokio::test]
async fn arc_recorder_can_be_mounted_and_still_closed() {
    let rec = Arc::new(CountingRecorder::default());

    // The adapter consumes its own handle, as the real one does.
    mount_and_record(Arc::clone(&rec), "user.login")
        .await
        .unwrap();

    // The caller still holds one, which is the whole point: this is
    // where close() would go on a real BufferedRecorder.
    assert_eq!(
        rec.seen.lock().unwrap().as_slice(),
        &["user.login".to_string()]
    );
}

#[tokio::test]
async fn arc_forward_records_through_to_the_inner_recorder() {
    let rec = Arc::new(CountingRecorder::default());
    let handle: Arc<CountingRecorder> = Arc::clone(&rec);

    let ev = Event {
        action: "billing.invoice.refunded".to_string(),
        ..Default::default()
    };
    handle.record(ev).await.unwrap();

    assert_eq!(
        rec.seen.lock().unwrap().as_slice(),
        &["billing.invoice.refunded".to_string()]
    );
}
