use std::error::Error;

use bellows::{
    RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError, TrySendError,
};

#[test]
fn debug_does_not_need_the_payload_to_be_debug() {
    struct Opaque;
    assert_eq!(format!("{:?}", SendError(Opaque)), "SendError(..)");
    assert_eq!(format!("{:?}", TrySendError::Full(Opaque)), "Full(..)");
    assert_eq!(
        format!("{:?}", TrySendError::Disconnected(Opaque)),
        "Disconnected(..)"
    );
    assert_eq!(
        format!("{:?}", SendTimeoutError::Timeout(Opaque)),
        "Timeout(..)"
    );
}

#[test]
fn display_messages() {
    assert_eq!(
        SendError(1).to_string(),
        "sending on a channel with no receivers"
    );
    assert_eq!(
        TrySendError::Full(1).to_string(),
        "sending on a full channel"
    );
    assert_eq!(
        TryRecvError::Empty.to_string(),
        "receiving on an empty channel"
    );
    assert_eq!(
        RecvError.to_string(),
        "receiving on an empty channel with no senders"
    );
    assert_eq!(
        RecvTimeoutError::Timeout.to_string(),
        "timed out waiting for a message"
    );
    assert_eq!(
        SendTimeoutError::Timeout(1).to_string(),
        "timed out waiting for space"
    );
}

#[test]
fn errors_work_as_trait_objects() {
    let boxed: Box<dyn Error> = Box::new(RecvError);
    assert!(boxed.to_string().contains("no senders"));
    let boxed: Box<dyn Error> = Box::new(TrySendError::Disconnected(5));
    assert!(boxed.to_string().contains("no receivers"));
}

#[test]
fn try_send_error_helpers() {
    let full = TrySendError::Full(7);
    assert!(full.is_full());
    assert!(!full.is_disconnected());
    assert_eq!(full.into_inner(), 7);
    let gone = TrySendError::Disconnected(8);
    assert!(gone.is_disconnected());
    assert_eq!(gone.into_inner(), 8);
}

#[test]
fn send_timeout_error_helpers() {
    let timeout = SendTimeoutError::Timeout("x");
    assert!(timeout.is_timeout());
    assert_eq!(timeout.into_inner(), "x");
    assert!(!SendTimeoutError::Disconnected("y").is_timeout());
}

