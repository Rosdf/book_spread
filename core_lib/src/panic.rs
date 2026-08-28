//! Turning a caught panic back into something a log line can carry.

use std::any::Any;

/// The message out of a caught panic's payload, for a log field.
///
/// `panic!` produces a `&'static str` for a literal and a `String` for a formatted message;
/// anything else is a hand-rolled `panic_any`, which nothing here does.
pub fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("<non-string panic payload>")
}
