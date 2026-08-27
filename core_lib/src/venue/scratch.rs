//! A reused scratch buffer for simd-json, which rewrites its input in place and so needs a
//! mutable, exclusively-owned byte slice to parse from - not every `Bytes` handed to a
//! connection is that on its own.
//!
//! One method, taking ownership: nothing needs the parsed buffer handed back any more. It used
//! to, because a bootstrapping symbol's frames were stashed as raw bytes and re-parsed after
//! the snapshot landed - which is exactly the thing that could not survive an escaped payload,
//! since the first parse had already unescaped it *into this buffer*. Diffs are now parsed once
//! on arrival (see [`crate::venue::pending`]), so the take-and-restore dance went with them.

use bytes::Bytes;

#[derive(Debug, Default)]
pub struct Scratch {
    data: Vec<u8>,
}

impl Scratch {
    /// Gets `f` a mutable slice to parse `raw` in place, without allocating when possible.
    ///
    /// `Bytes::try_into_mut` succeeds when this handle is the only owner of its buffer, in
    /// which case `f` runs directly on that buffer and no copy happens at all. Otherwise the
    /// bytes are copied into `self.data`, which is reused across calls rather than allocated
    /// each time.
    ///
    /// # Which branch actually runs
    ///
    /// Not the one the shape of this function suggests. On the **WebSocket** path the copy is
    /// taken every single time, and cannot be avoided from here: tungstenite builds each
    /// payload with `in_buffer.split_to(len)`, and `BytesMut::split_to` promotes the buffer to
    /// a shared refcount, so `try_into_mut` sees a count of two and returns `Err` for as long
    /// as the reader's own buffer lives - which is always. That would need a different
    /// WebSocket crate to change, and it costs less than it looks like: simd-json copies the
    /// input into its own aligned buffer regardless.
    ///
    /// On the **REST snapshot** path the probe does succeed, and that is what it is now
    /// really for. It is kept on the frame path too because it is a single atomic load, which
    /// is not worth a branch of its own to skip.
    pub fn with_owned_bytes<R>(&mut self, raw: Bytes, f: impl FnOnce(&mut [u8]) -> R) -> R {
        match raw.try_into_mut() {
            Ok(mut bytes) => f(&mut bytes),
            Err(bytes) => {
                self.data.clear();
                self.data.extend_from_slice(bytes.as_ref());
                f(&mut self.data)
            }
        }
    }
}
