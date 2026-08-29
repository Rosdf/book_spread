use std::{
    borrow::{Borrow, Cow},
    cmp::Ordering,
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
    hint::unreachable_unchecked,
    mem::ManuallyDrop,
    num::NonZeroU64,
    ops::Deref,
    ptr::NonNull,
};

use serde::{Deserializer, Serializer};

use crate::shared_string::shared::Shared;

mod shared {
    use std::{
        alloc::{Layout, alloc, dealloc},
        ops::Deref,
        ptr::NonNull,
        sync::atomic::{AtomicUsize, Ordering},
    };

    /// Cache-line-aligned refcount. The `repr(align(64))` puts it on its own
    /// cache line, so `fetch_add` / `fetch_sub` on clone / drop dirty only
    /// this line — readers touching the string on other cores aren't
    /// invalidated. (64 bytes matches x86 and DPDK's `RTE_CACHE_LINE_SIZE`;
    /// Apple Silicon's 128 means short bursts of false sharing there.)
    #[repr(align(64))]
    pub(super) struct Counter(AtomicUsize);

    impl Counter {
        fn new(v: usize) -> Self {
            Self(AtomicUsize::new(v))
        }
    }

    impl Deref for Counter {
        type Target = AtomicUsize;

        #[inline(always)]
        fn deref(&self) -> &AtomicUsize {
            &self.0
        }
    }

    pub(super) struct Shared(NonNull<u8>);
    impl Shared {
        /// Byte offset of the trailing counter for a string of `len` bytes:
        /// `len` rounded up to the counter's alignment.
        const fn counter_offset(len: usize) -> usize {
            const A: usize = align_of::<Counter>();
            (len + A - 1) & !(A - 1)
        }

        pub(super) fn new(data: &str) -> Self {
            let data_len = data.len();

            // SAFETY:
            // layout contains Counter, so it is not zero
            let ptr = unsafe { alloc(Self::layout(data_len)) };
            assert!(!ptr.is_null(), "allocation failure");

            // Data at [0..data_len], counter at the aligned offset that
            // immediately follows it.
            //
            // SAFETY:
            // 1. pointers are valid and do not overlap
            // 2. allocation reserves [0..data_len] for the string
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data_len);
            }

            // SAFETY:
            // 1. allocation reserves [counter_offset..counter_offset+size_of::<Counter>()]
            //    for the counter
            // 2. counter_offset is a multiple of ALIGN = align_of::<Counter>(),
            //    and `ptr` is ALIGN-aligned, so the counter address is aligned
            #[expect(clippy::cast_ptr_alignment, reason = "counter offset is internally aligned")]
            unsafe {
                ptr.add(Self::counter_offset(data_len))
                    .cast::<Counter>()
                    .write(Counter::new(1));
            }

            // SAFETY:
            // we checked for nonnull earlier
            Self(unsafe { NonNull::new_unchecked(ptr) })
        }

        fn layout(len: usize) -> Layout {
            let size = Self::counter_offset(len) + size_of::<Counter>();
            Layout::from_size_align(size, align_of::<Counter>()).unwrap()
        }

        /// # Safety
        /// `len` should be the same as from string that initialized `self`
        pub(super) const unsafe fn get(&self, len: usize) -> &str {
            // Data lives at offset 0.
            //
            // SAFETY:
            // caller guaranties that len is the same as we allocated
            let slice = unsafe { std::slice::from_raw_parts(self.0.as_ptr().cast_const(), len) };

            // SAFETY:
            // we stored valid UTF8 string
            unsafe { str::from_utf8_unchecked(slice) }
        }

        /// # Safety
        /// `len` should be the same as at construction of `self`
        pub(super) unsafe fn drop(self, len: usize) {
            // SAFETY:
            // provided by caller — `len` matches construction
            let counter = unsafe { self.get_counter(len) };

            let old = counter.fetch_sub(1, Ordering::Relaxed);

            if old > 1 {
                return;
            }

            // SAFETY:
            // caller guaranties that size is the same as at allocation
            unsafe {
                dealloc(self.0.as_ptr(), Self::layout(len));
            }
        }

        /// # Safety
        /// `len` must match the string length used at construction.
        unsafe fn get_counter(&self, len: usize) -> &Counter {
            // Counter lives at `counter_offset(len)`, aligned to ALIGN.
            //
            // SAFETY:
            // 1. caller guaranty provides that we are inside allocation
            // 2. ptr + counter_offset(len) is ALIGN-aligned
            unsafe {
                self.0
                    .add(Self::counter_offset(len))
                    .cast::<Counter>()
                    .as_ref()
            }
        }

        /// # Safety
        /// `len` must match the string length used at construction.
        pub(super) unsafe fn clone(&self, len: usize) -> Self {
            // SAFETY:
            // provided by caller — `len` matches construction
            let count = unsafe { self.get_counter(len) };
            count.fetch_add(1, Ordering::Relaxed);

            Self(self.0)
        }

        /// This method does not dec ref counter, so by using pointer to get string
        /// resulting string is static because ref count will not ever go to zero
        pub(super) fn leak(self) -> NonNull<u8> {
            self.0
        }
    }
}

union Inner {
    shared: ManuallyDrop<Shared>,
    static_str: NonNull<u8>,
    inlined: NonZeroU64,
}

const SHARED_PREFIX: u8 = 0b1000_0000;
const STATIC_PREFIX: u8 = 0b0100_0000;
const INLINE_PREFIX: u8 = 0b1100_0000;
const ONLY_PREFIX: u8 = 0b1100_0000;
const PREFIX_REMOVED: u8 = !ONLY_PREFIX;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
enum Prefix {
    Shared = SHARED_PREFIX,
    Static = STATIC_PREFIX,
    Inline = INLINE_PREFIX,
}

#[repr(C)]
pub struct SharedString {
    header: [u8; 8],
    inner: Inner,
}

const _: () = assert!(
    size_of::<SharedString>() == 16,
    "SharedString should be 16 bytes"
);

impl Default for SharedString {
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! handle_small_or {
    ($st:ident, $f:tt) => {{
        if $st.len() <= 15 {
            let raw_data_buf = {
                let mut buf = [1u8; 16];
                let mut i = 0;
                let st_bytes = $st.as_bytes();

                while i < st_bytes.len() {
                    buf[i + 1] = st_bytes[i];
                    i += 1;
                }

                buf
            };

            let mut len_buf = {
                let mut buf = [0u8; 8];
                let mut i = 0;

                while i < 8 {
                    buf[i] = raw_data_buf[i];
                    i += 1;
                }

                buf
            };

            let data_buf = {
                let mut buf = [0u8; 8];
                let mut i = 0;

                while i < 8 {
                    buf[i] = raw_data_buf[i + 8];
                    i += 1;
                }

                buf
            };

            let data = u64::from_le_bytes(data_buf);

            match NonZeroU64::new(data) {
                Some(val) => {
                    #[expect(clippy::cast_possible_truncation, reason = "len is small enough")]
                    let len = $st.len() as u8 | INLINE_PREFIX;
                    len_buf[0] = len;

                    return Self {
                        header: len_buf,
                        inner: Inner { inlined: val },
                    };
                }
                None => Self::$f($st),
            }
        } else {
            Self::$f($st)
        }
    }};
}

impl SharedString {
    pub const fn new() -> Self {
        const DATA: NonZeroU64 = NonZeroU64::new(1).unwrap();
        const HEADER: [u8; 8] = [INLINE_PREFIX, 0, 0, 0, 0, 0, 0, 0];

        Self {
            header: HEADER,
            inner: Inner { inlined: DATA },
        }
    }

    fn from_str(st: &str) -> Self {
        Self::assert_str_size(st);

        handle_small_or!(st, make_shared)
    }

    #[cfg(test)]
    fn is_static(&self) -> bool {
        let prefix = self.get_prefix();
        prefix == Prefix::Static
    }

    const fn assert_str_size(st: &str) {
        const MAX_LEN: usize = usize::from_be_bytes([
            ONLY_PREFIX,
            u8::MAX,
            u8::MAX,
            u8::MAX,
            u8::MAX,
            u8::MAX,
            u8::MAX,
            u8::MAX,
        ]);

        assert!(st.len() < MAX_LEN, "shared string should be smaller");
    }

    fn make_shared(st: &str) -> Self {
        let shared = Shared::new(st);
        let mut prefix_buf = st.len().to_be_bytes();
        prefix_buf[0] |= SHARED_PREFIX;

        Self {
            header: prefix_buf,
            inner: Inner {
                shared: ManuallyDrop::new(shared),
            },
        }
    }

    pub const fn from_static_str(st: &'static str) -> Self {
        Self::assert_str_size(st);
        handle_small_or!(st, make_static)
    }

    const fn make_static(st: &'static str) -> Self {
        let mut prefix_buf = st.len().to_be_bytes();
        prefix_buf[0] |= STATIC_PREFIX;

        Self {
            header: prefix_buf,
            inner: Inner {
                static_str: NonNull::from_ref(st).cast(),
            },
        }
    }

    pub fn leak(&mut self) {
        let prefix = self.get_prefix();
        match prefix {
            Prefix::Shared => {
                // SAFETY:
                // we're never accessing this field after this line
                let leaked = unsafe { ManuallyDrop::take(&mut self.inner.shared) }.leak();
                self.inner.static_str = leaked;
                self.header[0] = (self.header[0] & PREFIX_REMOVED) | STATIC_PREFIX;
            }
            Prefix::Static | Prefix::Inline => {}
        }
    }

    pub fn leaked(mut this: Self) -> Self {
        this.leak();
        this
    }

    #[inline(always)]
    pub const fn len(&self) -> usize {
        let mut prefix_buf = self.header;
        prefix_buf[0] &= PREFIX_REMOVED;

        let prefix = self.get_prefix();
        if matches!(prefix, Prefix::Inline) {
            return prefix_buf[0] as usize;
        }

        usize::from_be_bytes(prefix_buf)
    }

    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    const fn get_prefix(&self) -> Prefix {
        match self.header[0] & ONLY_PREFIX {
            SHARED_PREFIX => Prefix::Shared,
            STATIC_PREFIX => Prefix::Static,
            INLINE_PREFIX => Prefix::Inline,
            // SAFETY:
            // these are the only possible values
            _ => unsafe { unreachable_unchecked() },
        }
    }

    pub const fn as_str(&self) -> &str {
        let len = self.len();

        let prefix = self.get_prefix();
        match prefix {
            Prefix::Shared => {
                // SAFETY:
                // 1. ManuallyDrop is transparent, so this cast is ok
                // 2. SHARED_PREFIX indicates that we are storing shared union variant
                // we are doing this in this way, because dereference is not allowed in const
                let shared = unsafe { &*std::ptr::from_ref(&self.inner.shared).cast::<Shared>() };

                // SAFETY:
                // len is passed from the same string
                unsafe { shared.get(len) }
            }
            Prefix::Inline => {
                // SAFETY:
                // header is 8 bytes, so offset by 1 is ok
                let data_ptr = unsafe { std::ptr::from_ref(self).cast::<u8>().add(1) };
                // SAFETY:
                // Self is repr(C), so data is stored after header, so len is valid
                let buf = unsafe { std::slice::from_raw_parts(data_ptr, len) };
                // SAFETY:
                // we got data from valid utf8 string
                unsafe { std::str::from_utf8_unchecked(buf) }
            }
            Prefix::Static => {
                // SAFETY:
                // STATIC_PREFIX indicates that we are storing static_str variant
                let data_ptr = unsafe { self.inner.static_str }.as_ptr().cast_const();
                // SAFETY:
                // we stored string of length = len
                let buf = unsafe { std::slice::from_raw_parts(data_ptr, len) };
                // SAFETY:
                // we stored valid utf8 string
                unsafe { std::str::from_utf8_unchecked(buf) }
            }
        }
    }
}

impl Drop for SharedString {
    fn drop(&mut self) {
        let len = self.len();
        let prefix = self.get_prefix();

        if prefix == Prefix::Shared {
            // SAFETY:
            // 1. SHARED_PREFIX indicates that we have shared variant
            // 2. we are passing the same str len as from constructor
            // 3. this is destructor, data will not be accessed anymore
            unsafe {
                ManuallyDrop::take(&mut self.inner.shared).drop(len);
            }
        }
    }
}

#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "implementation detail is not Send, whole structure is send"
)]
// SAFETY:
// data is inlined or static or atomically ref counted
unsafe impl Send for SharedString {}
// SAFETY:
// data is inlined or static or atomically ref counted
unsafe impl Sync for SharedString {}

impl From<&str> for SharedString {
    fn from(value: &str) -> Self {
        Self::from_str(value)
    }
}

impl From<Cow<'_, str>> for SharedString {
    fn from(value: Cow<str>) -> Self {
        Self::from_str(value.as_ref())
    }
}

impl From<String> for SharedString {
    fn from(value: String) -> Self {
        Self::from_str(value.as_str())
    }
}

impl Debug for SharedString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.as_str(), f)
    }
}

impl Display for SharedString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.as_str(), f)
    }
}

impl Clone for SharedString {
    fn clone(&self) -> Self {
        let prefix = self.get_prefix();

        match prefix {
            Prefix::Shared => {
                let len = self.len();
                // SAFETY:
                // 1. SHARED_PREFIX means we have shared variant
                // 2. len matches the string length used at construction
                let shared = unsafe { (*self.inner.shared).clone(len) };

                Self {
                    header: self.header,
                    inner: Inner {
                        shared: ManuallyDrop::new(shared),
                    },
                }
            }
            Prefix::Inline => Self {
                header: self.header,
                inner: Inner {
                    // SAFETY:
                    // INLINE_PREFIX means we have inlined variant
                    inlined: unsafe { self.inner.inlined },
                },
            },
            Prefix::Static => Self {
                header: self.header,
                inner: Inner {
                    // SAFETY:
                    // STATIC_PREFIX means we have static_str variant
                    static_str: unsafe { self.inner.static_str },
                },
            },
        }
    }
}

impl Deref for SharedString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for SharedString {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for SharedString {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq for SharedString {
    fn eq(&self, other: &Self) -> bool {
        self.as_str().eq(other.as_str())
    }
}

impl PartialEq<str> for SharedString {
    fn eq(&self, other: &str) -> bool {
        self.as_str().eq(other)
    }
}

impl PartialEq<&str> for SharedString {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq(*other)
    }
}

impl Eq for SharedString {}

impl PartialOrd for SharedString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialOrd<str> for SharedString {
    fn partial_cmp(&self, other: &str) -> Option<Ordering> {
        self.as_str().partial_cmp(other)
    }
}

impl Ord for SharedString {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Hash for SharedString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl serde::Serialize for SharedString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_str().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for SharedString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = Cow::<str>::deserialize(deserializer)?;
        Ok(Self::from_str(s.as_ref()))
    }
}

#[cfg(test)]
mod test {
    use crate::shared_string::SharedString;

    #[test]
    fn as_str() {
        let mut buf = String::new();
        for _ in 0..30 {
            let st = SharedString::from_str(buf.as_str());
            assert_eq!(st.as_str(), buf.as_str());
            assert_eq!(st.len(), buf.len());
            assert_eq!(st.is_empty(), buf.is_empty());
            buf.push('b');
        }
    }

    #[test]
    fn clone() {
        let mut buf = String::new();
        for _ in 0..30 {
            let st1 = SharedString::from_str(buf.as_str());
            assert_eq!(st1.len(), buf.len());
            assert_eq!(st1.is_empty(), buf.is_empty());

            let st2 = st1.clone();
            drop(st1);

            assert_eq!(st2.as_str(), buf.as_str());
            assert_eq!(st2.len(), buf.len());
            assert_eq!(st2.is_empty(), buf.is_empty());

            buf.push('b');
        }
    }

    #[test]
    fn is_static() {
        let stat = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let st = SharedString::from_static_str(stat);
        assert!(st.is_static());
        assert_eq!(stat, st.as_str());
    }
}
