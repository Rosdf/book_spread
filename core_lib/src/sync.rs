//! Concurrency primitives, switched between the real ones and [`loom`]'s model.
//!
//! Building with `--cfg loom` swaps every atomic and cell used by
//! [`crate::shared_buffer`] and [`crate::connector::book_publisher`] for loom's
//! instrumented equivalents, so the loom test targets can enumerate thread
//! interleavings. Ordinary builds re-export the `std` types and compile to
//! exactly what they would without this module.
//!
//! The [`UnsafeCell`] wrapper exists because loom needs to see every access to
//! interior-mutable data, so its cell only hands out pointers inside a closure.
//! The `std` version below mirrors that shape, which keeps a single spelling of
//! each access in the call sites rather than two cfg-gated ones.

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};

/// Interior-mutable storage whose accesses loom can observe.
///
/// `with` and `with_mut` both take `&self`; upholding the aliasing rules for the
/// pointer they yield is the caller's job, exactly as with [`std::cell::UnsafeCell`].
pub(crate) struct UnsafeCell<T>(Inner<T>);

#[cfg(loom)]
type Inner<T> = loom::cell::UnsafeCell<T>;
#[cfg(not(loom))]
type Inner<T> = std::cell::UnsafeCell<T>;

impl<T> UnsafeCell<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(Inner::new(value))
    }

    /// Runs `f` on a shared pointer to the contents.
    pub(crate) fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(*const T) -> R,
    {
        #[cfg(loom)]
        {
            self.0.with(f)
        }
        #[cfg(not(loom))]
        {
            f(self.0.get())
        }
    }

    /// Runs `f` on an exclusive pointer to the contents.
    pub(crate) fn with_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(*mut T) -> R,
    {
        #[cfg(loom)]
        {
            self.0.with_mut(f)
        }
        #[cfg(not(loom))]
        {
            f(self.0.get())
        }
    }

    /// Borrows the contents for as long as the returned pointer lives.
    pub(crate) fn const_ptr(&self) -> ConstPtr<T> {
        #[cfg(loom)]
        {
            self.0.get()
        }
        #[cfg(not(loom))]
        {
            ConstPtr(self.0.get())
        }
    }
}

// Deliberately opaque: the contents may be uninitialised, and reading them to
// format would need the very synchronisation this type is a building block for.
impl<T> std::fmt::Debug for UnsafeCell<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UnsafeCell")
    }
}

/// A pointer into an [`UnsafeCell`] that keeps loom's access tracking alive for
/// as long as the pointer itself lives.
///
/// `with` only marks the cell as accessed for the duration of its closure, which
/// is too short for a reader that hands out a `&T` and keeps reading through it.
/// Holding one of these for the whole read is what lets loom see the overlap
/// between that read and a concurrent write.
#[cfg(loom)]
pub(crate) type ConstPtr<T> = loom::cell::ConstPtr<T>;

#[cfg(not(loom))]
#[derive(Debug)]
pub(crate) struct ConstPtr<T>(*const T);

#[cfg(not(loom))]
impl<T> ConstPtr<T> {
    /// # Safety
    ///
    /// Same contract as dereferencing the raw pointer this came from: no
    /// exclusive access to the cell may overlap the returned reference.
    pub(crate) unsafe fn deref(&self) -> &T {
        unsafe { &*self.0 }
    }
}
