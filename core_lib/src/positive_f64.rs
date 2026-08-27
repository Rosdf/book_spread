use std::cmp::Ordering;

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct PositiveF64(f64);

impl Eq for PositiveF64 {}

impl PartialOrd for PositiveF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PositiveF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        // SAFETY:
        // value is positive, nan can not be here
        unsafe { self.0.partial_cmp(&other.0).unwrap_unchecked() }
    }
}

impl PositiveF64 {
    const fn is_valid(value: f64) -> bool {
        value.is_sign_positive() && !value.is_nan()
    }

    pub const fn new(value: f64) -> Option<Self> {
        if Self::is_valid(value) {
            // SAFETY:
            // `Self::is_valid(value)` was just checked true above.
            Some(unsafe { Self::new_unchecked(value) })
        } else {
            None
        }
    }

    /// # Safety
    /// value should be positive and not Nan
    pub const unsafe fn new_unchecked(value: f64) -> Self {
        debug_assert!(Self::is_valid(value), "value is not valid");
        Self(value)
    }

    pub const fn get(self) -> f64 {
        self.0
    }
}
