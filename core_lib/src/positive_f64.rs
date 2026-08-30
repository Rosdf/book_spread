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
        // value is positive, nan can not be here, so comparing the raw bits is exact: for a
        // sign-positive, non-NaN `f64`, the sign bit is 0 and the exponent and mantissa are
        // laid out most-significant-first, so the IEEE bit pattern - read as an integer -
        // increases exactly where the value does. That is the same invariant `new_unchecked`
        // requires, so this is just `partial_cmp`'s answer reached without the NaN branch.
        self.0.to_bits().cmp(&other.0.to_bits())
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn new_rejects_negative_and_nan() {
        assert!(PositiveF64::new(-1.0).is_none());
        assert!(PositiveF64::new(-0.0).is_none());
        assert!(PositiveF64::new(f64::NAN).is_none());
        assert!(PositiveF64::new(0.0).is_some());
        assert!(PositiveF64::new(1.0).is_some());
    }

    #[test]
    fn ordering_matches_value_ordering() {
        let values = [
            0.0,
            f64::MIN_POSITIVE,
            1e-300,
            0.5,
            1.0,
            2.0,
            1e300,
            f64::MAX,
            f64::INFINITY,
        ];
        let wrapped: Vec<PositiveF64> = values
            .into_iter()
            .map(|v| PositiveF64::new(v).unwrap())
            .collect();

        for (i, &a) in wrapped.iter().enumerate() {
            for (j, &b) in wrapped.iter().enumerate() {
                assert_eq!(a.cmp(&b), i.cmp(&j), "mismatch comparing {a:?} and {b:?}");
                assert_eq!(a.partial_cmp(&b), Some(i.cmp(&j)));
            }
        }
    }

    #[test]
    fn equal_values_compare_equal() {
        let a = PositiveF64::new(1.5).unwrap();
        let b = PositiveF64::new(1.5).unwrap();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);
    }

    #[test]
    fn sorts_a_shuffled_list() {
        let mut sorted: Vec<PositiveF64> = [3.0, 1.0, 0.0, 2.5, 100.0, 0.001]
            .into_iter()
            .map(|v| PositiveF64::new(v).unwrap())
            .collect();
        sorted.sort();

        let values: Vec<f64> = sorted.into_iter().map(PositiveF64::get).collect();
        assert_eq!(values, [0.0, 0.001, 1.0, 2.5, 3.0, 100.0]);
    }

    #[test]
    fn get_round_trips_the_value() {
        let value = 42.25;
        assert_eq!(PositiveF64::new(value).unwrap().get(), value);
    }
}
