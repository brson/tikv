// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::bit_vec::BitVec;
use super::{ChunkRef, ChunkedVec, Evaluable, EvaluableRet, UnsafeRefInto};

/// A vector storing `Option<T>` with a compact layout.
///
/// `T` must be a primitive structure. All data must be stored
/// in that structure itself. This includes `Int`, `Real`, `Decimal`,
/// `DateTime` and `Duration` in copr framework.
///
/// Inside `ChunkedVecSized`, `bitmap` indicates if an element at given index is null,
/// and `data` stores actual data. If the element at given index is null (or `None`),
/// the corresponding `bitmap` bit is false, and `data` stores zero value for
/// that element. Otherwise, `data` stores actual data, and `bitmap` bit is true.
#[derive(Debug, PartialEq, Clone)]
pub struct ChunkedVecSized<T: Sized> {
    data: Vec<T>,
    bitmap: BitVec,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Sized + Clone> ChunkedVecSized<T> {
    pub fn from_slice(slice: &[Option<T>]) -> Self {
        let mut x = Self::with_capacity(slice.len());
        for i in slice {
            x.push(i.clone());
        }
        x
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            bitmap: BitVec::with_capacity(capacity),
            phantom: std::marker::PhantomData,
        }
    }

    pub fn from_vec(data: Vec<Option<T>>) -> Self {
        let mut x = Self::with_capacity(data.len());
        for element in data {
            x.push(element);
        }
        x
    }

    pub fn push_data(&mut self, value: T) {
        self.bitmap.push(true);
        self.data.push(value);
    }

    pub fn push_null(&mut self) {
        self.bitmap.push(false);
        self.data.push(unsafe { std::mem::zeroed() });
    }

    pub fn push(&mut self, value: Option<T>) {
        if let Some(x) = value {
            self.push_data(x);
        } else {
            self.push_null();
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn truncate(&mut self, len: usize) {
        self.data.truncate(len);
        self.bitmap.truncate(len);
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn append(&mut self, other: &mut Self) {
        self.data.append(&mut other.data);
        self.bitmap.append(&mut other.bitmap);
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        assert!(idx < self.data.len());
        if self.bitmap.get(idx) {
            Some(&self.data[idx])
        } else {
            None
        }
    }

    pub fn to_vec(&self) -> Vec<Option<T>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            x.push(self.get(i).cloned());
        }
        x
    }
}

impl<T: Clone> ChunkedVec<T> for ChunkedVecSized<T> {
    fn chunked_with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }
    fn chunked_push(&mut self, value: Option<T>) {
        self.push(value)
    }
}

impl<'a, T: Evaluable + EvaluableRet> ChunkRef<'a, &'a T> for &'a ChunkedVecSized<T> {
    fn get_option_ref(self, idx: usize) -> Option<&'a T> {
        self.get(idx)
    }

    fn phantom_data(self) -> Option<&'a T> {
        None
    }
}

impl<T: Clone> Into<ChunkedVecSized<T>> for Vec<Option<T>> {
    fn into(self) -> ChunkedVecSized<T> {
        ChunkedVecSized::from_vec(self)
    }
}

impl<'a, T: Evaluable> UnsafeRefInto<&'static ChunkedVecSized<T>> for &'a ChunkedVecSized<T> {
    unsafe fn unsafe_into(self) -> &'static ChunkedVecSized<T> {
        std::mem::transmute(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::codec::data_type::*;

    #[test]
    fn test_slice_vec() {
        let test_decimal: &[Option<Decimal>] = &[
            Decimal::from_f64(1.233).ok(),
            Decimal::from_f64(2.233).ok(),
            Decimal::from_f64(3.233).ok(),
            Decimal::from_f64(4.233).ok(),
            Decimal::from_f64(5.233).ok(),
            None,
        ];
        assert_eq!(
            ChunkedVecSized::<Decimal>::from_slice(test_decimal).to_vec(),
            test_decimal
        );
        assert_eq!(
            ChunkedVecSized::<Decimal>::from_vec(test_decimal.to_vec()).to_vec(),
            test_decimal
        );
        let test_real: &[Option<Real>] = &[
            Real::new(1.01001).ok(),
            Real::new(-0.01).ok(),
            Real::new(1.02001).ok(),
            Real::new(std::f64::MIN).ok(),
            Real::new(std::f64::MAX).ok(),
            None,
        ];
        assert_eq!(
            ChunkedVecSized::<Real>::from_slice(test_real).to_vec(),
            test_real
        );
        assert_eq!(
            ChunkedVecSized::<Real>::from_vec(test_real.to_vec()).to_vec(),
            test_real
        );
        let mut ctx = EvalContext::default();
        let test_duration: &[Option<Duration>] = &[
            Duration::parse(&mut ctx, b"17:51:04.78", 2).ok(),
            Duration::parse(&mut ctx, b"-17:51:04.78", 2).ok(),
            Duration::parse(&mut ctx, b"17:51:04.78", 0).ok(),
            Duration::parse(&mut ctx, b"-17:51:04.78", 0).ok(),
            None,
        ];
        assert_eq!(
            ChunkedVecSized::<Duration>::from_slice(test_duration).to_vec(),
            test_duration
        );
        assert_eq!(
            ChunkedVecSized::<Duration>::from_vec(test_duration.to_vec()).to_vec(),
            test_duration
        );
        let test_datetime: &[Option<DateTime>] = &[
            DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:00", 0, false).ok(),
            DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:01", 0, false).ok(),
            DateTime::parse_datetime(&mut ctx, "1000-01-01 00:00:02", 0, false).ok(),
        ];
        assert_eq!(
            ChunkedVecSized::<DateTime>::from_slice(test_datetime).to_vec(),
            test_datetime
        );
        assert_eq!(
            ChunkedVecSized::<DateTime>::from_vec(test_datetime.to_vec()).to_vec(),
            test_datetime
        );
        let test_int: &[Option<Int>] =
            &[Some(1), Some(1), Some(233), Some(2333), Some(23333), None];
        assert_eq!(
            ChunkedVecSized::<Int>::from_slice(test_int).to_vec(),
            test_int
        );
        assert_eq!(
            ChunkedVecSized::<Int>::from_vec(test_int.to_vec()).to_vec(),
            test_int
        );
    }

    #[test]
    fn test_basics() {
        let mut x: ChunkedVecSized<Int> = ChunkedVecSized::with_capacity(0);
        x.push(Some(1));
        x.push(Some(2));
        x.push(Some(3));
        x.push(None);
        assert_eq!(x.get(0), Some(&1));
        assert_eq!(x.get(1), Some(&2));
        assert_eq!(x.get(2), Some(&3));
        assert_eq!(x.get(3), None);
        assert_eq!(x.len(), 4);
        assert!(!x.is_empty());
    }

    #[test]
    fn test_truncate() {
        let test_real: &[Option<Real>] = &[
            None,
            Real::new(1.01001).ok(),
            Real::new(-0.01).ok(),
            Real::new(1.02001).ok(),
            Real::new(std::f64::MIN).ok(),
            Real::new(std::f64::MAX).ok(),
            None,
        ];
        let mut chunked_vec = ChunkedVecSized::<Real>::from_slice(test_real);
        chunked_vec.truncate(100);
        assert_eq!(chunked_vec.len(), 7);
        chunked_vec.truncate(3);
        assert_eq!(chunked_vec.len(), 3);
        assert_eq!(chunked_vec.get(0), None);
        assert_eq!(chunked_vec.get(1), Real::new(1.01001).ok().as_ref());
        assert_eq!(chunked_vec.get(2), Real::new(-0.01).ok().as_ref());
        chunked_vec.truncate(0);
        assert_eq!(chunked_vec.len(), 0);
    }

    #[test]
    fn test_append() {
        let test_real_1: &[Option<Real>] = &[None, Real::new(1.01001).ok(), Real::new(-0.01).ok()];
        let test_real_2: &[Option<Real>] = &[
            Real::new(1.02001).ok(),
            Real::new(std::f64::MIN).ok(),
            Real::new(std::f64::MAX).ok(),
            None,
        ];
        let mut chunked_vec_1 = ChunkedVecSized::<Real>::from_slice(test_real_1);
        let mut chunked_vec_2 = ChunkedVecSized::<Real>::from_slice(test_real_2);
        chunked_vec_1.append(&mut chunked_vec_2);
        assert_eq!(chunked_vec_1.len(), 7);
        assert!(chunked_vec_2.is_empty());
        assert_eq!(
            chunked_vec_1.to_vec(),
            &[
                None,
                Real::new(1.01001).ok(),
                Real::new(-0.01).ok(),
                Real::new(1.02001).ok(),
                Real::new(std::f64::MIN).ok(),
                Real::new(std::f64::MAX).ok(),
                None,
            ]
        );
    }
}
