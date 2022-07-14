#[cfg(feature = "by-key")]
mod bykey;
mod create_tx;
pub(crate) mod database_reader;
pub mod formatted;
mod key_reader;
pub(crate) mod merge;
pub(crate) mod rayon;
mod records;
pub mod row_format;
pub(crate) mod segment;
pub(crate) mod segment_reader;
mod wildcard;
pub(crate) mod write;

pub use write::WriteFailure;

pub use crate::rayon::*;
#[cfg(feature = "by-key")]
pub use bykey::*;
pub use create_tx::*;
pub use database_reader::*;
pub use key_reader::*;
pub use records::*;
pub(crate) use segment::*;
pub use wildcard::*;
#[cfg(test)]
mod tests;

/// Nanoseconds since the unix epoch
pub type Timestamp = u64;

use std::ops::{Bound, RangeBounds};

pub(crate) fn disassemble_range_bound<'k, T: Copy>(
	rb: impl RangeBounds<T> + 'k,
) -> (Bound<T>, Bound<T>) {
	fn fix_bound<A: Copy>(b: Bound<&A>) -> Bound<A> {
		match b {
			Bound::Included(a) => Bound::Included(*a),
			Bound::Excluded(a) => Bound::Excluded(*a),
			Bound::Unbounded => Bound::Unbounded,
		}
	}
	let range = (fix_bound(rb.start_bound()), fix_bound(rb.end_bound()));

	range
}

use std::borrow::Cow;
/*
pub(crate) fn disassemble_range_bound_cow<'k, T: ToOwned+?Sized>(
	rb: impl RangeBounds<T>+'k,
) -> (Bound<Cow<'k, T>>, Bound<Cow<'k, T>>) {

	fn fix_bound<'a, T: ?Sized+ToOwned>(b: Bound<&'a T>) -> Bound<Cow<'a,T>> {
		match b {
			Bound::Included(a) => Bound::Included(Cow::Borrowed(&*a)),
			Bound::Excluded(a) => Bound::Excluded(Cow::Borrowed(&*a)),
			Bound::Unbounded => Bound::Unbounded,
		}
	}
	let range = (fix_bound(rb.start_bound()), fix_bound(rb.end_bound()));

	range
}*/

#[derive(Clone, Debug)]
pub(crate) struct CowStringRange<'a> {
	pub(crate) begin: Bound<Cow<'a, str>>,
	pub(crate) end: Bound<Cow<'a, str>>,
}

pub(crate) fn bound_deep_copy(b: Bound<&str>) -> Bound<String> {
	match b {
		Bound::Included(a) => Bound::Included(a.to_owned()),
		Bound::Excluded(a) => Bound::Excluded(a.to_owned()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

impl<'a> CowStringRange<'a> {
	fn start_bound(&'a self) -> Bound<&'a str> {
		match &self.begin {
			Bound::Included(a) => Bound::Included(&a[..]),
			Bound::Excluded(a) => Bound::Excluded(&a[..]),
			Bound::Unbounded => Bound::Unbounded,
		}
	}
	fn end_bound(&'a self) -> Bound<&'a str> {
		match &self.end {
			Bound::Included(a) => Bound::Included(&a[..]),
			Bound::Excluded(a) => Bound::Excluded(&a[..]),
			Bound::Unbounded => Bound::Unbounded,
		}
	}
}

impl From<(Bound<String>, Bound<String>)> for CowStringRange<'static> {
	fn from(bound: (Bound<String>, Bound<String>)) -> CowStringRange<'static> {
		let begin = match bound.0 {
			Bound::Included(a) => Bound::Included(Cow::Owned(a)),
			Bound::Excluded(a) => Bound::Excluded(Cow::Owned(a)),
			Bound::Unbounded => Bound::Unbounded,
		};
		let end = match bound.1 {
			Bound::Included(a) => Bound::Included(Cow::Owned(a)),
			Bound::Excluded(a) => Bound::Excluded(Cow::Owned(a)),
			Bound::Unbounded => Bound::Unbounded,
		};
		CowStringRange { begin, end }
	}
}

impl<'a> From<(Bound<&'a str>, Bound<&'a str>)> for CowStringRange<'a> {
	fn from(bound: (Bound<&'a str>, Bound<&'a str>)) -> CowStringRange<'a> {
		let begin = match bound.0 {
			Bound::Included(a) => Bound::Included(Cow::Borrowed(a)),
			Bound::Excluded(a) => Bound::Excluded(Cow::Borrowed(a)),
			Bound::Unbounded => Bound::Unbounded,
		};
		let end = match bound.1 {
			Bound::Included(a) => Bound::Included(Cow::Borrowed(a)),
			Bound::Excluded(a) => Bound::Excluded(Cow::Borrowed(a)),
			Bound::Unbounded => Bound::Unbounded,
		};
		CowStringRange { begin, end }
	}
}
impl<'a> From<(Bound<&'a str>, Bound<String>)> for CowStringRange<'a> {
	fn from(bound: (Bound<&'a str>, Bound<String>)) -> CowStringRange<'a> {
		let begin = match bound.0 {
			Bound::Included(a) => Bound::Included(Cow::Borrowed(a)),
			Bound::Excluded(a) => Bound::Excluded(Cow::Borrowed(a)),
			Bound::Unbounded => Bound::Unbounded,
		};
		let end = match bound.1 {
			Bound::Included(a) => Bound::Included(Cow::Owned(a)),
			Bound::Excluded(a) => Bound::Excluded(Cow::Owned(a)),
			Bound::Unbounded => Bound::Unbounded,
		};
		CowStringRange { begin, end }
	}
}

impl<'a> From<(Bound<String>, Bound<&'a str>)> for CowStringRange<'a> {
	fn from(bound: (Bound<String>, Bound<&'a str>)) -> CowStringRange<'a> {
		let begin = match bound.0 {
			Bound::Included(a) => Bound::Included(Cow::Owned(a)),
			Bound::Excluded(a) => Bound::Excluded(Cow::Owned(a)),
			Bound::Unbounded => Bound::Unbounded,
		};
		let end = match bound.1 {
			Bound::Included(a) => Bound::Included(Cow::Borrowed(a)),
			Bound::Excluded(a) => Bound::Excluded(Cow::Borrowed(a)),
			Bound::Unbounded => Bound::Unbounded,
		};
		CowStringRange { begin, end }
	}
}

impl<'a> From<(Bound<Cow<'a, str>>, Bound<Cow<'a, str>>)> for CowStringRange<'a> {
	fn from(bound: (Bound<Cow<'a, str>>, Bound<Cow<'a, str>>)) -> CowStringRange<'a> {
		CowStringRange {
			begin: bound.0,
			end: bound.1,
		}
	}
}
