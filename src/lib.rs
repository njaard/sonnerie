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

/// Perform a compaction of the DB.
///
/// For use with downstream crates needing periodic
/// compactions. Returns the number of records compacted.
pub fn compact(dir: &std::path::Path, major: bool) -> std::io::Result<u64> {
	use fs2::FileExt;
	let lock = std::fs::File::create(dir.join(".compact"))?;
	lock.try_lock_exclusive()?;

	let db = if major {
		DatabaseReader::new(dir)?
	} else {
		DatabaseReader::without_main_db(dir)?
	};

	{
		let ps = db.transaction_paths();
		if db.num_txes() <= 1 || (ps.len() == 1 && ps[0].file_name().expect("filename") == "main") {
			return Ok(0);
		}
	}
	// println!("Processing {} .txes", db.num_txes());
	let db = std::sync::Arc::new(db);
	let mut compacted = CreateTx::new(dir)?;

	// create the new transaction after opening the database reader
	let reader = db.get_range(..);
	let mut n = 0u64;
	for record in reader {
		compacted
			.add_record_raw(record.key(), record.format(), record.raw())
			.expect("Error adding record");
		n += 1;
	}
	_purge_compacted_files(compacted, dir, &db, major)?;
	
	Ok(n)
}

// not part of public api
#[doc(hidden)]
pub fn _purge_compacted_files(
	compacted: CreateTx,
	dir: &std::path::Path,
	db: &DatabaseReader,
	major: bool,
) -> std::io::Result<()> {
	let source_transaction_paths = db.transaction_paths();

	let removed_transaction_paths = if major {
		compacted.commit_to(&dir.join("main"))?;
		&source_transaction_paths[..]
	} else {
		// allow OS to atomically replace `first_path` (and don't delete it afterwards)
		let keep_path = &source_transaction_paths.last().unwrap();

		compacted.commit_to(keep_path)?;
		&source_transaction_paths[..source_transaction_paths.len() - 1]
	};

	for txfile in removed_transaction_paths {
		if txfile.file_name().expect("filename in txfile") == "main" {
			continue;
		}
		if let Err(e) = std::fs::remove_file(txfile) {
			eprintln!("warning: failed to remove {:?}: {}", txfile, e);
		}
	}

	if major {
		for txfile in db.delete_txes_paths() {
			if let Err(e) = std::fs::remove_file(txfile) {
				eprintln!("warning: failed to remove {:?}: {}", txfile, e);
			}
		}
	}

	Ok(())
}
