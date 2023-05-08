//! Read a database.

use std::fs::File;
use std::io::Seek;
use std::path::{Path, PathBuf};

use crate::key_reader::*;
use crate::merge::Merge;
use crate::segment_reader::DeleteMarker;
use crate::Record;
use crate::Wildcard;
use std::ops::Bound;

use chrono::NaiveDateTime;
use either::Either;
use regex::Regex;

#[cfg(feature = "by-key")]
use crate::bykey::DatabaseKeyReader;

/// Read a database in key-timestamp sorted format.
///
/// Open a database with [`new`](#method.new) and then [`get`](#method.get),
/// [`get_filter`](#method.get_filter) or [`get_range`](#method.get_range) to select which keys to read.
pub struct DatabaseReader {
	_dir: PathBuf,
	pub(crate) txes: Vec<(usize, PathBuf, Reader)>,
	pub(crate) filter_out: Vec<(usize, PathBuf, DeleteMarker)>,
}

impl DatabaseReader {
	/// Open a database at the given path.
	///
	/// All of the committed transactions are opened.
	///
	/// Any transactions that appear after `new` is called
	/// are not opened (create a new `DatabaseReader`).
	pub fn new(dir: &Path) -> std::io::Result<DatabaseReader> {
		Self::new_opts(dir, true)
	}

	/// Open a database at the given path, but not the `main` file.
	///
	/// This is only useful for doing a minor compaction.
	pub fn without_main_db(dir: &Path) -> std::io::Result<DatabaseReader> {
		Self::new_opts(dir, false)
	}

	/// Open a database at the given path.
	///
	/// The `include_main_db` option, if set to false indicates that
	/// the main database should not be opened. This is useful for
	/// minor compaction.
	fn new_opts(dir: &Path, include_main_db: bool) -> std::io::Result<DatabaseReader> {
		use Either::*;

		let dir_reader = std::fs::read_dir(dir)?;

		let mut paths = vec![];

		for entry in dir_reader {
			let entry = entry?;
			if let Some(s) = entry.file_name().to_str() {
				if s.starts_with("tx.") && !s.ends_with(".tmp") {
					paths.push(entry.path());
				}
			}
		}

		paths.sort();
		let mut txes: Vec<(usize, PathBuf, Reader)> = Vec::with_capacity(paths.len());

		if include_main_db {
			let main_db_name = dir.join("main");
			let mut f = File::open(&main_db_name)?;
			let len = f.seek(std::io::SeekFrom::End(0))? as usize;
			if len == 0 {
				eprintln!("disregarding main database, it is zero length");
			} else {
				match Reader::new(f)? {
					Left(main_db) => txes.push((0, main_db_name, main_db)),
					// the main database cannot be a delete marker
					Right(_) => unreachable!(),
				}
			}
		}

		let mut filter_out = vec![];

		let iter = paths
			.into_iter()
			.enumerate()
			// we add 1 because we'd reserve the 0 for the main database,
			// regardless of whether `include_main_db` or not
			.map(|(txid, p)| (txid + 1, p));
		for (txid, p) in iter {
			let mut f = File::open(&p)?;
			let len = f.seek(std::io::SeekFrom::End(0))? as usize;
			if len == 0 {
				eprintln!("disregarding {:?}, it is zero length", p);
				continue;
			}
			let r = Reader::new(f)?;

			// match the reader if it is indeed a reader or a delete marker
			match r {
				Left(r) => txes.push((txid, p, r)),
				Right(d) => filter_out.push((txid, p, d)),
			}
		}

		Ok(DatabaseReader {
			txes,
			filter_out,
			_dir: dir.to_owned(),
		})
	}

	/// Get the filenames of each transaction.
	///
	/// This is useful for compacting, because after
	/// compaction is complete, you would delete all
	/// of the transaction files.
	///
	/// This function also returns the path for `main`,
	/// which is overwritten. Don't delete that.
	///
	/// This function doesn't return the transactions that
	/// contain deletions.
	pub fn transaction_paths(&self) -> Vec<PathBuf> {
		self.txes.iter().map(|(_, e, _)| e.clone()).collect()
	}

	/// Get the filenames of the transactions that have a delete marker in them.
	pub fn delete_txes_paths<'a>(&'a self) -> impl Iterator<Item = &Path> {
		self.filter_out.iter().map(|(_, path, _)| &**path)
	}

	/// Get a reader for only a single key
	///
	/// Returns an object that will read all of the
	/// records for only one key.
	pub fn get<'rdr>(&'rdr self, key: &'rdr str) -> DatabaseRecordReader<'rdr> {
		DatabaseRecordReader {
			db: self,
			matcher: None,
			prefix: "",
			range: crate::disassemble_range_bound(key..=key).into(),
		}
	}

	/// Get a reader for a lexicographic range of keys
	///
	/// Use inclusive or exclusive range syntax to select a range.
	///
	/// Example: `rdr.get_range("chimpan-ay" ..= "chimpan-zee")`
	///
	/// Range queries are always efficient and readahead
	/// may occur.
	pub fn get_range<'d>(
		&'d self,
		range: impl std::ops::RangeBounds<&'d str> + 'd + Clone,
	) -> DatabaseRecordReader<'d> {
		DatabaseRecordReader {
			db: self,
			matcher: None,
			prefix: "",
			range: crate::disassemble_range_bound(range).into(),
		}
	}

	/// Get a key reader for a lexicographic range of keys **`feature=by-key`**
	///
	/// Each iterator represents a given key, you may iterate over each of those
	/// to get each record for that key.
	///
	/// Use inclusive or exclusive range syntax to select a range.
	///
	/// Example: `rdr.get_range("chimpan-ay" ..= "chimpan-zee")`
	///
	/// Range queries are always efficient and readahead
	/// may occur.
	#[cfg(feature = "by-key")]
	pub fn get_range_keys<'d>(
		&'d self,
		range: impl std::ops::RangeBounds<&'d str> + 'd + Clone,
	) -> DatabaseKeyReader<'d> {
		DatabaseKeyReader {
			db: self,
			matcher: None,
			prefix: "",
			range: crate::disassemble_range_bound(range).into(),
		}
	}

	/// Get a reader that filters on SQL's "LIKE"-like syntax.
	///
	/// A wildcard filter that has a fixed prefix, such as
	/// `"chimp%"` is always efficient.
	pub fn get_filter<'d>(&'d self, wildcard: &'d Wildcard) -> DatabaseRecordReader<'d> {
		if wildcard.is_exact() {
			DatabaseRecordReader {
				db: self,
				matcher: wildcard.as_regex(),
				prefix: wildcard.prefix(),
				range: crate::disassemble_range_bound(wildcard.prefix()..=wildcard.prefix()).into(),
			}
		} else {
			DatabaseRecordReader {
				db: self,
				matcher: wildcard.as_regex(),
				prefix: wildcard.prefix(),
				range: crate::disassemble_range_bound(wildcard.prefix()..).into(),
			}
		}
	}

	/// Get a key reader that filters on SQL's "LIKE"-like syntax. **`feature=by-key`**
	///
	/// Each iterator represents a given key, you may iterate over each of those
	/// to get each record for that key.
	///
	/// A wildcard filter that has a fixed prefix, such as
	/// `"chimp%"` is always efficient.
	#[cfg(feature = "by-key")]
	pub fn get_filter_keys<'d>(&'d self, wildcard: &'d Wildcard) -> DatabaseKeyReader<'d> {
		if wildcard.is_exact() {
			DatabaseKeyReader {
				db: self,
				matcher: wildcard.as_regex(),
				prefix: wildcard.prefix(),
				range: crate::disassemble_range_bound(wildcard.prefix()..=wildcard.prefix()).into(),
			}
		} else {
			DatabaseKeyReader {
				db: self,
				matcher: wildcard.as_regex(),
				prefix: wildcard.prefix(),
				range: crate::disassemble_range_bound(wildcard.prefix()..).into(),
			}
		}
	}
}

/// Keeps a range associated with a query, implements `IntoIterator`
///
/// You can call [`into_par_iter`](https://docs.rs/rayon/1.1/rayon/iter/trait.IntoParallelIterator.html#tymethod.into_par_iter)
/// on this object to get a Rayon parallel iterator.
///
/// Note that only one thread will get any specific key; keys are never
/// divided between multiple workers.
pub struct DatabaseRecordReader<'d> {
	db: &'d DatabaseReader,
	matcher: Option<regex::Regex>,
	prefix: &'d str,
	range: crate::CowStringRange<'d>,
}

impl<'d> DatabaseRecordReader<'d> {
	pub(crate) fn check(&self) {
		match (self.range.start_bound(), self.range.end_bound()) {
			(Bound::Unbounded, _) => {}
			(_, Bound::Unbounded) => {}
			(Bound::Included(a), Bound::Included(b)) => assert!(a <= b, "a={:?}, b={:?}", a, b),
			(Bound::Excluded(a), Bound::Included(b)) => assert!(a < b, "a={:?}, b={:?}", a, b),
			(Bound::Included(a), Bound::Excluded(b)) => assert!(a < b, "a={:?}, b={:?}", a, b),
			(Bound::Excluded(a), Bound::Excluded(b)) => assert!(a < b, "a={:?}, b={:?}", a, b),
		}
	}

	pub(crate) fn split(&self) -> Option<(DatabaseRecordReader<'d>, DatabaseRecordReader<'d>)> {
		// look into the readers and see which Reader was biggest
		let (biggest_reader, biggest_portion_size) = self
			.db
			.txes
			.iter()
			.map(|tx| {
				let filter =
					tx.2.get_filter_range(self.matcher.clone(), self.prefix, self.range.clone());
				let b = filter.compressed_bytes();
				(filter, b)
			})
			.max_by_key(|(_, rsize)| *rsize)
			.unwrap();

		// biggest_reader is a StringKeyRangeReader

		let starting_offset = biggest_reader.segment.as_ref()?.segment_offset;

		if biggest_portion_size < crate::write::SEGMENT_SIZE_GOAL * 32 {
			return None;
		}

		let middle_offset =
			starting_offset + biggest_portion_size / 2 - crate::write::SEGMENT_SIZE_GOAL;

		let middle = biggest_reader.reader.segments.scan_from(middle_offset)?;

		let middle_start_key = middle.first_key;
		if Bound::Included(middle_start_key) == self.range.end_bound() {
			return None;
		}

		// the first reader reads from the true beginning to
		// the middle

		assert_ne!(self.range.start_bound(), Bound::Included(middle_start_key));

		let first_half = DatabaseRecordReader {
			db: self.db,
			matcher: self.matcher.clone(),
			prefix: self.prefix,
			range: (
				crate::bound_deep_copy(self.range.start_bound()),
				Bound::Included(middle_start_key.to_owned()),
			)
				.into(),
		};
		first_half.check();
		if let Bound::Included(e) = self.range.start_bound() {
			assert!(e < middle_start_key);
		}

		assert!(
			middle.first_key.starts_with(self.prefix),
			"{} {}",
			middle.first_key,
			self.prefix
		);

		// TODO sometimes we need to use included bounds and sometimes not

		let second_half = DatabaseRecordReader {
			db: self.db,
			matcher: self.matcher.clone(),
			prefix: self.prefix,
			range: (
				Bound::Excluded(middle_start_key.to_owned()),
				crate::bound_deep_copy(self.range.end_bound()),
			)
				.into(),
		};

		if let Bound::Excluded(e) = self.range.end_bound() {
			assert!(middle_start_key < e);
		}

		second_half.check();
		assert_ne!(self.range.start_bound(), Bound::Excluded(middle_start_key));
		assert_ne!(Bound::Included(middle_start_key), self.range.end_bound());

		Some((first_half, second_half))
	}
}

impl<'d> IntoIterator for DatabaseRecordReader<'d> {
	type Item = Record;
	type IntoIter = DatabaseRecordIterator<'d>;

	fn into_iter(self) -> Self::IntoIter {
		self.check();

		let mut readers = Vec::with_capacity(self.db.txes.len());

		for (txid, _path, reader) in self.db.txes.iter() {
			let iter =
				reader.get_filter_range(self.matcher.clone(), self.prefix, self.range.clone());

			readers.push((*txid, iter));
		}
		let merge = Merge::new(readers, |a, b| {
			a.key()
				.cmp(b.key())
				.then_with(|| a.timestamp_nanos().cmp(&b.timestamp_nanos()))
		});

		let filter_out: Vec<_> = self
			.db
			.filter_out
			.iter()
			.map(|(txid, _path, dm)| (*txid, DeleteMarkerPrecomputed::from_delete_marker(dm)))
			.collect();

		DatabaseRecordIterator {
			filter_out,
			merge: Box::new(merge),
		}
	}
}

/// An iterator over the filtered keys in a database.
///
/// Yields an [`Record`](record/struct.Record.html)
/// for each row in the database, sorted by key and timestamp.
pub struct DatabaseRecordIterator<'d> {
	filter_out: Vec<(usize, DeleteMarkerPrecomputed<'d>)>,
	merge: Box<Merge<StringKeyRangeReader<'d, 'd>, Record>>,
}

pub(crate) struct DeleteMarkerPrecomputed<'a> {
	pub first_key: &'a str,
	pub last_key: &'a str,
	pub first_timestamp: NaiveDateTime,
	pub last_timestamp: NaiveDateTime,
	pub wildcard: Either<Regex, &'a str>,
}

impl<'a> DeleteMarkerPrecomputed<'a> {
	pub(crate) fn from_delete_marker(marker: &'a DeleteMarker) -> DeleteMarkerPrecomputed<'a> {
		use Either::*;

		let wildcard = match Wildcard::new(&*marker.wildcard).as_regex() {
			Some(re) => Left(re),
			None => {
				let starts_with = marker.wildcard.split('%').next().unwrap();
				Right(starts_with)
			}
		};

		DeleteMarkerPrecomputed {
			first_key: &*marker.first_key,
			last_key: &*marker.last_key,
			first_timestamp: marker.first_timestamp,
			last_timestamp: marker.last_timestamp,
			wildcard,
		}
	}

	pub(crate) fn wildcard_matches(&self, key: &str) -> bool {
		use Either::*;

		match &self.wildcard {
			Left(re) => re.is_match(key),
			Right(start) => key.starts_with(start),
		}
	}
}

impl<'d> Iterator for DatabaseRecordIterator<'d> {
	type Item = Record;

	fn next(&mut self) -> Option<Self::Item> {
		for (txid, record) in self.merge.by_ref() {
			let is_filtered_out = self
				.filter_out
				.iter()
				// select only transactions that are indexed lower than the
				// delete transaction
				.filter(|(del_txid, _)| txid < *del_txid)
				// check if the record's timestamp is within filtering out
				// this assumes that the filter_out is sorted ascending by
				// first timestamp (which should have been done in
				// DatabaseReader::new())
				.filter(|(_, filter)| {
					let record_time = record.time();
					(filter.first_timestamp..filter.last_timestamp).contains(&record_time)
				})
				.filter(|(_, filter)| record.time() <= filter.last_timestamp)
				// if any of the filters went here (i.e. any() returns a true),
				// then that means that filter found one filter that filters out
				// the current record. that should be discarded
				.any(|(_, filter)| {
					let key = record.key();

					if &*filter.first_key > key {
						return false;
					}

					if !filter.last_key.is_empty() && key >= &*filter.last_key {
						return false;
					}

					filter.wildcard_matches(key)
				});

			if !is_filtered_out {
				return Some(record);
			}
		}

		None
	}
}
