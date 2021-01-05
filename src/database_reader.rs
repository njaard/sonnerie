//! Read a database.

use std::fs::File;
use std::io::Seek;
use std::path::{Path, PathBuf};

use crate::key_reader::*;
use crate::merge::Merge;
use crate::record::OwnedRecord;
use crate::Wildcard;

use byteorder::ByteOrder;

/// Read a database in key-timestamp sorted format.
///
/// Open a database with [`new`](#method.new) and then [`get`](#method.get),
/// [`get_filter`](#method.get_filter) or [`get_range`](#method.get_range) to select which keys to read.
pub struct DatabaseReader {
	_dir: PathBuf,
	txes: Vec<(PathBuf, Reader)>,
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

	fn new_opts(dir: &Path, include_main_db: bool) -> std::io::Result<DatabaseReader> {
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
		let mut txes = Vec::with_capacity(paths.len());

		if include_main_db {
			let main_db_name = dir.join("main");
			let mut f = File::open(&main_db_name)?;
			let len = f.seek(std::io::SeekFrom::End(0))? as usize;
			if len == 0 {
				eprintln!("disregarding main database, it is zero length");
			} else {
				let main_db = Reader::new(f)?;
				txes.push((main_db_name, main_db));
			}
		}

		for p in paths {
			let mut f = File::open(&p)?;
			let len = f.seek(std::io::SeekFrom::End(0))? as usize;
			if len == 0 {
				eprintln!("disregarding {:?}, it is zero length", p);
				continue;
			}
			let r = Reader::new(f)?;
			txes.push((p, r));
		}

		Ok(DatabaseReader {
			txes,
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
	pub fn transaction_paths(&self) -> Vec<PathBuf> {
		self.txes.iter().map(|e| e.0.clone()).collect()
	}

	/// Get a reader for only a single key
	///
	/// Returns an object that will read all of the
	/// records for only one key.
	pub fn get<'rdr, 'k>(&'rdr self, key: &'k str) -> DatabaseKeyReader<'rdr, 'k> {
		self.get_range(key..=key)
	}

	/// Get a reader for a lexicographic range of keys
	///
	/// Use inclusive or exclusive range syntax to select a range.
	///
	/// Example: `rdr.get_range("chimpan-ay" ..= "chimpan-zee")`
	///
	/// Range queries are always efficient and readahead
	/// may occur.
	pub fn get_range<'d, 'k>(
		&'d self,
		range: impl std::ops::RangeBounds<&'k str> + Clone + 'k,
	) -> DatabaseKeyReader<'d, 'k> {
		let mut readers = Vec::with_capacity(self.txes.len());

		for tx in &self.txes {
			readers.push(tx.1.get_range(range.clone()));
		}
		let merge = Merge::new(readers, |a, b| {
			a.key().cmp(b.key()).then_with(|| {
				byteorder::BigEndian::read_u64(a.value())
					.cmp(&byteorder::BigEndian::read_u64(b.value()))
			})
		});

		DatabaseKeyReader {
			_db: self,
			merge: Box::new(merge),
		}
	}

	/// Get a reader that filters on SQL's "LIKE"-like syntax.
	///
	/// A wildcard filter that has a fixed prefix, such as
	/// `"chimp%"` is always efficient.
	pub fn get_filter<'d, 'k>(&'d self, wildcard: &'k Wildcard) -> DatabaseKeyReader<'d, 'k> {
		let mut readers = Vec::with_capacity(self.txes.len());

		for tx in &self.txes {
			readers.push(tx.1.get_filter(wildcard));
		}
		let merge = Merge::new(readers, |a, b| {
			a.key().cmp(b.key()).then_with(|| {
				byteorder::BigEndian::read_u64(a.value())
					.cmp(&byteorder::BigEndian::read_u64(b.value()))
			})
		});

		DatabaseKeyReader {
			_db: self,
			merge: Box::new(merge),
		}
	}
}

/// An iterator over the filtered keys in a database.
///
/// Yields an [`OwnedRecord`](record/struct.OwnedRecord.html)
/// for each row in the database, sorted by key and timestamp.
pub struct DatabaseKeyReader<'d, 'k> {
	_db: &'d DatabaseReader,
	merge: Box<Merge<StringKeyRangeReader<'d, 'k>, OwnedRecord>>,
}

impl<'d, 'k> Iterator for DatabaseKeyReader<'d, 'k> {
	type Item = OwnedRecord;

	fn next(&mut self) -> Option<Self::Item> {
		self.merge.next()
	}
}
