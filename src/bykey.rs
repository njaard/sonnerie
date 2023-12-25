use crate::database_reader::DatabaseReader;
use crate::key_reader::*;
use crate::merge::Merge;
use crate::DeleteMarkerPrecomputed;
use crate::Record;
use lending_cell::{BorrowedCell, LendingCell};
use std::ops::Bound;

/// Iterate over keys, from which you may iterate over each record with that key. **`feature=by-key`**
///
/// Create this object with [`DatabaseReader::get_filter_keys`].
///
/// This Iterator generates items of the type [`DatabaseKeyIterator`], and that
/// is an iterator of [`Record`]s.
///
/// You can call [`into_par_iter`](https://docs.rs/rayon/1.1/rayon/iter/trait.IntoParallelIterator.html#tymethod.into_par_iter)
/// on this object to get a Rayon parallel iterator.
pub struct DatabaseKeyReader<'d> {
	pub(crate) db: &'d DatabaseReader,
	pub(crate) matcher: Option<regex::Regex>,
	pub(crate) prefix: &'d str,
	pub(crate) range: crate::CowStringRange<'d>,
}

impl<'d> DatabaseKeyReader<'d> {
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

	pub(crate) fn split(&self) -> Option<(DatabaseKeyReader<'d>, DatabaseKeyReader<'d>)> {
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

		let first_half = DatabaseKeyReader {
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

		let second_half = DatabaseKeyReader {
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

impl<'d> IntoIterator for DatabaseKeyReader<'d> {
	type Item = KeyRecordReader<'d>;
	type IntoIter = DatabaseKeyIterator<'d>;

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
			.map(|(txid, _path, dm)| (*txid, DeleteMarkerPrecomputed::from_delete_marker(&dm)))
			.collect();

		let mut hot_potato = HotPotato {
			filter_out,
			merge: Box::new(merge),
			queued_record: None,
			current_key: String::new(),
		};

		if let Some(next) = hot_potato.get_next() {
			hot_potato.queued_record = Some(next);
		}

		DatabaseKeyIterator {
			hot_potato_hole: LendingCell::new(hot_potato),
		}
	}
}

struct HotPotato<'d> {
	filter_out: Vec<(usize, DeleteMarkerPrecomputed<'d>)>,
	merge: Box<Merge<StringKeyRangeReader<'d, 'd>, Record>>,
	queued_record: Option<Record>, // record hasn't been outputted yet
	current_key: String,
}

impl<'d> HotPotato<'d> {
	fn get_next(&mut self) -> Option<Record> {
		if let Some(n) = self.queued_record.take() {
			return Some(n);
		}

		while let Some((txid, record)) = self.merge.next() {
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

					if !(&*filter.first_key <= key) {
						return false;
					}

					if &*filter.last_key != "" {
						if !(key < &*filter.last_key) {
							return false;
						}
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

/// An iterator over each key. **`feature=by-key`**
///
/// Yields a [`KeyRecordReader`]
/// for each row in the database, sorted by timestamp.
///
/// # Warning
/// It is a runtime error (panic) to call `next` on this
/// object when you still have instance of [`KeyRecordReader`]
/// that this yielded in-scope.
///
/// For example:
/// ```no_run
/// # let database = sonnerie::DatabaseReader::new(std::path::Path::new("")).unwrap();
/// let mut keys = database.get_range_keys(..).into_iter();
/// let item = keys.next().unwrap();
/// drop(item); // this must be included or the following statement will panic
/// keys.next();
/// ```
///
/// Of course, normal nested iterations will work fine:
/// ```no_run
/// # let database = sonnerie::DatabaseReader::new(std::path::Path::new("")).unwrap();
/// for keys in database.get_range_keys(..) {
///    for record in keys {
///       dbg!(record);
///    }
/// }
/// ```
pub struct DatabaseKeyIterator<'d> {
	hot_potato_hole: LendingCell<HotPotato<'d>>,
}

impl<'d> Iterator for DatabaseKeyIterator<'d> {
	type Item = KeyRecordReader<'d>;

	fn next(&mut self) -> Option<Self::Item> {
		let hot_potato = self.hot_potato_hole.get_mut();

		// if the previous KeyRecordReader was dropped before it got to the end,
		// we have to skip all the records from the old key
		loop {
			let next = hot_potato.get_next()?;
			if hot_potato.current_key != next.key() {
				hot_potato.current_key.replace_range(.., next.key());
				hot_potato.queued_record = Some(next);
				return Some(KeyRecordReader {
					hot_potato: self.hot_potato_hole.to_borrowed(),
				});
			}
			//eprintln!("discarding {next:?}, c={}", hot_potato.current_key);
		}
	}
}

/// Allows iterating over every record for a given key. **`feature=by-key`**
///
/// [`DatabaseKeyIterator`] yields one of these for every
/// record for the given key.
pub struct KeyRecordReader<'d> {
	hot_potato: BorrowedCell<HotPotato<'d>>,
}

impl<'d> KeyRecordReader<'d> {
	/// Returns the value of the key that this iterator reads.
	/// This key will almost match the [`Record::key`] for every element
	/// yielded by this iterator.
	pub fn key(&self) -> &str {
		&self.hot_potato.current_key
	}
}

impl<'d> Iterator for KeyRecordReader<'d> {
	type Item = Record;

	fn next(&mut self) -> Option<Record> {
		let hot_potato = &mut self.hot_potato;
		if let Some(s) = hot_potato.queued_record.take() {
			return Some(s);
		}

		let next = hot_potato.get_next()?;

		if &hot_potato.current_key != next.key() {
			hot_potato.queued_record = Some(next);
			return None;
		}

		Some(next)
	}
}

#[cfg(test)]
mod tests {
	use crate::*;
	#[test]
	fn high_level_writer() {
		let t = tempfile::TempDir::new().unwrap();

		{
			let mut tx = CreateTx::new(t.path()).expect("creating tx");
			tx.add_record(
				"a",
				"2010-01-01T00:00:01".parse().unwrap(),
				&[&42u32 as &dyn crate::ToRecord],
			)
			.unwrap();
			tx.add_record(
				"a",
				"2010-01-01T00:00:02".parse().unwrap(),
				&[&84u32 as &dyn crate::ToRecord],
			)
			.unwrap();
			tx.add_record(
				"a",
				"2010-01-01T00:00:03".parse().unwrap(),
				&[&66u32 as &dyn crate::ToRecord],
			)
			.unwrap();
			tx.add_record(
				"b",
				"2010-01-01T00:00:04".parse().unwrap(),
				&[&34.0f64 as &dyn crate::ToRecord, &22.0f32],
			)
			.unwrap();
			tx.add_record(
				"b",
				"2010-01-01T00:00:05".parse().unwrap(),
				&[&3.1415f64 as &dyn crate::ToRecord, &2.7182f32],
			)
			.unwrap();
			tx.add_record(
				"c",
				"2010-01-01T00:00:01".parse().unwrap(),
				&[&"Hello World" as &dyn crate::ToRecord, &"Rustacean"],
			)
			.unwrap();

			tx.commit_to(&t.path().join("main")).expect("committed");
		}
		let r = DatabaseReader::new(t.path()).unwrap();
		let w = crate::Wildcard::new("%");

		{
			let mut ks = r.get_filter_keys(&w).into_iter();
			{
				let mut k = ks.next().unwrap();
				assert_eq!(k.key(), "a");
				let r = k.next().unwrap();
				assert_eq!(r.value::<u32>(), 42);
				let r = k.next().unwrap();
				assert_eq!(r.value::<u32>(), 84);
				let r = k.next().unwrap();
				assert_eq!(r.value::<u32>(), 66);
				assert!(k.next().is_none());
			}

			{
				let mut k = ks.next().unwrap();
				assert_eq!(k.key(), "b");
				let r = k.next().unwrap();
				assert_eq!(r.get::<f32>(1), 22.0f32);
				let r = k.next().unwrap();
				assert_eq!(r.get::<f32>(1), 2.7182f32);
				assert!(k.next().is_none());
			}

			{
				let mut k = ks.next().unwrap();
				assert_eq!(k.key(), "c");
				let r = k.next().unwrap();
				assert_eq!(r.get::<&str>(0), "Hello World");
				assert_eq!(r.get::<&str>(1), "Rustacean");
				assert!(k.next().is_none());
			}
		}

		{
			let ks = r.get_filter_keys(&w);
			for k in ks {
				println!("key: {}", k.key());
				for r in k {
					eprintln!("{r:?}");
				}
			}
		}
		{
			let ks = r.get_filter_keys(&w);
			for k in ks {
				println!("key: {}", k.key());
				for r in k {
					if r.key() == "a" {
						break;
					}
					eprintln!("{r:?}");
				}
			}
		}
	}
}
