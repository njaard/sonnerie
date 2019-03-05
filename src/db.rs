
use crate::metadata::Metadata;
pub use crate::metadata::Transaction;
use crate::wal::MemoryWal;
use crate::blocks::Blocks;
use crate::disk_wal::{DiskWalWriter,DiskWalReader};
use crate::block_file::BlockFile;

use ::std::path::{Path,PathBuf};
use ::std::collections::VecDeque;

use ::std::sync::Arc;
use antidote::{Mutex,Condvar};

pub use crate::metadata::Timestamp;

struct MergeState
{
	stop: bool,
	merging_min: u64,
}

pub struct Db
{
	metadatapath: PathBuf,
	path: PathBuf,
	/// .0 is the generation, and sorted by that
	unflushed_wal_files: Arc<Mutex<VecDeque<(u64,PathBuf)>>>,
	blocks: Arc<Blocks>,
	merge_state: Arc<(Mutex<MergeState>, Condvar)>,

	merging_thread: Option<::std::thread::JoinHandle<()>>,

	pub(crate) max_generation: Mutex<u64>,
	pub(crate) next_offset: Mutex<u64>,
}

impl Db
{
	#[allow(dead_code)] // only used in tests
	pub fn open(path: PathBuf) -> Db
	{
		Db::open2(path.clone(), path.clone())
	}
	pub fn open2(path: PathBuf, metadatadir: PathBuf) -> Db
	{
		let metadatapath = metadatadir.join("meta");

		let mut wal = MemoryWal::new();

		let unflushed_wal_files =
			read_unflushed_wal_files(&path, &mut wal);

		let blockfilename = path.join("blocks");
		let file = BlockFile::new(&blockfilename);

		let blocks = Arc::new(Blocks::new(file, wal));

		let mut max_generation;
		let next_offset;

		{
			let metadata = Metadata::new(&metadatapath, blocks.clone());
			max_generation = metadata.last_generation();
			next_offset = metadata.next_offset();
		}

		if let Some((gen, _)) = unflushed_wal_files.back()
		{
			max_generation = max_generation.max(*gen);
		}

		let merge_state = Arc::new((
			Mutex::new(
				MergeState
				{
					stop: false,
					merging_min: max_generation,
				}
			),
			Condvar::new()
		));

		let unflushed_wal_files =
			Arc::new(Mutex::new(unflushed_wal_files));

		Db
		{
			merge_state: merge_state,
			path: path,
			metadatapath: metadatapath,
			unflushed_wal_files: unflushed_wal_files,
			blocks: blocks,
			merging_thread: None,
			max_generation: Mutex::new(max_generation),
			next_offset: Mutex::new(next_offset),
		}
	}

	pub fn start_merge_thread(&mut self)
	{
		assert!(self.merging_thread.is_none());
		let blocks = self.blocks.clone();
		let unflushed_wal_files = self.unflushed_wal_files.clone();
		let merge_state = self.merge_state.clone();
		let th = ::std::thread::Builder::new()
			.name("sonnerie-merge".into());

		let metadatapath = self.metadatapath.clone();

		let merging_thread = th.spawn(
			move ||
			{
				let mut exit = false;
				let mut previously_merged_to: u64 = 0;

				let db = rusqlite::Connection::open_with_flags(
					&metadatapath,
					rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
						| rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
						| rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
				).unwrap();
				let mut checkpoint = db.prepare("PRAGMA wal_checkpoint")
					.unwrap();

				while !exit
				{
					checkpoint.query(&[]).unwrap();

					let now_merging;
					{
						let mut l = merge_state.0.lock();
						while l.merging_min == previously_merged_to && !l.stop
						{
							l = merge_state.1.wait(l);
						}
						exit = l.stop;
						now_merging = l.merging_min;
					}

					crate::wal::merge(
						&blocks.wal,
						&blocks.file,
					);

					{
						previously_merged_to = now_merging;
						blocks.file.sync();

						loop
						{
							let mut u = unflushed_wal_files.lock();
							if let Some((fg, _)) = u.front().cloned()
							{
								if fg <= now_merging
								{
									let f = u.pop_front();
									drop(u);
									let f = f.unwrap();
									::std::fs::remove_file(&f.1)
										.unwrap();
								}
								else
								{
									break;
								}
							}
							else
							{
								break;
							}
						}
					}
				}
			}
		).expect("failed to spawn merging thread");

		self.merging_thread = Some(merging_thread);
	}

	pub fn read_transaction(&self) -> Transaction
	{
		let metadata = Metadata::open(&self.metadatapath, self.blocks.clone());
		metadata.as_read_transaction()
	}

	pub fn write_transaction(&self) -> Transaction
	{
		let metadata = Metadata::open(
			&self.metadatapath, self.blocks.clone()
		);

		let tx = metadata.as_write_transaction(self);

		// we don't create the disk wal until the tx
		// is ready to go
		let g = tx.metadata.generation;

		let (walwriter, file) = DiskWalWriter::new(g, &self.path);
		self.blocks.set_disk_wal(walwriter);
		self.unflushed_wal_files.lock().push_back((g, file));

		tx
	}

	pub fn committing(
		&self,
		next_offset: u64,
		generation: u64,
	)
	{
		*self.max_generation.lock() += 1;
		*self.next_offset.lock() = next_offset;

		{
			let mut l = self.merge_state.0.lock();
			l.merging_min = generation;
			self.merge_state.1.notify_one();
		}
	}
}

impl Drop for Db
{
	fn drop(&mut self)
	{
		{
			let mut l = self.merge_state.0.lock();
			l.stop = true;
			self.merge_state.1.notify_one();
		}

		if let Some(m) = self.merging_thread.take()
		{
			m.join().unwrap();
		}
		else
		{
			eprintln!("warning: no merging thread");
		}
	}
}

fn read_unflushed_wal_files(
	dbdir: &Path,
	into: &mut MemoryWal,
) -> VecDeque<(u64,PathBuf)>
{
	// we must read in generational order
	let mut all_wals = vec!();

	for entry in ::std::fs::read_dir(dbdir).unwrap()
	{
		let entry = entry.unwrap();
		if !entry.file_type().unwrap().is_file()
			{ continue; }
		if !entry.file_name().to_str().unwrap()
			.ends_with(".wal")
			{ continue; }

		let d = DiskWalReader::open(&entry.path());

		all_wals.push( (d.generation(), entry.path()) );
	}

	all_wals.sort_unstable_by_key(|(g,_)| *g);
	for (_,f) in &all_wals
	{
		let mut d = DiskWalReader::open(&f);
		eprintln!("* reading from {:?}", f);
		d.read_into(into);
	}

	VecDeque::from(all_wals)
}

#[cfg(test)]
mod tests
{
	use crate::db::{Db,Timestamp};

	fn n() -> (tempfile::TempDir, Db)
	{
		let tmp = tempfile::TempDir::new().unwrap();
		let m = Db::open(tmp.path().to_path_buf());
		//m.start_merge_thread();
		(tmp, m)
	}

	fn read_vals<T>(tx: &crate::metadata::Transaction, series_id: u64, timestamp1: u64, timestamp2: u64)
		-> Vec<(Timestamp, T)>
	where
		T: std::str::FromStr + std::fmt::Debug,
		<T as std::str::FromStr>::Err: std::fmt::Debug
	{
		let mut results = vec!();

		tx.read_series(
			series_id, Timestamp(timestamp1), Timestamp(timestamp2),
			|ts, format, data|
			{
				let mut o = ::std::io::Cursor::new(vec!());
				format.to_protocol_format(data, &mut o).unwrap();
				let o = String::from_utf8(o.into_inner()).unwrap();
				let v = o.parse().unwrap();
				results.push((*ts, v));
			}
		);
		results
	}

	/// inserts a single value into a series
	fn insert_val<T>(
		tx: &mut crate::metadata::Transaction, series_id: u64, ts: Timestamp, value: T,
	)
	where T: std::str::FromStr + std::fmt::Debug + std::fmt::Display,
		<T as std::str::FromStr>::Err: std::fmt::Debug
	{
		let mut has = true;
		tx.insert_into_series(
			series_id,
			|format, dest|
			{
				if has
				{
					has = false;
					format.to_stored_format(
						&ts,
						&format!("{}", value),
						dest
					)?;
					Ok(Some(ts))
				}
				else
				{
					Ok(None)
				}
			}
		).unwrap();
	}


	#[test]
	fn dbmeta1()
	{
		let (tmp,m) = n();
		eprintln!("created in {:?}", tmp.path());
		{
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_val::<f64>(&mut txw, h, Timestamp(1000), 42.0);
			insert_val::<f64>(&mut txw, h, Timestamp(1001), 43.0);

			assert_eq!(
				format!("{:?}", read_vals::<f64>(&txw, h, 1000, 1001)),
				"[(Timestamp(1000), 42.0), (Timestamp(1001), 43.0)]"
			);

			let txr = m.read_transaction();
			assert_eq!(
				format!("{:?}", read_vals::<f64>(&txr, h, 1000, 1001)),
				"[]"
			);

			txw.commit();
			assert_eq!(
				format!("{:?}", read_vals::<f64>(&txr, h, 1000, 1001)),
				"[]"
			);
			let txr2 = m.read_transaction();
			assert_eq!(
				format!("{:?}", read_vals::<f64>(&txr2, h, 1000, 1001)),
				"[(Timestamp(1000), 42.0), (Timestamp(1001), 43.0)]"
			);

		}
	}

	#[test]
	fn save_disk_wal()
	{
		let (tmp,m) = n();
		{
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_val::<f64>(&mut txw, h, Timestamp(1000), 42.0);
			insert_val::<f64>(&mut txw, h, Timestamp(1001), 43.0);
			txw.commit();
		}
		drop(m);
		{
			let mut m = Db::open(tmp.path().to_path_buf());
			m.start_merge_thread();
			let txr = m.read_transaction();
			let h = txr.series_id("horse").unwrap();
			assert_eq!(
				format!("{:?}", read_vals::<f64>(&txr, h, 1000, 1001)),
				"[(Timestamp(1000), 42.0), (Timestamp(1001), 43.0)]"
			);
		}
	}

	#[test]
	fn two_series()
	{
		let (_tmp,m) = n();
		{
			let mut txw = m.write_transaction();
			let h1 = txw.create_series("horse1", "F").unwrap();
			let h2 = txw.create_series("horse2", "F").unwrap();
			insert_val::<f64>(&mut txw, h1, Timestamp(1000), 101.0);
			insert_val::<f64>(&mut txw, h1, Timestamp(1001), 102.0);
			insert_val::<f64>(&mut txw, h2, Timestamp(1000), 201.0);
			insert_val::<f64>(&mut txw, h2, Timestamp(1001), 202.0);
			assert_eq!(
				format!("{:?}", read_vals::<f64>(&txw, h1, 1000, 1001)),
				"[(Timestamp(1000), 101.0), (Timestamp(1001), 102.0)]"
			);
			assert_eq!(
				format!("{:?}", read_vals::<f64>(&txw, h2, 1000, 1001)),
				"[(Timestamp(1000), 201.0), (Timestamp(1001), 202.0)]"
			);
		}
	}

	#[test]
	fn select_weird_ranges()
	{
		let (_tmp,m) = n();
		{
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_val::<f64>(&mut txw, h, Timestamp(1000), 1.0);
			insert_val::<f64>(&mut txw, h, Timestamp(1001), 2.0);
			insert_val::<f64>(&mut txw, h, Timestamp(1002), 3.0);
			insert_val::<f64>(&mut txw, h, Timestamp(1003), 4.0);
			assert_eq!(
				format!("{:?}", read_vals::<f64>(&txw, h, 1001, 1003)),
				"[(Timestamp(1001), 2.0), (Timestamp(1002), 3.0), (Timestamp(1003), 4.0)]"
			);
			assert_eq!(
				format!("{:?}", read_vals::<f64>(&txw, h, 1001, 1001)),
				"[(Timestamp(1001), 2.0)]"
			);
		}
	}

	#[test]
	fn boundary_crossing()
	{
		let (_tmp,m) = n();
		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "F").unwrap();
		for x in 1..30000
		{
			insert_val::<f64>(&mut txw, h, Timestamp(x), x as f64);
		}
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txw, h, 10012, 10012)),
			"[(Timestamp(10012), 10012.0)]"
		);

		for start in (1..25000).step_by(11)
		{
			for len in 1..17
			{
				let s = read_vals::<f64>(&txw, h, start, start+len-1);
				assert_eq!(s.len(), len as usize);
				for (idx,a) in s.iter().enumerate()
				{
					assert_eq!((a.0).0 as u64, start+idx as u64);
					assert_eq!(a.1, (start+idx as u64) as f64);
				}
			}
		}
	}

	fn generator_f64<'q>(items: &'q [(Timestamp, f64)])
		-> impl 'q + FnMut(&crate	::row_format::RowFormat, &mut Vec<u8>)
			-> Result<Option<Timestamp>, String>
	{
		let mut i = items.iter();

		let f = move |format: &crate::row_format::RowFormat, data: &mut Vec<u8>|
			-> Result<Option<Timestamp>, String>
		{
			if let Some((ts,val)) = i.next()
			{
				let formatted = format!("{}", val);
				format.to_stored_format(ts, &formatted, data)?;
				Ok(Some(*ts))
			}
			else
			{
				Ok(None)
			}
		};
		f
	}


	#[test]
	fn boundary_crossing_bulk_load()
	{
		let (tmp,m) = n();
		eprintln!("created in {:?}", tmp.path());
		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "F").unwrap();
		let mut items_to_insert = vec!();
		for x in 1..30000
		{
			items_to_insert.push((Timestamp(x), (x*10) as f64));
		}
		txw.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txw, h, 10012, 10012)),
			"[(Timestamp(10012), 100120.0)]"
		);

		for start in (1..25000).step_by(11)
		{
			for len in 1..17
			{
				let s = read_vals::<f64>(&txw, h, start, start+len-1);
				assert_eq!(s.len(), len as usize);
				for (idx,a) in s.iter().enumerate()
				{
					assert_eq!(a.0 .0 as u64, start+idx as u64);
					assert_eq!(a.1, ((start+idx as u64)*10) as f64);
				}
			}
		}
	}

	#[test]
	fn block_overflows()
	{
		let (tmp,m) = n();
		eprintln!("created in {:?}", tmp.path());
		let mut txw = m.write_transaction();
		let h1 = txw.create_series("horse1", "F").unwrap();
		let h2 = txw.create_series("horse2", "F").unwrap();
		{
			let mut items_to_insert = vec!();
			for x in 1..513
			{
				items_to_insert.push((Timestamp(x), (x) as f64));
			}
			txw.insert_into_series(h1, generator_f64(&items_to_insert)).unwrap();
			txw.insert_into_series(h2, generator_f64(&items_to_insert)).unwrap();
		}
		{
			let mut items_to_insert = vec!();
			for x in 540..541
			{
				items_to_insert.push((Timestamp(x), (x) as f64));
			}
			txw.insert_into_series(h1, generator_f64(&items_to_insert)).unwrap();
		}
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txw, h1, 1, 1)),
			"[(Timestamp(1), 1.0)]"
		);
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txw, h2, 1, 1)),
			"[(Timestamp(1), 1.0)]"
		);

		txw.commit();

		let db = ::rusqlite::Connection::open(tmp.path().join("meta")).unwrap();
		let count: i64 = db.query_row(
			"select count(*) from series_blocks where series_id=1",
			&[], |a| a.get(0)
		).unwrap();
		assert_eq!(count, 2);
	}

	#[test]
	fn stored_offset()
	{
		let (tmp,m) = n();
		{
			let m = m;
			eprintln!("created in {:?}", tmp.path());
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse1", "F").unwrap();
			{
				let mut items_to_insert = vec!();
				for x in 1..513
				{
					items_to_insert.push((Timestamp(x), (x) as f64));
				}
				txw.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();
			}
			txw.commit();
		}
		{
			let db = ::rusqlite::Connection::open(tmp.path().join("meta")).unwrap();
			let offset: i64 = db.query_row(
				"select offset from end_offset",
				&[], |a| a.get(0)
			).unwrap();
			assert_eq!(offset, 4096+8192);
		}

		{
			let mut m = Db::open(tmp.path().to_path_buf());
			m.start_merge_thread();
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse2", "F").unwrap();
			{
				let mut items_to_insert = vec!();
				for x in 1..513
				{
					items_to_insert.push((Timestamp(x), (x) as f64));
				}
				txw.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();
			}
			txw.commit();
		}

		{
			let db = ::rusqlite::Connection::open(tmp.path().join("meta")).unwrap();
			let offset: i64 = db.query_row(
				"select offset from end_offset",
				&[], |a| a.get(0)
			).unwrap();
			assert_eq!(offset, 4096+8192*2);
		}
		{
			let mut m = Db::open(tmp.path().to_path_buf());
			m.start_merge_thread();
			let txr = m.read_transaction();
			txr.commit();
		}

		{
			let db = ::rusqlite::Connection::open(tmp.path().join("meta")).unwrap();
			let offset: i64 = db.query_row(
				"select offset from end_offset",
				&[], |a| a.get(0)
			).unwrap();
			assert_eq!(offset, 4096+8192*2);
		}
	}

	#[test]
	fn restart_offset()
	{
		let (tmp,m) = n();
		{
			eprintln!("created in {:?}", tmp.path());
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse1", "F").unwrap();
			{
				let mut items_to_insert = vec!();
				for x in 1..513
				{
					items_to_insert.push((Timestamp(x), (x) as f64));
				}
				txw.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();
			}
			txw.commit();
			drop(n);
		}
		{
			let mut m = Db::open(tmp.path().to_path_buf());
			m.start_merge_thread();
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse2", "F").unwrap();
			{
				let mut items_to_insert = vec!();
				for x in 1..513
				{
					items_to_insert.push((Timestamp(x), (x) as f64));
				}
				txw.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();
			}
			txw.commit();
			drop(n);
		}

		let db = ::rusqlite::Connection::open(tmp.path().join("meta")).unwrap();
		let count: i64 = db.query_row(
			"select count(*) from series_blocks where offset=4096",
			&[], |a| a.get(0)
		).unwrap();
		assert_eq!(count, 1);
		let db = ::rusqlite::Connection::open(tmp.path().join("meta")).unwrap();
		let count: i64 = db.query_row(
			"select count(*) from series_blocks where offset=4096+8192",
			&[], |a| a.get(0)
		).unwrap();
		assert_eq!(count, 1);
	}

	#[test]
	fn read_direction_multi_block()
	{
		let (tmp,m) = n();
		eprintln!("created in {:?}", tmp.path());
		let mut tx = m.write_transaction();
		let h = tx.create_series("horse", "F").unwrap();
		let mut items_to_insert = vec!();
		for x in 10..=30000
		{
			items_to_insert.push((Timestamp(x*10), (x*10) as f64));
		}
		tx.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();

		let get =
			|ts: Timestamp, reverse: bool|
				-> Option<(Timestamp, f64)>
			{
				let mut v = vec![(h, "horse".to_string())];

				let mut out = None;

				tx.read_direction_multi(
					v.drain(..),
					ts,
					reverse,
					|_, ts, format, data|
					{
						if out.is_some() { panic!("two values"); }
						let mut o = ::std::io::Cursor::new(vec!());
						format.to_protocol_format(data, &mut o).unwrap();
						let o = String::from_utf8(o.into_inner()).unwrap();
						let v = o.parse().unwrap();
						out = Some((*ts, v));
					}
				);

				out
			};

		assert_eq!(get(Timestamp(1), false).unwrap(), (Timestamp(100), 100.0));
		assert_eq!(get(Timestamp(1000), false).unwrap(), (Timestamp(1000), 1000.0));
		assert_eq!(get(Timestamp(2555), false).unwrap(), (Timestamp(2560), 2560.0));
		assert_eq!(get(Timestamp(2550), false).unwrap(), (Timestamp(2550), 2550.0));
		assert!(get(Timestamp(5), true).is_none());
		assert_eq!(get(Timestamp(300009), true).unwrap(), (Timestamp(300000), 300000.0));
		assert_eq!(get(Timestamp(299999), true).unwrap(), (Timestamp(299990), 299990.0));
	}

	#[test]
	fn insertion_bulk()
	{
		let (tmp,m) = n();
		eprintln!("created in {:?}", tmp.path());

		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "F").unwrap();

		{
			let items_to_insert =
				[
					(Timestamp(1000),  1000.0),
					(Timestamp(1010),  1010.0),
					(Timestamp(1020),  1020.0),
				];
			txw.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();
		}
		{
			let items_to_insert =
				[
					(Timestamp(900),  900.0),
					(Timestamp(901),  901.0),
					(Timestamp(902),  902.0),
					(Timestamp(1011),  1011.0),
					(Timestamp(1012),  1012.0),
					(Timestamp(1030),  1030.0),
					(Timestamp(1031),  1031.0),
				];
			txw.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();
		}
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txw, h, 900, 2000)),
			"[(Timestamp(900), 900.0), (Timestamp(901), 901.0), \
			(Timestamp(902), 902.0), (Timestamp(1000), 1000.0), \
			(Timestamp(1010), 1010.0), (Timestamp(1011), 1011.0), \
			(Timestamp(1012), 1012.0), (Timestamp(1020), 1020.0), \
			(Timestamp(1030), 1030.0), (Timestamp(1031), 1031.0)]"
		);
	}

	#[test]
	fn insertion_twice()
	{
		let (_tmp,m) = n();

		let h;

		{
			let mut txw = m.write_transaction();
			h = txw.create_series("horse", "F").unwrap();
			let items_to_insert =
				[
					(Timestamp(1000),  1000.0),
					(Timestamp(1010),  1010.0),
					(Timestamp(1020),  1020.0),
				];
			txw.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();
			txw.commit();
		}
		{
			let mut txw = m.write_transaction();
			let items_to_insert =
				[
					(Timestamp(1030),  1030.0),
					(Timestamp(1040),  1040.0),
					(Timestamp(1050),  1050.0),
				];
			txw.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();
			txw.commit();
		}
		let txr = m.read_transaction();
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txr, h, 900, 2000)),
			"[(Timestamp(1000), 1000.0), (Timestamp(1010), 1010.0), \
			(Timestamp(1020), 1020.0), (Timestamp(1030), 1030.0), \
			(Timestamp(1040), 1040.0), (Timestamp(1050), 1050.0)]"
		);
	}

	fn create_three_blocks(h: u64, tx: &mut crate::metadata::Transaction)
	{
		{
			let items_to_insert =
				[
					(Timestamp(500),  500.0),
					(Timestamp(510),  510.0),
					(Timestamp(520),  520.0),
					(Timestamp(530),  530.0),
				];
			tx.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();
		}
		{
			let items_to_insert =
				[
					(Timestamp(400),  400.0),
					(Timestamp(410),  410.0),
					(Timestamp(420),  420.0),
					(Timestamp(430),  430.0),
				];
			tx.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();
		}
		{
			let items_to_insert =
				[
					(Timestamp(300),  300.0),
					(Timestamp(310),  310.0),
					(Timestamp(320),  320.0),
					(Timestamp(330),  330.0),
				];
			tx.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();
		}
	}

	#[test]
	fn erase_ranges1()
	{
		let (tmp,m) = n();
		eprintln!("created in {:?}", tmp.path());

		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "F").unwrap();
		create_three_blocks(h, &mut txw);

		txw.erase_range(h, Timestamp(400), Timestamp(499)).unwrap();
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txw, h, 0, 1000)),
			"[(Timestamp(300), 300.0), (Timestamp(310), 310.0), \
			(Timestamp(320), 320.0), (Timestamp(330), 330.0), \
			(Timestamp(500), 500.0), (Timestamp(510), 510.0), \
			(Timestamp(520), 520.0), (Timestamp(530), 530.0)]"
		);
	}
	#[test]
	fn erase_ranges2()
	{
		let (tmp,m) = n();
		eprintln!("created in {:?}", tmp.path());

		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "F").unwrap();

		create_three_blocks(h, &mut txw);
		txw.erase_range(h, Timestamp(410), Timestamp(420)).unwrap();
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txw, h, 0, 1000)),
			"[(Timestamp(300), 300.0), (Timestamp(310), 310.0), \
			(Timestamp(320), 320.0), (Timestamp(330), 330.0), \
			(Timestamp(400), 400.0), \
			(Timestamp(430), 430.0), \
			(Timestamp(500), 500.0), (Timestamp(510), 510.0), \
			(Timestamp(520), 520.0), (Timestamp(530), 530.0)]"
		);
	}
	#[test]
	fn erase_ranges3()
	{
		let (tmp,m) = n();
		eprintln!("created in {:?}", tmp.path());

		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "F").unwrap();

		create_three_blocks(h, &mut txw);
		txw.erase_range(h, Timestamp(400), Timestamp(400)).unwrap();
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txw, h, 0, 1000)),
			"[(Timestamp(300), 300.0), (Timestamp(310), 310.0), \
			(Timestamp(320), 320.0), (Timestamp(330), 330.0), \
			(Timestamp(410), 410.0), \
			(Timestamp(420), 420.0), (Timestamp(430), 430.0), \
			(Timestamp(500), 500.0), (Timestamp(510), 510.0), \
			(Timestamp(520), 520.0), (Timestamp(530), 530.0)]"
		);
	}

	#[test]
	fn erase_ranges4()
	{
		let (tmp,m) = n();
		eprintln!("created in {:?}", tmp.path());

		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "F").unwrap();

		create_three_blocks(h, &mut txw);
		txw.erase_range(h, Timestamp(420), Timestamp(510)).unwrap();
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txw, h, 0, 1000)),
			"[(Timestamp(300), 300.0), (Timestamp(310), 310.0), \
			(Timestamp(320), 320.0), (Timestamp(330), 330.0), \
			(Timestamp(400), 400.0), (Timestamp(410), 410.0), \
			(Timestamp(520), 520.0), (Timestamp(530), 530.0)]"
		);
	}
	#[test]
	fn erase_ranges5()
	{
		let (tmp,m) = n();
		eprintln!("created in {:?}", tmp.path());

		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "F").unwrap();

		create_three_blocks(h, &mut txw);
		txw.erase_range(h, Timestamp(310), Timestamp(520)).unwrap();
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txw, h, 0, 1000)),
			"[(Timestamp(300), 300.0), (Timestamp(530), 530.0)]"
		);
	}

	fn create_two_on(m: &Db)
	{
		{
			let mut txw = m.write_transaction();
			let h1 = txw.create_series("horse1", "u").unwrap();
			insert_val::<u32>(&mut txw, h1, Timestamp(1000), 101);
			insert_val::<u32>(&mut txw, h1, Timestamp(1001), 102);
			txw.commit();
		}
		{
			let mut txw = m.write_transaction();
			let h2 = txw.create_series("horse2", "F").unwrap();
			insert_val::<f32>(&mut txw, h2, Timestamp(1000), 201.0);
			insert_val::<f32>(&mut txw, h2, Timestamp(1001), 202.0);
			txw.commit();
		}
	}

	#[test]
	fn list_like()
	{
		let (_tmp,m) = n();
		create_two_on(&m);

		let txr = m.read_transaction();
		let mut count = 0usize;
		txr.series_like("horse%", |_,_| count +=1 ).unwrap();

		assert_eq!(count, 2);
	}

	#[test]
	fn dump_like()
	{
		let (_tmp,m) = n();
		create_two_on(&m);

		let txr = m.read_transaction();
		let mut s = String::new();

		txr.dump_series_like(
			"horse%", Timestamp(0), Timestamp(5000),
			|n, ts, _fmt, data|
			{
				s += &format!("{} {} {:?}\n", n, ts.0, data);
			}
		).unwrap();
		assert_eq!(s, "\
		horse1 1000 [0, 0, 0, 101]\n\
		horse1 1001 [0, 0, 0, 102]\n\
		horse2 1000 [64, 105, 32, 0, 0, 0, 0, 0]\n\
		horse2 1001 [64, 105, 64, 0, 0, 0, 0, 0]\n");
	}

	#[test]
	fn dump_like_2blk()
	{
		let (_tmp,m) = n();
		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "f").unwrap();

		create_three_blocks(h, &mut txw);
		txw.commit();
		{
			let db = ::rusqlite::Connection::open(_tmp.path().join("meta")).unwrap();
			db.execute(
				"update series_blocks set generation=2 where rowid=2",
				&[],
			).unwrap();
			db.execute(
				"update series_blocks set generation=3 where rowid=3",
				&[],
			).unwrap();
		}

		let txr = m.read_transaction();
		let mut s = String::new();

		txr.dump_series_like(
			"horse", Timestamp(430), Timestamp(510),
			|n, ts, _fmt, data|
			{
				s += &format!("{} {} {:?}\n", n, ts.0, data);
			}
		).unwrap();
		assert_eq!(s, "\
		horse 430 [67, 215, 0, 0]\n\
		horse 500 [67, 250, 0, 0]\n\
		horse 510 [67, 255, 0, 0]\n");

		let mut n_recs = 0;
		txr.dump_series_like(
			"horse", Timestamp(0), Timestamp(1000),
			|n, ts, _fmt, data|
			{
				n_recs += 1;
			}
		).unwrap();
		assert_eq!(n_recs, 12);
	}

	#[test]
	fn two_tx()
	{
		let (_tmp,m) = n();
		create_two_on(&m);

		let txr = m.read_transaction();
		let h1 = txr.series_id("horse1").unwrap();
		let h2 = txr.series_id("horse2").unwrap();
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txr, h1, 1000, 1001)),
			"[(Timestamp(1000), 101.0), (Timestamp(1001), 102.0)]"
		);
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txr, h2, 1000, 1001)),
			"[(Timestamp(1000), 201.0), (Timestamp(1001), 202.0)]"
		);
	}

	#[test]
	fn two_tx_reopen()
	{
		let (tmp,m) = n();
		create_two_on(&m);
		drop(m);

		let mut m = Db::open(tmp.path().to_path_buf());
		m.start_merge_thread();
		let txr = m.read_transaction();
		let h1 = txr.series_id("horse1").unwrap();
		let h2 = txr.series_id("horse2").unwrap();
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txr, h1, 1000, 1001)),
			"[(Timestamp(1000), 101.0), (Timestamp(1001), 102.0)]"
		);
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txr, h2, 1000, 1001)),
			"[(Timestamp(1000), 201.0), (Timestamp(1001), 202.0)]"
		);
	}

	#[test]
	fn discard_disk_wal()
	{
		let (tmp,m) = n();
		{
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_val::<f64>(&mut txw, h, Timestamp(1000), 42.0);
			insert_val::<f64>(&mut txw, h, Timestamp(1001), 43.0);
			// don't commit
		}
		drop(m);
		{
			let mut m = Db::open(tmp.path().to_path_buf());
			m.start_merge_thread();
			let txr = m.read_transaction();
			assert!(txr.series_id("horse").is_none());
		}
	}

	#[test]
	#[should_panic]
	fn write_should_panic()
	{
		let (_tmp,m) = n();
		let mut txr = m.read_transaction();
		txr.create_series("horse", "F").unwrap();
	}

	#[test]
	#[should_panic]
	fn duplicate_seq()
	{
		let (_tmp,m) = n();
		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "F").unwrap();
		insert_val::<f64>(&mut txw, h, Timestamp(1000), 42.0);
		insert_val::<f64>(&mut txw, h, Timestamp(1000), 43.0);
		txw.commit();
	}

	#[test]
	#[should_panic]
	fn duplicate_at_once()
	{
		let (_tmp,m) = n();
		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "F").unwrap();
		txw.insert_into_series(
			h,
			generator_f64(&[
				(Timestamp(1000), 42.0),
				(Timestamp(1000), 43.0),
			])
		).unwrap();
	}

	#[test]
	#[should_panic]
	fn backwards_illegal()
	{
		// this will one day be permitted
		let (_tmp,m) = n();
		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "F").unwrap();
		txw.insert_into_series(
			h,
			generator_f64(&[
				(Timestamp(1000), 42.0),
				(Timestamp(999), 42.0),
			])
		).unwrap();
	}
	#[test]
	fn backwards_two()
	{
		let (_tmp,m) = n();
		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "F").unwrap();
		insert_val::<f64>(&mut txw, h, Timestamp(1000), 42.0);
		insert_val::<f64>(&mut txw, h, Timestamp(998), 40.0);
		insert_val::<f64>(&mut txw, h, Timestamp(999), 41.0);
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txw, h, 999, 1001)),
			"[(Timestamp(999), 41.0), (Timestamp(1000), 42.0)]"
		);
	}

	#[test]
	fn backwards_break()
	{
		let (_tmp,m) = n();
		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "F").unwrap();
		insert_val::<f64>(&mut txw, h, Timestamp(1000), 40.0);
		insert_val::<f64>(&mut txw, h, Timestamp(1002), 42.0);
		insert_val::<f64>(&mut txw, h, Timestamp(1001), 41.0);
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txw, h, 1000, 1002)),
			"[(Timestamp(1000), 40.0), (Timestamp(1001), 41.0), (Timestamp(1002), 42.0)]"
		);
	}

	#[test]
	fn blocks_exact_file()
	{
		let (tmp,mut m) = n();
		{
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_val::<f64>(&mut txw, h, Timestamp(42), 42.0);
			insert_val::<f64>(&mut txw, h, Timestamp(43), 43.0);
			txw.commit();
		}
		m.start_merge_thread();
		drop(m);

		use std::io::Seek;
		use std::io::Read;
		let mut f = ::std::fs::File::open(tmp.path().join("blocks")).unwrap();
		f.seek(::std::io::SeekFrom::Start(4096)).unwrap();

		let mut a = vec![];
		a.resize(512, 0u8);
		f.read(&mut a).unwrap();
		assert_eq!(
			&a[0..27],
			&[
				0, 0, 0, 0, 0, 0, 0, 42, 64, 69, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 43, 64, 69, 128
			]
		);
	}

	#[test]
	fn rollback1()
	{
		let (_tmp,m) = n();
		{
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_val::<f64>(&mut txw, h, Timestamp(42), 42.0);
		}
		{
			let txr = m.read_transaction();
			assert!(txr.series_id("horse").is_none());
		}
	}
	#[test]
	fn rollback2()
	{
		let (_tmp,m) = n();
		{
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_val::<f64>(&mut txw, h, Timestamp(42), 42.0);
			txw.commit();
		}
		{
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_val::<f64>(&mut txw, h, Timestamp(43), 43.0);
		}
		{
			let txr = m.read_transaction();
			let h1 = txr.series_id("horse").unwrap();
			assert_eq!(
				format!("{:?}", read_vals::<f64>(&txr, h1, 0, 1000)),
				"[(Timestamp(42), 42.0)]"
			);
		}
	}

	#[test]
	fn write_queued_parallel()
	{
		let (_tmp, mm) = n();
		use std::sync::Arc;

		let mm = Arc::new(mm);
		let m = mm.clone();

		let j1 = std::thread::spawn(
			move ||
			{
				let mut txw = m.write_transaction();
				eprintln!("{}", txw.next_offset.get());
				let h1 = txw.create_series("h1", "F").unwrap();
				insert_val::<f64>(&mut txw, h1, Timestamp(42), 42.0);
				insert_val::<f64>(&mut txw, h1, Timestamp(43), 43.0);
				txw.commit();
			}
		);
		let m = mm.clone();
		let j2 = std::thread::spawn(
			move ||
			{
				let mut txw = m.write_transaction();
				eprintln!("{}", txw.next_offset.get());
				let h2 = txw.create_series("h2", "F").unwrap();
				insert_val::<f64>(&mut txw, h2, Timestamp(45), 45.0);
				insert_val::<f64>(&mut txw, h2, Timestamp(46), 46.0);
				txw.commit();
			}
		);

		j1.join().unwrap();
		j2.join().unwrap();

		let txr = mm.read_transaction();
		let h1 = txr.series_id("h1").unwrap();
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txr, h1, 0, 1000)),
			"[(Timestamp(42), 42.0), (Timestamp(43), 43.0)]"
		);
		let h2 = txr.series_id("h2").unwrap();
		assert_eq!(
			format!("{:?}", read_vals::<f64>(&txr, h2, 0, 1000)),
			"[(Timestamp(45), 45.0), (Timestamp(46), 46.0)]"
		);

	}

	fn blocks_10k_knives_basic(do_restart: bool)
	{
		let (tmp,mut m) = n();
		if !do_restart
			{ m.start_merge_thread(); }
		{
			let mut txw = m.write_transaction();
			for idx in 1..10001
			{
				let h = txw.create_series(&format!("k{}", idx), "F").unwrap();
				insert_val::<f64>(&mut txw, h, Timestamp(100), 100.0);
				insert_val::<f64>(&mut txw, h, Timestamp(101), 101.0);
				insert_val::<f64>(&mut txw, h, Timestamp(102), 102.0);
				insert_val::<f64>(&mut txw, h, Timestamp(103), 103.0);
				insert_val::<f64>(&mut txw, h, Timestamp(104), 104.0);
				insert_val::<f64>(&mut txw, h, Timestamp(105), 105.0);
			}
			txw.commit();
		}
		{
			let mut txw = m.write_transaction();
			for h in 1..10001
			{
				insert_val::<f64>(&mut txw, h, Timestamp(106), 106.0);
			}
			txw.commit();
		}

		if do_restart
		{
			let m2 = Db::open(tmp.path().to_path_buf());
			m = m2;
		}

		{
			let txr = m.read_transaction();
			for h in 1..10001
			{
				let v = read_vals::<f64>(&txr, h, 100, 106);
				assert_eq!(v.get(0).map(|(_,v)| *v), Some(100.0));
				assert_eq!(v.get(1).map(|(_,v)| *v), Some(101.0));
				assert_eq!(v.get(2).map(|(_,v)| *v), Some(102.0));
				assert_eq!(v.get(3).map(|(_,v)| *v), Some(103.0));
				assert_eq!(v.get(4).map(|(_,v)| *v), Some(104.0));
				assert_eq!(v.get(5).map(|(_,v)| *v), Some(105.0));
				assert_eq!(v.get(6).map(|(_,v)| *v), Some(106.0));
				assert_eq!(v.get(7).map(|(_,v)| *v), None);
			}
		}
	}
	#[test]
	fn blocks_10k_knives_restart()
	{
		blocks_10k_knives_basic(true);
	}
	#[test]
	fn blocks_10k_knives_atonce()
	{
		blocks_10k_knives_basic(false);
	}

	#[test]
	fn blocks_100_knives()
	{
		let (_tmp,m) = n();
		for idx in 0..100
		{
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "u").unwrap();
			insert_val::<u32>(&mut txw, h, Timestamp(100+idx as u64), 100+idx as u32);
			txw.commit();
		}
		{
			let txr = m.read_transaction();
			let h = txr.series_id("horse").unwrap();
			let vals = read_vals::<u32>(&txr, h, 0, 200);
			for idx in 0..100
			{
				assert_eq!(vals[idx].1, 100+idx as u32);
			}
		}
	}

	#[test]
	fn two_blocks_then_duplicate()
	{
		let (tmp,m) = n();
		let count_blocks =
			|| -> i64
			{
				let db = ::rusqlite::Connection::open(tmp.path().join("meta")).unwrap();
				db.query_row(
					"select count(*) from series_blocks",
					&[],
					|a| a.get(0)
				).unwrap()
			};

		let mut txw = m.write_transaction();
		let h = txw.create_series("horse", "u").unwrap();
		for i in 1..=512
			{ insert_val::<u32>(&mut txw, h, Timestamp(i), i as u32); }

		insert_val::<u32>(&mut txw, h, Timestamp(1000), 500);
		insert_val::<u32>(&mut txw, h, Timestamp(1001), 500);
		insert_val::<u32>(&mut txw, h, Timestamp(1002), 500);
		txw.commit();
		assert_eq!(count_blocks(), 2);
		let mut txw = m.write_transaction();
		insert_val::<u32>(&mut txw, h, Timestamp(998), 500);
		insert_val::<u32>(&mut txw, h, Timestamp(999), 500);

		let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
			|| insert_val::<u32>(&mut txw, h, Timestamp(1000), 500)
		));
		assert!(r.is_err());
    }

	#[test]
	fn generation_increases()
	{
		let (tmp, m) = n();

		let read_generation =
			|| -> i64
			{
				let db = ::rusqlite::Connection::open(tmp.path().join("meta")).unwrap();
				db.query_row(
					"select max(generation) from series_blocks where \
						series_id=1",
					&[],
					|a| a.get(0)
				).unwrap()
			};

		{
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_val::<f64>(&mut txw, h, Timestamp(42), 42.0);
			txw.commit();
		}
		assert_eq!(read_generation(), 1);
		{
			let mut txw = m.write_transaction();
			let h = txw.series_id("horse").unwrap();
			insert_val::<f64>(&mut txw, h, Timestamp(43), 43.0);
			txw.commit();
		}
		assert_eq!(read_generation(), 2);
		{
			let mut txw = m.write_transaction();
			let h = txw.series_id("horse").unwrap();
			insert_val::<f64>(&mut txw, h, Timestamp(44), 44.0);
			txw.commit();
		}
		assert_eq!(read_generation(), 3);

	}
}
