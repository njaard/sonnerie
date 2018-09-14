extern crate antidote;

use metadata::Metadata;
pub use metadata::Transaction;
use wal::MemoryWal;
use blocks::Blocks;
use disk_wal::{DiskWalWriter,DiskWalReader};
use block_file::BlockFile;

use ::std::path::{Path,PathBuf};
use ::std::collections::VecDeque;

use ::std::sync::Arc;
use metadata::RwLock;
use self::antidote::{Mutex,Condvar};

pub use metadata::Timestamp;

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
	blocks: Arc<RwLock<Blocks>>,
	merge_state: Arc<(Mutex<MergeState>, Condvar)>,

	merging_thread: Option<::std::thread::JoinHandle<()>>,

	max_generation: Mutex<u64>,
	next_offset: Mutex<u64>,
}

impl Db
{
	pub fn open(path: PathBuf) -> Db
	{
		let metadatapath = path.join("meta");

		let mut wal = MemoryWal::new();

		let unflushed_wal_files =
			read_unflushed_wal_files(&path, &mut wal);

		let blockfilename = path.join("blocks");
		let file = BlockFile::new(&blockfilename);

		let blocks = Arc::new(RwLock::new(Blocks::new(file, wal)));

		let mut max_generation;

		{
			let metadata = Metadata::new(4096, &metadatapath, blocks.clone());
			max_generation = metadata.last_generation();
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

		let merging_thread;
		{
			let blocks = blocks.clone();
			let unflushed_wal_files = unflushed_wal_files.clone();
			let merge_state = merge_state.clone();
			merging_thread =
				::std::thread::spawn(
					move ||
					{
						let mut exit = false;
						let mut previously_merged_to: u64 = 0;

						while !exit
						{
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

							::wal::merge(
								&blocks.read().wal,
								&blocks.read().file,
							);

							{
								previously_merged_to = now_merging;
								blocks.read().file.sync();

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
				);
		};

		Db
		{
			merge_state: merge_state,
			path: path,
			metadatapath: metadatapath,
			unflushed_wal_files: unflushed_wal_files,
			blocks: blocks,
			merging_thread: Some(merging_thread),
			max_generation: Mutex::new(max_generation),
			next_offset: Mutex::new(4096),
		}
	}

	pub fn read_transaction(&self) -> Transaction
	{
		let metadata = Metadata::new(0, &self.metadatapath, self.blocks.clone());
		metadata.as_read_transaction()
	}

	pub fn write_transaction(&self) -> Transaction
	{
		let g = (*self.max_generation.lock())+1;

		let (walwriter, file) = DiskWalWriter::new(g, &self.path);

		self.blocks.write()
			.set_disk_wal(walwriter);
		let metadata = Metadata::open(
			*self.next_offset.lock(), &self.metadatapath, self.blocks.clone()
		);

		self.unflushed_wal_files.lock().push_back((g, file));
		metadata.as_write_transaction(
			g,
			self,
		)
	}

	pub fn committing(&self, committed_metadata: &Metadata)
	{
		*self.max_generation.lock() += 1;
		*self.next_offset.lock() = committed_metadata.next_offset.get();

		{
			let mut l = self.merge_state.0.lock();
			l.merging_min = committed_metadata.generation;
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

		self.merging_thread.take().unwrap().join().unwrap();
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
	extern crate tempfile;
	use ::db::{Db,Timestamp};

	fn n() -> tempfile::TempDir
	{
		tempfile::TempDir::new().unwrap()
	}

	fn read_f64s(tx: &::metadata::Transaction, series_id: u64, timestamp1: u64, timestamp2: u64)
		-> Vec<(Timestamp, f64)>
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
	fn insert_f64(
		tx: &mut ::metadata::Transaction, series_id: u64, ts: Timestamp, value: f64,
	)
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
					).unwrap();
					Some(ts)
				}
				else
				{
					None
				}
			}
		).unwrap();
	}


	#[test]
	fn dbmeta1()
	{
		let tmp = n();
		eprintln!("created in {:?}", tmp.path());
		let m = Db::open(tmp.path().to_path_buf());
		{
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_f64(&mut txw, h, Timestamp(1000), 42.0);
			insert_f64(&mut txw, h, Timestamp(1001), 43.0);

			assert_eq!(
				format!("{:?}", read_f64s(&txw, h, 1000, 1001)),
				"[(Timestamp(1000), 42.0), (Timestamp(1001), 43.0)]"
			);

			let txr = m.read_transaction();
			assert_eq!(
				format!("{:?}", read_f64s(&txr, h, 1000, 1001)),
				"[]"
			);

			txw.commit();
			assert_eq!(
				format!("{:?}", read_f64s(&txr, h, 1000, 1001)),
				"[]"
			);
			let txr2 = m.read_transaction();
			assert_eq!(
				format!("{:?}", read_f64s(&txr2, h, 1000, 1001)),
				"[(Timestamp(1000), 42.0), (Timestamp(1001), 43.0)]"
			);

		}
	}

	#[test]
	fn save_disk_wal()
	{
		let tmp = n();
		{
			let m = Db::open(tmp.path().to_path_buf());
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_f64(&mut txw, h, Timestamp(1000), 42.0);
			insert_f64(&mut txw, h, Timestamp(1001), 43.0);
			txw.commit();
		}
		{
			let m = Db::open(tmp.path().to_path_buf());
			let txr = m.read_transaction();
			let h = txr.series_id("horse").unwrap();
			assert_eq!(
				format!("{:?}", read_f64s(&txr, h, 1000, 1001)),
				"[(Timestamp(1000), 42.0), (Timestamp(1001), 43.0)]"
			);
		}
	}

	#[test]
	fn two_series()
	{
		let tmp = n();
		{
			let m = Db::open(tmp.path().to_path_buf());
			let mut txw = m.write_transaction();
			let h1 = txw.create_series("horse1", "F").unwrap();
			let h2 = txw.create_series("horse2", "F").unwrap();
			insert_f64(&mut txw, h1, Timestamp(1000), 101.0);
			insert_f64(&mut txw, h1, Timestamp(1001), 102.0);
			insert_f64(&mut txw, h2, Timestamp(1000), 201.0);
			insert_f64(&mut txw, h2, Timestamp(1001), 202.0);
			assert_eq!(
				format!("{:?}", read_f64s(&txw, h1, 1000, 1001)),
				"[(Timestamp(1000), 101.0), (Timestamp(1001), 102.0)]"
			);
			assert_eq!(
				format!("{:?}", read_f64s(&txw, h2, 1000, 1001)),
				"[(Timestamp(1000), 201.0), (Timestamp(1001), 202.0)]"
			);
		}
	}

	#[test]
	fn select_weird_ranges()
	{
		let tmp = n();
		{
			let m = Db::open(tmp.path().to_path_buf());
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_f64(&mut txw, h, Timestamp(1000), 1.0);
			insert_f64(&mut txw, h, Timestamp(1001), 2.0);
			insert_f64(&mut txw, h, Timestamp(1002), 3.0);
			insert_f64(&mut txw, h, Timestamp(1003), 4.0);
			assert_eq!(
				format!("{:?}", read_f64s(&txw, h, 1001, 1003)),
				"[(Timestamp(1001), 2.0), (Timestamp(1002), 3.0), (Timestamp(1003), 4.0)]"
			);
			assert_eq!(
				format!("{:?}", read_f64s(&txw, h, 1001, 1001)),
				"[(Timestamp(1001), 2.0)]"
			);
		}
	}

	#[test]
	fn boundary_crossing()
	{
		let tmp = n();
		{
			let m = Db::open(tmp.path().to_path_buf());
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			for x in 1..30000
			{
				insert_f64(&mut txw, h, Timestamp(x), x as f64);
			}
			assert_eq!(
				format!("{:?}", read_f64s(&txw, h, 10012, 10012)),
				"[(Timestamp(10012), 10012.0)]"
			);

			for start in (1..25000).step_by(11)
			{
				for len in 1..17
				{
					let s = read_f64s(&txw, h, start, start+len-1);
					assert_eq!(s.len(), len as usize);
					for (idx,a) in s.iter().enumerate()
					{
						assert_eq!((a.0).0 as u64, start+idx as u64);
						assert_eq!(a.1, (start+idx as u64) as f64);
					}
				}
			}
		}
	}

	fn generator_f64<'q>(items: &'q [(Timestamp, f64)])
		-> impl 'q + FnMut(&::row_format::RowFormat, &mut Vec<u8>)
			-> Option<Timestamp>
	{
		let mut i = items.iter();

		let f = move |format: &::row_format::RowFormat, data: &mut Vec<u8>|
			-> Option<Timestamp>
		{
			if let Some((ts,val)) = i.next()
			{
				let formatted = format!("{}", val);
				format.to_stored_format(ts, &formatted, data).unwrap();
				Some(*ts)
			}
			else
			{
				None
			}
		};
		f
	}


	#[test]
	fn boundary_crossing_bulk_load()
	{
		let tmp = n();
		eprintln!("created in {:?}", tmp.path());
		{
			let m = Db::open(tmp.path().to_path_buf());
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			let mut items_to_insert = vec!();
			for x in 1..30000
			{
				items_to_insert.push((Timestamp(x), (x*10) as f64));
			}
			txw.insert_into_series(h, generator_f64(&items_to_insert)).unwrap();
			assert_eq!(
				format!("{:?}", read_f64s(&txw, h, 10012, 10012)),
				"[(Timestamp(10012), 100120.0)]"
			);

			for start in (1..25000).step_by(11)
			{
				for len in 1..17
				{
					let s = read_f64s(&txw, h, start, start+len-1);
					assert_eq!(s.len(), len as usize);
					for (idx,a) in s.iter().enumerate()
					{
						assert_eq!(a.0 .0 as u64, start+idx as u64);
						assert_eq!(a.1, ((start+idx as u64)*10) as f64);
					}
				}
			}
		}
	}

	fn create_two_on(m: &Db)
	{
		{
			let mut txw = m.write_transaction();
			let h1 = txw.create_series("horse1", "F").unwrap();
			insert_f64(&mut txw, h1, Timestamp(1000), 101.0);
			insert_f64(&mut txw, h1, Timestamp(1001), 102.0);
			txw.commit();
		}
		{
			let mut txw = m.write_transaction();
			let h2 = txw.create_series("horse2", "F").unwrap();
			insert_f64(&mut txw, h2, Timestamp(1000), 201.0);
			insert_f64(&mut txw, h2, Timestamp(1001), 202.0);
			txw.commit();
		}
	}

	#[test]
	fn dump_some()
	{
		let tmp = n();
		let m = Db::open(tmp.path().to_path_buf());
		create_two_on(&m);

		let txr = m.read_transaction();
		let mut count = 0usize;
		txr.series_like("horse%", |_,_| count +=1 );

		assert_eq!(count, 2);
	}

	#[test]
	fn two_tx()
	{
		let tmp = n();
		let m = Db::open(tmp.path().to_path_buf());
		create_two_on(&m);

		let txr = m.read_transaction();
		let h1 = txr.series_id("horse1").unwrap();
		let h2 = txr.series_id("horse2").unwrap();
		assert_eq!(
			format!("{:?}", read_f64s(&txr, h1, 1000, 1001)),
			"[(Timestamp(1000), 101.0), (Timestamp(1001), 102.0)]"
		);
		assert_eq!(
			format!("{:?}", read_f64s(&txr, h2, 1000, 1001)),
			"[(Timestamp(1000), 201.0), (Timestamp(1001), 202.0)]"
		);
	}

	#[test]
	fn two_tx_reopen()
	{
		let tmp = n();
		{
			let m = Db::open(tmp.path().to_path_buf());
			create_two_on(&m);
		}

		let m = Db::open(tmp.path().to_path_buf());
		let txr = m.read_transaction();
		let h1 = txr.series_id("horse1").unwrap();
		let h2 = txr.series_id("horse2").unwrap();
		assert_eq!(
			format!("{:?}", read_f64s(&txr, h1, 1000, 1001)),
			"[(Timestamp(1000), 101.0), (Timestamp(1001), 102.0)]"
		);
		assert_eq!(
			format!("{:?}", read_f64s(&txr, h2, 1000, 1001)),
			"[(Timestamp(1000), 201.0), (Timestamp(1001), 202.0)]"
		);
	}

	#[test]
	fn discard_disk_wal()
	{
		let tmp = n();
		{
			let m = Db::open(tmp.path().to_path_buf());
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_f64(&mut txw, h, Timestamp(1000), 42.0);
			insert_f64(&mut txw, h, Timestamp(1001), 43.0);
			// don't commit
		}
		{
			let m = Db::open(tmp.path().to_path_buf());
			let txr = m.read_transaction();
			assert!(txr.series_id("horse").is_none());
		}
	}

	#[test]
	#[should_panic]
	fn write_should_panic()
	{
		let tmp = n();
		{
			let m = Db::open(tmp.path().to_path_buf());
			let mut txr = m.read_transaction();
			txr.create_series("horse", "F").unwrap();
		}
	}

	#[test]
	#[should_panic]
	fn duplicate_seq()
	{
		let tmp = n();
		{
			let m = Db::open(tmp.path().to_path_buf());
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_f64(&mut txw, h, Timestamp(1000), 42.0);
			insert_f64(&mut txw, h, Timestamp(1000), 43.0);
			txw.commit();
		}
	}

	#[test]
	#[should_panic]
	fn duplicate_at_once()
	{
		let tmp = n();
		{
			let m = Db::open(tmp.path().to_path_buf());
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
	}

	#[test]
	#[should_panic]
	fn backwards_illegal()
	{
		// this will one day be permitted
		let tmp = n();
		{
			let m = Db::open(tmp.path().to_path_buf());
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
	}
	#[test]
	#[should_panic]
	fn backwards_illegal_two()
	{
		// this will one day be permitted
		let tmp = n();
		{
			let m = Db::open(tmp.path().to_path_buf());
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_f64(&mut txw, h, Timestamp(1000), 42.0);
			insert_f64(&mut txw, h, Timestamp(999), 42.0);
		}
	}

	#[test]
	fn blocks_exact_file()
	{
		let tmp = n();
		{
			let m = Db::open(tmp.path().to_path_buf());
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_f64(&mut txw, h, Timestamp(42), 42.0);
			insert_f64(&mut txw, h, Timestamp(43), 43.0);
			txw.commit();
		}

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
	fn generation_increases()
	{
		let tmp = n();

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

		let m = Db::open(tmp.path().to_path_buf());
		{
			let mut txw = m.write_transaction();
			let h = txw.create_series("horse", "F").unwrap();
			insert_f64(&mut txw, h, Timestamp(42), 42.0);
			txw.commit();
		}
		assert_eq!(read_generation(), 1);
		{
			let mut txw = m.write_transaction();
			let h = txw.series_id("horse").unwrap();
			insert_f64(&mut txw, h, Timestamp(43), 43.0);
			txw.commit();
		}
		assert_eq!(read_generation(), 2);
		{
			let mut txw = m.write_transaction();
			let h = txw.series_id("horse").unwrap();
			insert_f64(&mut txw, h, Timestamp(44), 44.0);
			txw.commit();
		}
		assert_eq!(read_generation(), 3);

	}
}
