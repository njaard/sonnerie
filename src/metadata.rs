extern crate rusqlite;
extern crate byteorder;
extern crate antidote;
extern crate libc;

#[derive(Debug,Clone,Copy,PartialEq,PartialOrd)]
pub struct Timestamp(pub u64);

use ::row_format::{parse_row_format, RowFormat};
use ::db::Db;
use ::blocks::Blocks;
use self::byteorder::{ByteOrder, BigEndian};
use std::path::Path;

use std::sync::Arc;
pub use self::antidote::RwLock;
pub use self::antidote::Mutex;
use std::cell::Cell;

/// Maintain all the information needed to locate data
/// One of these is opened per transaction/thread
pub struct Metadata
{
	db: rusqlite::Connection,
	blocks: Arc<Blocks>,
	blocks_raw_fd: ::std::os::unix::io::RawFd,
	pub next_offset: Cell<u64>,
	pub generation: u64,
}

impl Metadata
{
	/// open an existing database.
	///
	/// `next_offset` is the end of the block data where new blocks are created
	/// `f` is the filename of the existing metadata file
	/// `blocks` is shared between threads
	pub fn open(next_offset: u64, f: &Path, blocks: Arc<Blocks>)
		-> Metadata
	{
		let db = rusqlite::Connection::open_with_flags(
			f,
			rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
				| rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
		).unwrap();
		db.execute_batch("PRAGMA case_sensitive_like=ON;").unwrap();
		db.execute_batch("PRAGMA busy_timeout = 7200000;").unwrap();

		let fd = blocks.as_raw_fd();
		Metadata
		{
			db: db,
			next_offset: Cell::new(next_offset),
			blocks: blocks,
			blocks_raw_fd: fd,
			generation: 1,
		}
	}

	/// open or create a metadata file.
	///
	/// This is called only once at startup
	pub fn new(f: &Path, blocks: Arc<Blocks>)
		-> Metadata
	{
		let db = rusqlite::Connection::open_with_flags(
			f,
			rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
				| rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
				| rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
		).unwrap();
		db.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
		db.execute_batch("PRAGMA case_sensitive_like=ON;").unwrap();
		db.execute_batch("PRAGMA busy_timeout = 600000;").unwrap();

		db.execute_batch(
			"
				begin;
				create table if not exists schema_version (
					-- the version of the schema (for upgrading)
					version integer primary key not null
				);

				create table if not exists series (
					-- each series gets a numeric id
					series_id integer primary key autoincrement,
					-- the string that the user refers to this series by
					name text,
					-- which transaction did this appear in
					-- (this series is not visible to transactions
					-- that predate this generation)
					generation integer,
					format text
				);
				create table if not exists end_offset (offset);

				create index if not exists series_name on series (name collate binary);
				create index if not exists series_gen on series (generation);

				-- which blocks are associated with which series
				create table if not exists series_blocks (
					series_id integer,
					-- when this block last changed (for backup)
					generation integer,
					first_timestamp integer,
					last_timestamp integer,
					offset integer,
					capacity integer,
					size integer,
					constraint series_ts primary key (series_id, first_timestamp)
				);
				commit;
			"
		).unwrap();

		let next_offset: i64 = db.query_row(
			"select offset from end_offset limit 1",
			&[],
			|r| r.get(0)
		).unwrap_or_else(
			|_|
			{
				let next_offset: Option<i64> = db.query_row(
					"select max(offset+capacity) from series_blocks",
					&[],
					|r| r.get(0)
				).unwrap();
				next_offset.unwrap_or(4096)
			}
		);
		let next_offset = next_offset as u64;

		let fd = blocks.as_raw_fd();
		Metadata
		{
			db: db,
			next_offset: Cell::new(next_offset),
			blocks: blocks,
			blocks_raw_fd: fd,
			generation: 1,
		}
	}

	/// Called on startup to determine what generation the db is at
	pub fn last_generation(&self)
		-> u64
	{
		let g: i64 = self.db.query_row(
			"select generation from series order by generation desc limit 1",
			&[],
			|r| r.get(0)
		).unwrap_or(0);
		g as u64
	}

	/// Starts a transaction and converts me to a Transaction
	pub fn as_read_transaction(self)
		-> Transaction<'static>
	{
		self.db.execute("begin", &[]).unwrap();
		Transaction
		{
			metadata: self,
			writing: false,
			committed: false,
			finishing_on: None,
		}
	}

	/// Starts a transaction and converts me to a writable Transaction
	pub fn as_write_transaction<'db>(
		mut self,
		new_generation: u64,
		finishing_on: &'db Db,
	)
		-> Transaction<'db>
	{
		self.db.execute("begin immediate", &[]).unwrap();
		self.generation = new_generation;
		Transaction
		{
			metadata: self,
			writing: true,
			committed: false,
			finishing_on: Some(finishing_on)
		}
	}
}

pub struct Transaction<'db>
{
	metadata: Metadata,
	writing: bool,
	committed: bool,
	finishing_on: Option<&'db Db>,
}

impl<'db> Transaction<'db>
{
	/// Gets the blocks associated with a range of timestamps
	fn blocks_for_range(
		&self,
		series_id: u64,
		first_ts: Timestamp,
		last_ts: Timestamp,
	) -> Vec<Block>
	{
		let mut s = self.metadata.db.prepare_cached("
			select
				first_timestamp,
				last_timestamp,
				offset,
				capacity,
				size
			from series_blocks
			where
				series_id=? and
				? >= first_timestamp AND last_timestamp >= ?
		").unwrap();

		let mut rows = s.query(&[
			&(series_id as i64),
			&last_ts.to_sqlite(),
			&first_ts.to_sqlite(),
		]).unwrap();

		let mut blocks = vec!();
		while let Some(row) = rows.next()
		{
			let row = row.unwrap();
			let b = Block
			{
				first_timestamp: Timestamp::from_sqlite(row.get(0)),
				last_timestamp: Timestamp::from_sqlite(row.get(1)),
				offset: row.get::<_,i64>(2) as u64,
				capacity: row.get::<_,i64>(3) as u64,
				size: row.get::<_,i64>(4) as u64,
			};
			blocks.push( b );
		}
		blocks
	}

	fn series_format(&self, series_id: u64) -> Box<RowFormat>
	{
		let mut c = self.metadata.db.prepare_cached(
			"select format from series where series_id=?"
		).unwrap();

		let v: String = c.query(&[&(series_id as i64)]).unwrap()
			.next()
			.map(|e| e.unwrap().get(0))
			.unwrap();

		let f = parse_row_format(&v);
		f
	}

	pub fn series_format_string(&self, name: &str)
		-> Option<String>
	{
		let mut c = self.metadata.db.prepare_cached(
			"select format from series where name=?"
		).unwrap();

		let v = c.query(&[&name]).unwrap()
			.next()
			.map(|e| e.unwrap().get(0));
		v
	}

	/// creates a new series if necessary
	///
	/// Returns its ID, or None if the format doesn't match
	pub fn create_series(
		&mut self,
		name: &str,
		format: &str
	) -> Option<u64>
	{
		if !self.writing
			{ panic!("attempt to write in a read-only transaction"); }

		let mut q = self.metadata.db.prepare_cached(
			"select series_id,format from series where name=?"
		).unwrap();
		let mut row = q.query(&[&name]).unwrap();
		if let Some(row) = row.next()
		{
			let row = row.unwrap();
			let id: i64 = row.get(0);
			let stored_format: String = row.get(1);
			if stored_format != format
			{
				return None;
			}
			return Some(id as u64);
		}

		self.metadata.db.execute(
			"insert into series (name, generation, format)
				values (?, ?, ?)
			",
			&[
				&name,
				&(self.metadata.generation as i64),
				&format,
			]
		).unwrap();

		Some(self.metadata.db.last_insert_rowid() as u64)
	}

	/// Returns a series's ID
	pub fn series_id(
		&self,
		name: &str
	) -> Option<u64>
	{
		let mut c = self.metadata.db.prepare_cached(
			"select series_id from series where name=?"
		).unwrap();

		let v = c.query(&[&name]).unwrap()
			.next()
			.map(|e| e.unwrap().get::<_,i64>(0) as u64);
		v
	}

	/// return all of the series IDs that are SQL-like
	/// this string
	pub fn series_like<F>(
		&self,
		like: &str,
		mut callback: F,
	) -> Result<(), String>
		where F: FnMut(String, u64)
	{
		let mut c = self.metadata.db.prepare_cached(
			"select name, series_id from series where name like ?"
		).unwrap();
		let mut rows = c.query(&[&like]).unwrap();
		while let Some(row) = rows.next()
		{
			let row = row.unwrap();
			callback(
				row.get::<_,String>(0),
				row.get::<_,i64>(1) as u64,
			);
		}
		Ok(())
	}

	pub fn erase_range(
		&self,
		series_id: u64,
		first_erase: Timestamp,
		last_erase: Timestamp,
	) -> Result<(), String>
	{
		if !self.writing
		{
			Err("attempt to write in a \
				read-only transaction".to_string())?;
		}
		let mut save = Savepoint::new(&self.metadata.db)?;

		let blocks = self.blocks_for_range(
			series_id,
			first_erase,
			last_erase,
		);
		if blocks.is_empty() { return Ok(()); }
		let format = self.series_format(series_id);
		let mut buffer = vec!();

		for block in blocks
		{
			let mut s = self.metadata.db.prepare_cached(
				"delete from series_blocks
				where series_id=?
				and first_timestamp=?
				"
			).unwrap();
			s.execute(&[
				&(series_id as i64),
				&block.first_timestamp.to_sqlite(),
			]).unwrap();

			if block.first_timestamp < first_erase
				|| block.last_timestamp > last_erase
			{ // we have to keep some of this block's contents
				buffer.resize(block.size as usize, 0u8);
				self.metadata.blocks
					.read(block.offset, &mut buffer[..]);

				// there are three strategies for saving some
				// of this block's contents:
				// 1. We keep a little from the start and a little from the end
				// 2. We keep some of the beginning and toss the rest
				// 3. We keep some of the end and toss the rest

				if first_erase > block.first_timestamp && last_erase < block.last_timestamp
				{
					let (keeping1, _, _, remainder)
						= split_raw_at_ts(&*format, &buffer, first_erase, true);

					let (_, _, _, keeping2)
						= split_raw_at_ts(&*format, remainder, last_erase, false);

					assert!(keeping1.len() > 0);
					assert!(keeping2.len() > 0);

					let newblock = self.create_new_block(
						series_id,
						block.first_timestamp,
						block.last_timestamp,
						keeping1.len() + keeping2.len(),
						format.preferred_block_size() as usize
					);
					self.metadata.blocks
						.write(
							newblock.offset,
							&keeping1
						);
					self.metadata.blocks
						.write(
							newblock.offset + keeping1.len() as u64,
							&keeping2
						);
				}
				else if first_erase > block.first_timestamp
				{
					let (keeping, last_keeping_ts, _, _)
						= split_raw_at_ts(&*format, &buffer, first_erase, true);
					assert!(keeping.len() > 0);
					let newblock = self.create_new_block(
						series_id,
						block.first_timestamp,
						last_keeping_ts,
						keeping.len(),
						format.preferred_block_size() as usize
					);
					self.metadata.blocks
						.write(
							newblock.offset,
							&keeping
						);
				}
				else if last_erase < block.last_timestamp
				{
					let (_, _, first_keeping_ts, keeping)
						= split_raw_at_ts(&*format, &buffer, last_erase, false);
					assert!(keeping.len() > 0);
					let newblock = self.create_new_block(
						series_id,
						first_keeping_ts,
						block.last_timestamp,
						keeping.len(),
						format.preferred_block_size() as usize
					);
					self.metadata.blocks
						.write(
							newblock.offset,
							&keeping
						);

				}
			}
		}
		save.commit()?;
		Ok(())
	}

	/// Inserts many values into a series
	///
	/// The timestamps must be sorted
	pub fn insert_into_series<Generator>(
		&mut self,
		series_id: u64,
		generator: Generator,
	) -> Result<(), String>
		where Generator: FnMut(&RowFormat, &mut Vec<u8>)
			-> Result<Option<Timestamp>, String>
	{
		if !self.writing
		{
			Err("attempt to write in a \
				read-only transaction".to_string())?;
		}
		let mut save = Savepoint::new(&self.metadata.db)?;

		let mut i = Inserter::new(self, series_id, generator);
		i.perform()?;
		save.commit()?;
		Ok(())
	}

	/// reads values for a range of timestamps.
	///
	/// the timestamps are inclusive
	pub fn read_series<Output>(
		&self,
		series_id: u64,
		first_timestamp: Timestamp,
		last_timestamp: Timestamp,
		mut out: Output,
	)
		where Output: FnMut(&Timestamp, &RowFormat, &[u8])
	{
		let blocks = self.blocks_for_range(
			series_id,
			first_timestamp,
			last_timestamp,
		);
		// eprintln!("blocks for range: {:?}", blocks);
		if blocks.is_empty() { return; }

		let format = self.series_format(series_id);

		let mut block_data = vec!();
		block_data.reserve(format.preferred_block_size());

		let mut done = false;

		for block in blocks
		{
			block_data.resize(block.size as usize, 0u8);
			self.metadata.blocks
				.read(block.offset, &mut block_data[..]);

			for sample in block_data.chunks(format.row_size())
			{
				let t = Timestamp(BigEndian::read_u64(&sample[0..8]));
				if t >= first_timestamp
				{
					if t > last_timestamp
					{
						done = true;
						break;
					}
					out(&t, &*format, &sample[8..]);
				}
			}

			if done { break; }
		}
	}

	/// creates a block in the metadata (does not populate the block)
	///
	/// `initial_size` is its used sized, all of which must be populated.
	///
	/// `initial_size` may be larger than the default capacity (a
	/// larger capacity is used).
	fn create_new_block(
		&self,
		series_id: u64,
		first_timestamp: Timestamp,
		last_timestamp: Timestamp,
		initial_size: usize, // not capacity
		capacity: usize,
	) -> Block
	{
		let capacity = capacity.max(initial_size);

		self.metadata.db.execute(
			"insert into series_blocks (
				series_id, generation, first_timestamp,
				last_timestamp, offset,
				capacity, size
			) values (
				?,?,?,?,?,?,?
			)",
			&[
				&(series_id as i64),
				&(self.metadata.generation as i64),
				&first_timestamp.to_sqlite(),
				&last_timestamp.to_sqlite(),
				&(self.metadata.next_offset.get() as i64),
				&(capacity as i64), &(initial_size as i64),
			]
		).unwrap();
		let b = Block
		{
			first_timestamp: first_timestamp,
			last_timestamp: last_timestamp,
			offset: self.metadata.next_offset.get(),
			capacity: capacity as u64,
			size: initial_size as u64,
		};


		self.metadata.next_offset.set(
			self.metadata.next_offset.get() + capacity as u64
		);

		b
	}

	fn resize_existing_block(
		&self,
		series_id: u64,
		first_timestamp: Timestamp,
		new_last_timestamp: Timestamp,
		new_size: u64,
	)
	{
		let mut stmt = self.metadata.db.prepare_cached(
			"update series_blocks
			set
				size=?, last_timestamp=?,
				generation=?
			where
				series_id=? and first_timestamp=?
			"
		).unwrap();
		stmt.execute(
			&[
				&(new_size as i64), &new_last_timestamp.to_sqlite(),
				&(self.metadata.generation as i64),
				&(series_id as i64), &first_timestamp.to_sqlite(),
			]
		).unwrap();
	}

	/// return a tuple of the block that would contain
	/// this timestamp. If the timestamp is at
	/// a boundary between blocks, return both
	fn block_for_series_timestamp(
		&self,
		series_id: u64,
		timestamp: Timestamp,
	) -> (Option<Block>, Option<Block>)
	{
		let mut before_stmt = self.metadata.db.prepare_cached(
			"select
				first_timestamp,
				last_timestamp,
				offset,
				capacity,
				size
			from series_blocks
			where
				series_id=?
				and first_timestamp<=?
			order by first_timestamp desc
			limit 1"
		).unwrap();

		let mut after_stmt = self.metadata.db.prepare_cached(
			"
			select
				first_timestamp,
				last_timestamp,
				offset,
				capacity,
				size
			from series_blocks
			where
				series_id=?
				and first_timestamp>?
			order by first_timestamp asc
			limit 1
		").unwrap();

		let mut before_rows = before_stmt.query(
			&[
				&(series_id as i64),
				&timestamp.to_sqlite(),
			]
		).unwrap();
		let mut after_rows = after_stmt.query(
			&[
				&(series_id as i64),
				&timestamp.to_sqlite(),
			]
		).unwrap();

		let before;
		if let Some(row) = before_rows.next()
		{
			let row = row.unwrap();
			before = Some(Block
			{
				first_timestamp: Timestamp::from_sqlite(row.get(0)),
				last_timestamp: Timestamp::from_sqlite(row.get(1)),
				offset: row.get::<_,i64>(2) as u64,
				capacity: row.get::<_,i64>(3) as u64,
				size: row.get::<_,i64>(4) as u64,
			});
		}
		else
		{
			before = None;
		}

		let after;
		if let Some(row) = after_rows.next()
		{
			let row = row.unwrap();
			after = Some(Block
			{
				first_timestamp: Timestamp::from_sqlite(row.get(0)),
				last_timestamp: Timestamp::from_sqlite(row.get(1)),
				offset: row.get::<_,i64>(2) as u64,
				capacity: row.get::<_,i64>(3) as u64,
				size: row.get::<_,i64>(4) as u64,
			});
		}
		else
		{
			after = None;
		}
		(before, after)
	}

	pub fn dump_series_like<Output>(
		&self,
		like: &str,
		first_timestamp: Timestamp,
		last_timestamp: Timestamp,
		mut out: Output,
	) -> Result<(), String>
	where
		Output: FnMut(&str, &Timestamp, &RowFormat, &[u8]),
	{
		let fd = self.metadata.blocks_raw_fd;

		let mut c = self.metadata.db.prepare_cached(
			"with selected_series as (select * from series where name like ?)
			select
				name, format,
				first_timestamp,
				last_timestamp,
				offset,
				capacity,
				size
			from selected_series
			natural left join series_blocks
			where
				? >= first_timestamp AND last_timestamp >= ?
			order by name, first_timestamp
			"
		).unwrap();
		let mut rows = c.query(&[
			&like,
			&last_timestamp.to_sqlite(),
			&first_timestamp.to_sqlite(),
		]).unwrap();
		let mut block_data = vec!();

		let mut names = Vec::with_capacity(32);
		let mut fmts = Vec::with_capacity(32);
		let mut blocks_group = Vec::with_capacity(32);

		loop
		{
			names.clear();
			fmts.clear();
			blocks_group.clear();

			while let Some(row) = rows.next()
			{
				let row = row.unwrap();
				let name: String = row.get(0);
				let fmt: String = row.get(1);

				let b = Block
				{
					first_timestamp: Timestamp::from_sqlite(row.get(2)),
					last_timestamp: Timestamp::from_sqlite(row.get(3)),
					offset: row.get::<_,i64>(4) as u64,
					capacity: row.get::<_,i64>(5) as u64,
					size: row.get::<_,i64>(6) as u64,
				};

				unsafe
				{
					libc::posix_fadvise(
						fd,
						b.offset as i64,
						b.size as i64,
						libc::POSIX_FADV_WILLNEED
					);
				}

				names.push( name );
				fmts.push( fmt );
				blocks_group.push( b );

				if blocks_group.len() == 32
					{ break; }
			}

			if blocks_group.len() == 0 { break; }

			for ((name, fmt), block) in names.iter().zip(&fmts).zip(&blocks_group)
			{
				let format = parse_row_format(&fmt);
				block_data.resize(block.size as usize, 0u8);
				self.metadata.blocks
					.read(block.offset, &mut block_data[..]);

				for sample in block_data.chunks(format.row_size())
				{
					let t = Timestamp(BigEndian::read_u64(&sample[0..8]));
					if t >= first_timestamp
					{
						if t > last_timestamp
						{
							break;
						}
						out(&name, &t, &*format, &sample[8..]);
					}
				}
			}
		}
		Ok(())
	}

	pub fn read_direction_multi<Something, It, Output>(
		&self,
		mut ids: It,
		timestamp: Timestamp,
		reverse: bool,
		mut out: Output,
	)
		where It: Iterator<Item=(u64, Something)>,
		Output: FnMut(Something, &Timestamp, &RowFormat, &[u8]),
		Something: Sized
	{
		let mut ids_group = Vec::with_capacity(32);
		let mut blocks_group = Vec::with_capacity(32);
		let fd = self.metadata.blocks_raw_fd;

		// get blocks and readahead
		loop
		{
			ids_group.clear();
			while ids_group.len() < 32
			{
				if let Some(n) = ids.next()
					{ ids_group.push(n); }
				else
					{ break; }
			}
			if ids_group.is_empty() { break; }

			blocks_group.clear();

			for (id,something) in ids_group.drain(..)
			{
				if let Some(b) = self.first_block_direction(id, timestamp, reverse)
				{
					unsafe
					{
						libc::posix_fadvise(
							fd,
							b.offset as i64,
							b.size as i64,
							libc::POSIX_FADV_WILLNEED
						);
					}
					blocks_group.push( (id, something, b) );
				}
			}

			let mut block_data = vec!();

			for (id, something, block) in blocks_group.drain(..)
			{
				let format = self.series_format(id);
				block_data.resize(block.size as usize, 0u8);
				self.metadata.blocks
					.read(block.offset, &mut block_data[..]);

				if reverse
				{
					for sample in block_data.chunks(format.row_size()).rev()
					{
						let t = Timestamp(BigEndian::read_u64(&sample[0..8]));
						if t <= timestamp
						{
							out(something, &t, &*format, &sample[8..]);
							break;
						}
					}
				}
				else
				{
					for sample in block_data.chunks(format.row_size())
					{
						let t = Timestamp(BigEndian::read_u64(&sample[0..8]));
						if t >= timestamp
						{
							out(something, &t, &*format, &sample[8..]);
							break;
						}
					}
				}
			}
		}
	}

	fn first_block_direction(
		&self,
		series_id: u64,
		timestamp: Timestamp,
		reverse: bool,
	) -> Option<Block>
	{
		if reverse
		{
			let mut s = self.metadata.db.prepare_cached("
				select
					first_timestamp,
					last_timestamp,
					offset,
					capacity,
					size
				from series_blocks
				where
					series_id=? and
					first_timestamp <= ?
				order by first_timestamp desc
				limit 1
			").unwrap();
			let mut rows = s.query(&[
				&(series_id as i64),
				&timestamp.to_sqlite(),
			]).unwrap();
			if let Some(row) = rows.next()
			{
				let row = row.unwrap();
				let b = Block
				{
					first_timestamp: Timestamp::from_sqlite(row.get(0)),
					last_timestamp: Timestamp::from_sqlite(row.get(1)),
					offset: row.get::<_,i64>(2) as u64,
					capacity: row.get::<_,i64>(3) as u64,
					size: row.get::<_,i64>(4) as u64,
				};
				Some(b)
			}
			else
			{
				None
			}
		}
		else
		{
			let mut s = self.metadata.db.prepare_cached("
				select * from
				(
					select
						first_timestamp,
						last_timestamp,
						offset,
						capacity,
						size
					from series_blocks
					where
						series_id=? and first_timestamp <= ?
					order by first_timestamp desc
					limit 1
				)
				union select * from
				(
					select
						first_timestamp,
						last_timestamp,
						offset,
						capacity,
						size
					from series_blocks
					where
						series_id=? and first_timestamp >= ?
					order by first_timestamp asc
					limit 1
				)
			").unwrap();
			let mut rows = s.query(&[
				&(series_id as i64),
				&timestamp.to_sqlite(),
				&(series_id as i64),
				&timestamp.to_sqlite(),
			]).unwrap();
			while let Some(row) = rows.next()
			{
				let row = row.unwrap();
				let b = Block
				{
					first_timestamp: Timestamp::from_sqlite(row.get(0)),
					last_timestamp: Timestamp::from_sqlite(row.get(1)),
					offset: row.get::<_,i64>(2) as u64,
					capacity: row.get::<_,i64>(3) as u64,
					size: row.get::<_,i64>(4) as u64,
				};

				if b.last_timestamp < timestamp
				{
					continue;
				}
				else
				{
					return Some(b);
				}
			}
			None
		}
	}

	pub fn commit(mut self)
	{
		if self.writing
		{
			self.metadata.blocks.commit();
			self.finishing_on.unwrap()
				.committing(&self.metadata);
			self.metadata.db.execute("delete from end_offset", &[]).unwrap();
			self.metadata.db.execute(
				"insert into end_offset values(?)", &[&(self.metadata.next_offset.get() as i64)]
			).unwrap();
		}
		self.committed = true;
		self.metadata.db.execute("commit", &[]).unwrap();
	}
}

struct Inserter<'m, Generator>
	where Generator: FnMut(&RowFormat, &mut Vec<u8>)
		-> Result<Option<Timestamp>, String>
{
	tx: &'m Transaction<'m>,
	format: Box<RowFormat>,
	series_id: u64,
	preferred_block_size: u64,
	buffer: Vec<u8>,

	creating_at: Option<Timestamp>,
	last_ts: Timestamp,
	previous_block: Option<Block>,
	following_block: Option<Block>,
	generator: Generator,
}

impl<'m, Generator> Inserter<'m, Generator>
	where Generator: FnMut(&RowFormat, &mut Vec<u8>)
		-> Result<Option<Timestamp>, String>
{
	fn new(tx: &'m Transaction<'m>, series_id: u64, generator: Generator)
		-> Self
	{
		let format = tx.series_format(series_id);
		let preferred_block_size = format.preferred_block_size();

		Inserter
		{
			tx: tx,
			format: format,
			series_id: series_id,
			preferred_block_size: preferred_block_size as u64,
			buffer: Vec::with_capacity(preferred_block_size),
			creating_at: None,
			last_ts: Timestamp(0),
			previous_block: None,
			following_block: None,
			generator: generator,
		}
	}

	fn perform(&mut self) -> Result<(), String>
	{
		loop
		{
			let len = self.buffer.len();

			let incoming = (self.generator)(&*self.format, &mut self.buffer)?;
			if incoming.is_none() { break; }
			let incoming = incoming.unwrap();
			if incoming <= self.last_ts
			{
				return Err("timestamps must be in ascending order".to_string());
			}
			self.handle_last_item(len, incoming)?;

			self.last_ts = incoming;
		}

		if !self.buffer.is_empty()
		{
			let l = self.buffer.len();
			let ts = self.last_ts;
			self.save_current_block(l, ts)?;
		}
		Ok(())
	}

	// we just added "at" to the end of the buffer
	fn handle_last_item(&mut self, len_before_adding: usize, at: Timestamp)
		-> Result<(), String>
	{
		let row_size = self.format.row_size() as u64;
		let mut boundary_reached = self.creating_at.is_none();
		loop
		{
			if boundary_reached
			{
				boundary_reached = false;
				let (previous_block, following_block)
					= self.tx.block_for_series_timestamp(self.series_id, at);

				self.previous_block = previous_block;
				self.following_block = following_block;
			}

			if self.creating_at.is_none()
			{
				if let Some(previous_block) = self.previous_block
				{
					if previous_block.last_timestamp == at
					{
						return Err("cannot overwrite timestamp".to_string());
					}
					if previous_block.last_timestamp > at
					{
						self.break_previous_block_at(at);
					}
					else if previous_block.size+row_size > previous_block.capacity
					{
						self.creating_at = Some(at);
					}
				}
				else
				{
					// there isn't a previous block
					self.creating_at = Some(at);
				}
			}

			// see if I've gotten to the next block
			if let Some(following_block) = self.following_block
			{
				if at == following_block.first_timestamp
				{
					return Err("cannot overwrite timestamp".to_string());
				}
				if at > following_block.first_timestamp
				{
					// we have finished with current_block, write it
					let ts = self.last_ts;
					self.save_current_block(len_before_adding, ts)?;
					boundary_reached = true;
					continue;
				}
			}

			break;
		}
		// disable incorrect warning?
		let _ = boundary_reached;

		Ok(())
	}

	fn save_current_block(&mut self, len_before_adding: usize, last_ts: Timestamp)
		-> Result<(), String>
	{
		if let Some(creating_at) = self.creating_at
		{
			let new_block = self.tx.create_new_block(
				self.series_id,
				creating_at,
				last_ts,
				len_before_adding,
				self.preferred_block_size as usize, // TODO: depending on if it's the last block
			);
			self.creating_at = None;
			self.tx.metadata.blocks
				.write(
					new_block.offset,
					&self.buffer[0..len_before_adding]
				);
		}
		else
		{
			let b = self.previous_block.unwrap();

			self.tx.resize_existing_block(
				self.series_id,
				b.first_timestamp,
				last_ts,
				b.size + len_before_adding as u64,
			);
			self.tx.metadata.blocks
				.write(
					b.offset + b.size,
					&self.buffer[0..len_before_adding]
				);
		}

		// put the last item at the front
		let new_len;
		{
			let (left, right) = self.buffer.split_at_mut(len_before_adding);
			new_len = right.len();
			left[0..new_len].copy_from_slice(right);
		}
		self.buffer.truncate(new_len);
		Ok(())
	}

	fn break_previous_block_at(&mut self, at: Timestamp)
	{
		let block = self.previous_block.take().unwrap();

		let mut buffer2 = vec!();
		buffer2.resize(block.size as usize, 0u8);
		self.tx.metadata.blocks
			.read(block.offset, &mut buffer2[..]);

		let resize_buffer_to;
		{
			let (one, _, first_ts, two)
				= split_raw_at_ts(&*self.format, &buffer2, at, false);

			assert!(one.len()>0);
			assert!(two.len()>0);
			{
				let mut s = self.tx.metadata.db.prepare_cached(
					"delete from series_blocks
					where series_id=?
					and first_timestamp=?
					"
				).unwrap();
				s.execute(&[
					&(self.series_id as i64),
					&block.first_timestamp.to_sqlite(),
				]).unwrap();

				// create the block for "two"
				let twoblock = self.tx.create_new_block(
					self.series_id,
					first_ts, block.last_timestamp,
					two.len(),
					self.preferred_block_size as usize, // TODO: depending on if it's the last block
				);

				self.tx.metadata.blocks
					.write(
						twoblock.offset,
						&two
					);
				self.following_block = Some(twoblock);
			}

			resize_buffer_to = one.len();
		}
		buffer2.truncate(resize_buffer_to);
		buffer2.extend_from_slice( &self.buffer );
		self.buffer = buffer2;
		self.creating_at = Some(block.first_timestamp);
	}

}

// return the data before and after the timestamp at
// .0: everything before incoming `at`
// .1: the last timestamp in .0
// .2: the first timestamp in .3
// .3: everything after `at`
fn split_raw_at_ts<'a>(
	format: &RowFormat,
	data: &'a [u8],
	at: Timestamp,
	inclusive: bool,
) -> (&'a [u8], Timestamp, Timestamp, &'a [u8])
{
	let stride = format.row_size();
	let mut pos = 0;
	let mut prev = Timestamp(0);
	while pos < data.len()
	{
		let t = Timestamp(BigEndian::read_u64(&data[pos..pos+8]));
		if (inclusive && t >= at) || (!inclusive && t > at)
		{
			return (&data[0..pos], prev, t, &data[pos..]);
		}
		prev = t;
		pos += stride;
	}

	(&data[..], prev, prev, &data[data.len()..])
}

impl<'db> Drop for Transaction<'db>
{
	fn drop(&mut self)
	{
		if !self.committed
		{
			self.metadata.db.execute("rollback", &[]).unwrap();
		}
	}
}

struct Savepoint<'conn>
{
	conn: &'conn rusqlite::Connection,
	done: bool,
}

impl<'conn> Savepoint<'conn>
{
	fn new(conn: &'conn rusqlite::Connection)
		-> Result<Savepoint, String>
	{
		conn.execute("savepoint sp", &[])
			.map_err(|e| format!("failed to begin savepoint: {}", e))?;
		Ok(Savepoint
		{
			conn: conn,
			done: false,
		})
	}

	fn commit(&mut self) -> Result<(), String>
	{
		self.conn.execute(
			"release savepoint sp", &[]
		)
			.map_err(|e| format!("failed to release savepoint: {}", e))?;
		self.done = true;
		Ok(())
	}
}

impl<'conn> Drop for Savepoint<'conn>
{
	fn drop(&mut self)
	{
		if !self.done
		{
			let _ = self.conn.execute(
				"rollback to savepoint sp", &[]
			);
		}
	}
}

/// Map u64 to i64, because sqlite doesn't do unsigned 64-bit
///
/// We just subtract the difference so that sorting is still the same
impl Timestamp
{
	fn to_sqlite(&self) -> i64
	{
		(self.0 as i64).wrapping_add(::std::i64::MIN)
	}
	fn from_sqlite(v: i64) -> Timestamp
	{
		Timestamp(v.wrapping_sub(::std::i64::MIN) as u64)
	}
}

#[cfg(test)]
mod tests
{
	use ::metadata::Timestamp;
	#[test]
	fn timestamp_range()
	{
		assert_eq!(Timestamp(::std::u64::MAX).to_sqlite(), ::std::i64::MAX);
		assert_eq!(Timestamp(500).to_sqlite(), ::std::i64::MIN+500);
		assert_eq!(Timestamp(0).to_sqlite(), ::std::i64::MIN);

		assert_eq!(Timestamp::from_sqlite(::std::i64::MIN).0, 0);
		assert_eq!(Timestamp::from_sqlite(0).0-1, ::std::i64::MAX as u64);

		for some in &[::std::i64::MIN, ::std::i64::MIN+100, 0, 100, ::std::i64::MAX-1000]
		{
			assert_eq!(Timestamp::from_sqlite(*some).to_sqlite(), *some);
		}
	}
}

#[derive(Debug,Copy,Clone)]
struct Block
{
	first_timestamp: Timestamp,
	last_timestamp: Timestamp,
	offset: u64,
	capacity: u64,
	size: u64,
}
