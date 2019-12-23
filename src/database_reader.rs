use std::path::{Path,PathBuf};
use std::fs::File;
use std::io::Seek;

use crate::merge::Merge;
use crate::record::OwnedRecord;
use crate::key_reader::*;
use crate::Wildcard;

use byteorder::ByteOrder;

pub struct DatabaseReader
{
	_dir: PathBuf,
	txes: Vec<(PathBuf,Reader)>,
}


impl DatabaseReader
{
	pub fn new(dir: &Path)
		-> std::io::Result<DatabaseReader>
	{
		Self::new_opts(dir, true)
	}

	pub fn without_main_db(dir: &Path)
		-> std::io::Result<DatabaseReader>
	{
		Self::new_opts(dir, false)
	}

	fn new_opts(dir: &Path, include_main_db: bool)
		-> std::io::Result<DatabaseReader>
	{
		let dir_reader = std::fs::read_dir(dir)?;

		let mut paths = vec!();

		for entry in dir_reader
		{
			let entry = entry?;
			if let Some(s) = entry.file_name().to_str()
			{
				if s.starts_with("tx.") && !s.ends_with(".tmp")
				{
					paths.push(entry.path());
				}
			}
		}

		paths.sort();
		let mut txes = Vec::with_capacity(paths.len());

		if include_main_db
		{
			let main_db_name = dir.join("main");
			let mut f = File::open(&main_db_name)?;
			let len = f.seek(std::io::SeekFrom::End(0))? as usize;
			if len == 0
			{
				eprintln!("disregarding main database, it is zero length");
			}
			else
			{
				let main_db = Reader::new(f)?;
				txes.push( (main_db_name, main_db) );
			}
		}

		for p in paths
		{
			let mut f = File::open(&p)?;
			let len = f.seek(std::io::SeekFrom::End(0))? as usize;
			if len == 0
			{
				eprintln!("disregarding {:?}, it is zero length", p);
				continue;
			}
			let r = Reader::new(f)?;
			txes.push( (p,r) );
		}

		Ok(DatabaseReader
		{
			txes,
			_dir: dir.to_owned(),
		})
	}

	pub fn transaction_paths(&self) -> Vec<PathBuf>
	{
		self.txes
			.iter()
			.map( |e| e.0.clone())
			.collect()
	}

	pub fn get<'rdr, 'k>(&'rdr self, key: &'k str)
		-> DatabaseKeyReader<'rdr, 'k, std::ops::RangeInclusive<&'k str>>
	{
		self.get_range( key ..= key )
	}

	pub fn get_range<'d, 'r, RB>(&'d self, range: RB)
		-> DatabaseKeyReader<'d, 'r, RB>
	where
		RB: std::ops::RangeBounds<&'r str> + Clone
	{
		let mut readers = Vec::with_capacity(self.txes.len());

		for tx in &self.txes
		{
			readers.push( tx.1.get_range(range.clone()) );
		}
		let merge = Merge::new(
			readers,
			|a, b|
			{
				a.key().cmp(b.key())
					.then_with(
						||
							byteorder::BigEndian::read_u64(a.value())
								.cmp(&byteorder::BigEndian::read_u64(b.value()))
					)
			},
		);

		DatabaseKeyReader
		{
			_db: self,
			merge: Box::new(merge),
		}
	}
	pub fn get_filter<'d, 'k>(&'d self, wildcard: &'k Wildcard)
		-> DatabaseKeyReader<'d, 'k, std::ops::RangeFrom<&'k str>>
	{
		let mut readers = Vec::with_capacity(self.txes.len());

		for tx in &self.txes
		{
			readers.push( tx.1.get_filter(wildcard) );
		}
		let merge = Merge::new(
			readers,
			|a, b|
			{
				a.key().cmp(b.key())
					.then_with(
						||
							byteorder::BigEndian::read_u64(a.value())
								.cmp(&byteorder::BigEndian::read_u64(b.value()))
					)
			},
		);

		DatabaseKeyReader
		{
			_db: self,
			merge: Box::new(merge),
		}
	}
}




pub struct DatabaseKeyReader<'d, 'r, RB>
where
	RB: std::ops::RangeBounds<&'r str>
{
	_db: &'d DatabaseReader,
	merge: Box<Merge<
		StringKeyRangeReader<'d, 'r, RB>, OwnedRecord,
	>>,
}

impl<'d, 'r, RB> Iterator for DatabaseKeyReader<'d, 'r, RB>
where
	RB: std::ops::RangeBounds<&'r str>
{
	type Item = OwnedRecord;

	fn next(&mut self) -> Option<Self::Item>
	{
		self.merge.next()
	}
}


