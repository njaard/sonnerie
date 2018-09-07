use std::fs::File;
use std::path::Path;
use std::os::unix::prelude::FileExt;

/// Read and write to the main file data
pub struct BlockFile
{
	file: File,
}

impl BlockFile
{
	/// open the file or panic
	pub fn new(filename: &Path)
		-> BlockFile
	{
		let f = ::std::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(filename)
			.unwrap();
		BlockFile
		{
			file: f,
		}
	}

	/// write exactly the given data, or panic
	pub fn write(&self, position: u64, data: &[u8])
	{
		assert_eq!(
			self.file.write_at(data, position)
				.expect("reading block file"),
			data.len()
		);
	}

	/// read from the file at exactly that position.
	///
	/// Short reads are permitted (because the WAL
	/// will make up the difference)
	pub fn read(&self, position: u64, data: &mut [u8])
	{
		self.file.read_at(data, position)
			.expect("reading block file");
	}

	/// call `fsync`
	pub fn sync(&self)
	{
		self.file.sync_all().unwrap();
	}
}
