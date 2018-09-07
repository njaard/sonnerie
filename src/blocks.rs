use wal;
use disk_wal::DiskWalWriter;
use block_file::BlockFile;

/// Permit reads of blocks
///
/// Attempts to read from the block file, then
/// overlay the WAL data.
///
/// Writes get queued into the WAL
pub struct Blocks
{
	pub file: BlockFile,
	pub wal: wal::MemoryWal,
	disk_wal: Option<DiskWalWriter>,
}

impl Blocks
{
	pub fn new(file: BlockFile, wal: wal::MemoryWal) -> Blocks
	{
		Blocks
		{
			file: file,
			wal: wal,
			disk_wal: None,
		}
	}

	pub fn set_disk_wal(&mut self, w: DiskWalWriter)
	{
		self.disk_wal = Some(w);
	}

	// write only to wal_not_written
	// (someone else will actually flush wal_not_written)
	pub fn write(&mut self, position: u64, data: &[u8])
	{
		let d = self.disk_wal.as_mut().unwrap();
		d.write(position, data);
		self.wal.write(position as usize, data);
	}

	// read from the file, then overlay the contents
	// of wal_not_written
	pub fn read(&self, position: u64, data: &mut [u8])
	{
		self.file.read(position, data);
		self.wal.read(position as usize, data);
	}

	pub fn commit(&mut self)
	{
		self.disk_wal.take();
	}
}


