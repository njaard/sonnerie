extern crate byteorder;
use self::byteorder::{ReadBytesExt, WriteBytesExt, BigEndian, ByteOrder};

const MAGIC: &'static [u8] = b"tsrust.wal.0000\n";
const MAGIC_LEN: usize = 16;


use ::std::io::{BufWriter,BufReader};
use ::std::path::{Path,PathBuf};
use std::io::Write;
use std::io::Read;

use wal::MemoryWal;

/// Write files in our WAL file format
///
/// The WAL files are written and discarded
/// once data for the WAL's generation is written
/// and synced to the block file.
///
/// The WAL files are read only on startup
/// to regenerate the in-memory WAL data.
pub struct DiskWalWriter
{
	file: BufWriter<::std::fs::File>,
}

impl DiskWalWriter
{
	/// create a file inside of `dir`
	/// for data of the given generation
	pub fn new(generation: u64, dir: &Path)
		-> (DiskWalWriter, PathBuf)
	{
		for idx in 0..10000
		{
			let filename =
			{
				use ::std::time::SystemTime;
				let now = SystemTime::now();
				let d = now.duration_since(
					::std::time::UNIX_EPOCH
				).unwrap();

				format!(
					"blocks-{}{}-{}.wal",
					d.as_secs(),
					d.subsec_nanos(),
					idx,
				)
			};

			let filepath = dir.join(&filename);
			let fileres = ::std::fs::OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(filepath.clone());
			if let Ok(file) = fileres
			{
				let mut file = BufWriter::new(file);
				file.write_all(MAGIC).unwrap();
				file.write_u64::<BigEndian>(generation).unwrap();

				let z = &[0u8; 512-MAGIC_LEN-8];
				file.write_all(z).unwrap();
				file.flush().unwrap();

				return (
					DiskWalWriter
					{
						file: file,
					},
					filepath,
				);
			}
		}
		panic!("failed to create wal file");
	}

	/// write a single record, with its magic number
	pub fn write(&mut self, position: u64, data: &[u8])
	{
		self.file.write_i32::<BigEndian>(0x07010503).unwrap();
		self.file.write_u64::<BigEndian>(position).unwrap();
		self.file.write_u64::<BigEndian>(data.len() as u64).unwrap();
		self.file.write_all(data).unwrap();
	}
}

/// Mark the file as completed, sync the file, and finally close it
impl Drop for DiskWalWriter
{
	fn drop(&mut self)
	{
		self.file.write_i32::<BigEndian>(0x0d011e00).unwrap();
		self.file.flush().unwrap();
		self.file.get_ref().sync_all().unwrap();
	}
}

/// Read a file written by the `DiskWalWriter`.
///
/// This code is run only on startup to restore the
/// last state after a shutdown with its committed
/// but unmerged transactions.
pub struct DiskWalReader
{
	file: BufReader<::std::fs::File>,
	generation: u64,
}

impl DiskWalReader
{
	pub fn open(file: &Path) -> DiskWalReader
	{
		let mut file = BufReader::new(
			::std::fs::File::open(file).unwrap()
		);

		let mut header = [0u8; 512];
		file.read_exact(&mut header).unwrap();
		if !header.starts_with(&MAGIC)
			{ panic!("invalid file"); }
		let generation =
			BigEndian::read_u64(&header[MAGIC.len()..MAGIC.len()+8]);
		DiskWalReader
		{
			file: file,
			generation: generation,
		}
	}

	pub fn read_into(&mut self, into: &mut MemoryWal)
	{
		let ref mut f = self.file;

		let mut buf = vec!();

		loop
		{
			let code = f.read_u32::<BigEndian>();

			if let Err(e) = code.as_ref()
			{
				// this is a sign the program was exited before
				// this wal file was fully written.
				// It's ok to apply the changes so far
				// because the meta db ensures they're not used
				if e.kind() == ::std::io::ErrorKind::UnexpectedEof
					{ break; }
			}

			let code = code.unwrap();
			if code == 0x0d011e00
				{ break; }
			else if code == 0x07010503
			{
				let position = f.read_u64::<BigEndian>().unwrap();
				let len = f.read_u64::<BigEndian>().unwrap();
				buf.resize(len as usize, 0u8);
				f.read_exact(&mut buf).unwrap();
				into.write(position as usize, &buf);
			}
			else
			{
				panic!("invalid wal file");
			}
		}
	}

	pub fn generation(&self) -> u64
	{
		self.generation
	}
}
