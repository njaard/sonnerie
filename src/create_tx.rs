//! Add data by means of a new transaction.

use crate::write::Writer;
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};

/// Create a transaction file in the specified db directory.
///
/// Add new records with [`CreateTx::add_record`]. They must be
/// in sorted order.
///
/// After adding records, call [`CreateTx::commit`] which ensures
/// the transaction is on disk. Not calling commit will
/// rollback the transaction.
pub struct CreateTx {
	writer: Writer<std::fs::File>,
	tmp: tempfile_fast::PersistableTempFile,
	dir: PathBuf,
}

impl CreateTx {
	/// Open a transaction file inside this specific directory.
	///
	/// The transaction is named "tx.XXX.tmp" where XXX is an
	/// increasing value basedo on timestamp.
	///
	/// On commit, the file is renamed to not have the ".tmp"
	/// suffix.
	pub fn new(dir: &Path) -> std::io::Result<CreateTx> {
		let tmp = tempfile_fast::PersistableTempFile::new_in(dir)?;
		let f = tmp.try_clone()?;

		let writer = Writer::new(f);

		let tx = CreateTx {
			writer,
			tmp,
			dir: dir.to_owned(),
		};
		Ok(tx)
	}

	/// Add a record with the given key, format, and payload.
	///
	/// The data must match the format (otherwise you can corrupt
	/// the database). The data also encodes the timestamp.
	///
	/// Each successive call to this function must have greater
	/// or equal values for key and timestamp.
	///
	/// Encode the data with [`crate::row_format::RowFormat`].
	pub fn add_record(
		&mut self,
		key: &str,
		format: &str,
		data: &[u8],
	) -> std::result::Result<(), crate::write::WriteFailure> {
		self.writer.add_record(key, format, data)
	}

	pub fn delete(
		&mut self,
		first_key: &str,
		last_key: &str,
		after_time: u64,
		before_time: u64,
		filter: &str,
	) -> std::result::Result<(), crate::write::WriteFailure> {
		use core::ops::IndexMut as _;

		use crate::row_format::{Element as _, ElementString};
		use byteorder::{BigEndian, ByteOrder as _};

		// write row format
		let key = first_key;
		let format = "\u{007f}";

		let mut row_data = Vec::with_capacity(
			first_key.as_bytes().len()
                + filter.as_bytes().len()
                + last_key.as_bytes().len()
                + 16 // length of two u64's
                + 27 // practical maximum length of three varints
                + 1, // the format
		);

		// bypass RowFormat entirely, we're going to be building row_data here
		// so we don't get to pass str values

		// write first key
		ElementString
			.to_stored_format(&first_key, &mut row_data)
			.unwrap();

		// write first timestamp
		row_data.extend_from_slice(&[0; 8]);
		BigEndian::write_u64(
			row_data.index_mut(row_data.len() - 8..row_data.len()),
			after_time,
		);

		// write last timestamp
		row_data.extend_from_slice(&[0; 8]);
		BigEndian::write_u64(
			row_data.index_mut(row_data.len() - 8..row_data.len()),
			before_time,
		);

		// write key wildcard
		ElementString
			.to_stored_format(&filter, &mut row_data)
			.unwrap();

		// write last key
		ElementString
			.to_stored_format(&last_key, &mut row_data)
			.unwrap();

		self.add_record(&key, &format, &row_data)
	}

	/// Commit the transaction, but give it a specific name.
	///
	/// This function is necessary for compacting, normally
	/// you would just call the basic [`CreateTx::commit`](.
	pub fn commit_to(self, final_name: &Path) -> std::io::Result<()> {
		let writer = self.writer;
		let mut file = writer.finish()?;
		file.flush()?;
		let len = file.seek(std::io::SeekFrom::End(0))? as usize;
		if len == 0 {
			// don't create an empty transaction file
			drop(file);
			if final_name.file_name().map(|n| n == "main") != Some(true) {
				let _ = std::fs::remove_file(&final_name);
			}
			return Ok(());
		}
		file.sync_all()?;
		drop(file);
		let named = self
			.tmp
			.persist_by_rename(&final_name)
			.map_err(|e| e.error)?;
		if let Some(umask) = get_umask() {
			use std::os::unix::fs::PermissionsExt;
			let p = std::fs::Permissions::from_mode((0o444 & !umask) as u32);
			let _ = std::fs::set_permissions(final_name, p);
		}
		Ok(named)
	}

	/// Commit the transaction.
	///
	/// On successful completion, the data is on disk (fsync is called)
	/// and the filename is renamed to lose its ".tmp" suffix.
	pub fn commit(self) -> std::io::Result<()> {
		{
			// maybe we can just replace `main`
			let mainpath = self.dir.join("main");
			let maininfo = std::fs::metadata(&mainpath)?;
			if maininfo.len() == 0 {
				use fs2::FileExt;
				// ok, try again, this time having locked the db
				let lock = std::fs::File::create(self.dir.join(".compact"))?;
				lock.lock_exclusive()?;
				let maininfo = std::fs::metadata(&mainpath)?;
				if maininfo.len() == 0 {
					// now, with a lock, `main` is still 0 bytes, so
					// we can safely replace it
					return self.commit_to(&mainpath);
				}
			}
		}

		for attempt in 0.. {
			let timestamp = std::time::SystemTime::now()
				.duration_since(std::time::SystemTime::UNIX_EPOCH)
				.expect("duration_since epoch")
				.as_secs();

			let n = format!("tx.{:016x}", timestamp);
			let final_name = self.dir.join(n);

			let f = std::fs::OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(&final_name);
			match f {
				Ok(_) => {
					if let Err(e) = self.commit_to(&final_name) {
						eprintln!("failure committing {:?}", final_name);
						return Err(e);
					} else {
						return Ok(());
					}
				}
				Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
					if attempt == 1000 {
						return Err(e);
					}
					std::thread::sleep(std::time::Duration::from_millis(100 * attempt));
					continue;
				}
				Err(e) => return Err(e),
			}
		}
		unreachable!();
	}
}

fn get_umask() -> Option<libc::mode_t> {
	let s = std::fs::read_to_string("/proc/self/status").ok()?;
	for line in s.split('\n') {
		if let Some(line) = line.strip_prefix("Umask:") {
			let line = line.trim();
			return libc::mode_t::from_str_radix(line, 8).ok();
		}
	}
	None
}
