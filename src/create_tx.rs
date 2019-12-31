//! Add data by means of a new transaction.

use std::path::{PathBuf,Path};
use crate::write::Writer;
use std::io::{Write,Seek};

/// Create a transaction file in the specified db directory.
///
/// Add new records with [`new_record`]. They must be
/// in sorted order.
///
/// After adding records, call [`commit`] which ensures
/// the transaction is on disk. Not calling commit will
/// rollback the transaction.
pub struct CreateTx
{
	writer: Option<Writer<std::fs::File>>,
	tmp_name: PathBuf,
	dir: PathBuf,
}

impl CreateTx
{
	/// Open a transaction file inside this specific directory.
	///
	/// The transaction is named "tx.XXX.tmp" where XXX is an
	/// increasing value basedo on timestamp.
	///
	/// On commit, the file is renamed to not have the ".tmp"
	/// suffix.
	pub fn new(dir: &Path) -> std::io::Result<CreateTx>
	{
		for attempt in 0..
		{
			let timestamp = std::time::SystemTime::now()
				.duration_since(std::time::SystemTime::UNIX_EPOCH)
				.expect("duration_since epoch")
				.as_secs();

			let n = format!("tx.{:016x}", timestamp);
			let tmp_name = dir.join(n + ".tmp");

			let f = std::fs::OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(&tmp_name);
			if let Err(e) = f
			{
				if attempt == 1000
					{ return Err(e); }
				std::thread::sleep(std::time::Duration::from_millis(100));
				continue;
			}

			let writer = f.unwrap();
			let writer = Writer::new(writer);

			let tx = CreateTx
				{
					writer: Some(writer),
					tmp_name,
					dir: dir.to_owned(),
				};
			return Ok(tx);
		}
		unreachable!();
	}

	/// Add a record with the given key, format, and payload.
	///
	/// The data must match the format (otherwise you can corrupt
	/// the database). The data also encodes the timestamp.
	///
	/// Each successive call to this function must have greater
	/// or equal values for key and timestamp.
	///
	/// Encode the data with [`row_format`].
	pub fn add_record(&mut self, key: &str, format: &str, data: &[u8])
		-> std::result::Result<(), crate::write::WriteFailure>
	{
		self.writer.as_mut().unwrap().add_record(key, format, data)
	}

	/// Commit the transaction, but give it a specific name.
	///
	/// This function is necessary for compacting, normally
	/// you would just call the basic [`commit`].
	pub fn commit_to(mut self, final_name: &Path)
		-> std::io::Result<()>
	{
		let writer = self.writer.take().unwrap();
		let mut file = writer.finish()?;
		file.flush()?;
		let len = file.seek(std::io::SeekFrom::End(0))? as usize;
		if len == 0
		{
			drop(file);
			let _ = std::fs::remove_file(&self.tmp_name);
			let _ = std::fs::remove_file(&final_name);
			return Ok(());
		}
		file.sync_all()?;
		drop(file);
		std::fs::rename(&self.tmp_name, &final_name)
	}

	/// Commit the transaction.
	///
	/// On successful completion, the data is on disk (fsync is called)
	/// and the filename is renamed to lose its ".tmp" suffix.
	pub fn commit(self)
		-> std::io::Result<()>
	{
		for attempt in 0..
		{
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
			if let Err(e) = f
			{
				if attempt == 1000
					{ return Err(e); }
				std::thread::sleep(std::time::Duration::from_millis(100));
				continue;
			}
			else
			{
				let tmp_name = self.tmp_name.clone();
				if let Err(e) = self.commit_to(&final_name)
				{
					eprintln!("failure committing {:?} {:?}", tmp_name, final_name);
					// don't leave files around from failed transactions
					std::fs::remove_file(&tmp_name)
						.expect("failed to remove file");
					std::fs::remove_file(&final_name)
						.expect("failed to remove file");
					return Err(e);
				}
				break;
			}
		}
		Ok(())
	}
}


impl Drop for CreateTx
{
	fn drop(&mut self)
	{
		if self.writer.is_some()
		{
			drop(self.writer.take());
			let _ = std::fs::remove_file(&self.tmp_name);
		}
	}
}
