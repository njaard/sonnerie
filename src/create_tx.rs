use std::path::{PathBuf,Path};
use crate::write::Writer;
use std::io::{Write,Seek};

pub struct CreateTx
{
	writer: Option<Writer<std::fs::File>>,
	tmp_name: PathBuf,
	dir: PathBuf,
}

impl CreateTx
{
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

	pub fn add_record(&mut self, key: &str, format: &str, data: &[u8])
		-> std::result::Result<(), crate::write::WriteFailure>
	{
		self.writer.as_mut().unwrap().add_record(key, format, data)
	}

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
