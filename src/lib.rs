mod write;
pub(crate) mod segment;
pub(crate) mod segment_reader;
pub mod key_reader;
pub mod record;
pub mod create_tx;
pub mod formatted;
pub mod row_format;
pub(crate) mod merge;
pub(crate) mod database_reader;
pub mod wildcard;

pub use write::WriteFailure;

pub(crate) use segment::*;
pub use key_reader::*;
pub use create_tx::*;
pub use formatted::*;
pub use row_format::*;
pub use wildcard::*;
pub use database_reader::*;

#[cfg(test)]
mod tests
{
	use std::io::BufWriter;
	use crate::write::Writer;
	use crate::segment_reader::*;
	use crate::CreateTx;
	use crate::database_reader::DatabaseReader;

	#[test]
	fn write()
	{
		let t = tempfile::TempDir::new().unwrap();

		{
			let w = std::fs::File::create(t.path().join("w")).unwrap();
			let w = BufWriter::new(w);

			let mut w = Writer::new(w);
			w.add_record("a", "u", b"\0\0\0\0").unwrap();
			w.finish().unwrap();
		}

		let mut w = std::fs::File::open(t.path().join("w")).unwrap();
		let o = SegmentReader::open(&mut w).unwrap();
		o.print_info(&mut std::io::stderr()).unwrap();
		let _ = o.find(b"a").unwrap();
	}

	#[test]
	fn database_merge()
	{
		let t = tempfile::TempDir::new().unwrap();
		{
			{
				use std::io::Write;
				let mut main = std::fs::File::create(t.path().join("main")).unwrap();
				main.write_all(&[0u8]).unwrap();
			}

			let mut tx = CreateTx::new(t.path()).unwrap();
			tx.add_record("a", "U", &[0,0,0,0,0,0,0,0]).unwrap();
			tx.add_record("a", "U", &[1,0,0,0,0,0,0,0]).unwrap();
			tx.add_record("c", "U", &[0,0,0,0,0,0,0,0]).unwrap();
			tx.add_record("c", "U", &[1,0,0,0,0,0,0,0]).unwrap();
			tx.commit().unwrap();

			let mut tx = CreateTx::new(t.path()).unwrap();
			tx.add_record("b", "U", &[0,0,0,0,0,0,0,0]).unwrap();
			tx.add_record("b", "U", &[1,0,0,0,0,0,0,0]).unwrap();
			tx.add_record("d", "U", &[0,0,0,0,0,0,0,0]).unwrap();
			tx.add_record("d", "U", &[1,0,0,0,0,0,0,0]).unwrap();
			tx.commit().unwrap();
		}

		let r = DatabaseReader::new(t.path()).unwrap();
		assert_eq!(r.transaction_paths().len(), 3);
		let mut reader = r.get_range(..);
		assert_eq!(reader.next().unwrap().key(), "a");
		assert_eq!(reader.next().unwrap().key(), "a");
		assert_eq!(reader.next().unwrap().key(), "b");
		assert_eq!(reader.next().unwrap().key(), "b");
		assert_eq!(reader.next().unwrap().key(), "c");
		assert_eq!(reader.next().unwrap().key(), "c");
		assert_eq!(reader.next().unwrap().key(), "d");
		assert_eq!(reader.next().unwrap().key(), "d");
	}

	#[test]
	fn database_merge_last()
	{
		let t = tempfile::TempDir::new().unwrap();
		{
			{
				use std::io::Write;
				let mut main = std::fs::File::create(t.path().join("main")).unwrap();
				main.write_all(&[0u8]).unwrap();
			}

			let mut tx = CreateTx::new(t.path()).unwrap();
			tx.add_record("a", "U", &[0,0,0,0,0,0,0,0,1]).unwrap();
			tx.commit().unwrap();

			let mut tx = CreateTx::new(t.path()).unwrap();
			tx.add_record("a","U", &[0,0,0,0,0,0,0,0,2]).unwrap();
			tx.commit().unwrap();
		}

		let r = DatabaseReader::new(t.path()).unwrap();
		let last = r.get_range(..).next().unwrap();
		assert_eq!(last.value()[8], 2);
	}

}
