use crate::database_reader::DatabaseReader;
use crate::formatted::*;
use crate::segment_reader::*;
use crate::write::Writer;
use crate::CreateTx;
use crate::Reader;

use core::mem::drop;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::io::BufWriter;

use byteorder::*;

#[cfg(test)]
fn make_keynames() -> impl Iterator<Item = String> {
	const MAX_KEYLEN: usize = 8;

	struct MakeKeynames {
		current: Vec<u8>,
	}
	impl Iterator for MakeKeynames {
		type Item = String;
		fn next(&mut self) -> Option<String> {
			let mut popping = false;
			while let Some(&back) = self.current.last() {
				if back == b'z' {
					self.current.pop();
					popping = true;
				} else {
					break;
				}
			}

			if !popping && self.current.len() < MAX_KEYLEN - 1 {
				self.current.push(b'a');
			} else if let Some(back) = self.current.last_mut() {
				*back += 1;
			}

			Some(String::from_utf8(self.current.clone()).unwrap())
		}
	}

	MakeKeynames {
		current: Vec::with_capacity(MAX_KEYLEN),
	}
}

#[cfg(test)]
fn make_big_database(count: usize) -> (tempfile::TempDir, DatabaseReader) {
	use rand::*;
	let t = tempfile::TempDir::new().unwrap();

	let mut tx = CreateTx::new(t.path()).unwrap();

	let mut random_values = rand::rngs::SmallRng::seed_from_u64(2001);

	let mut total = 0u64;

	let mut last = String::new();
	for name in make_keynames().take(count) {
		let n_timestamps = random_values.next_u32() % 1000;

		for timestamp in 0..n_timestamps {
			let mut buf = [0; 12];
			byteorder::BigEndian::write_u64(&mut buf[..], timestamp as u64);
			random_values.fill_bytes(&mut buf[8..]);
			tx.add_record(&name, "u", &buf[..]).unwrap();
			total += 1;
		}
		last = name;
	}

	tx.commit_to(&t.path().join("main")).expect("committed");
	eprintln!("wrote {} records (last={})", total, last);

	let r = DatabaseReader::new(t.path()).unwrap();

	(t, r)
}

#[test]
fn basic1() {
	let t = tempfile::TempDir::new().unwrap();

	{
		let w = std::fs::File::create(t.path().join("w")).unwrap();
		let w = BufWriter::new(w);

		let mut w = Writer::new(w);
		w.add_record("ab", "u", b"\0\0\0\0\0\0\0\0\0\0\0\0")
			.unwrap();
		w.add_record("ab", "u", b"\0\0\0\0\0\0\0\x01\0\0\0\x01")
			.unwrap();
		w.add_record("ab", "u", b"\0\0\0\0\0\0\0\x02\0\0\0\x02")
			.unwrap();
		w.add_record("ab", "u", b"\0\0\0\0\0\0\0\x03\x03\0\0\x03")
			.unwrap();
		w.finish().unwrap();
	}

	let w = std::fs::File::open(t.path().join("w")).unwrap();
	let o = Reader::new(w).unwrap().left().unwrap();
	let s = o.get("ab");
	let mut i = s.into_iter();
	let a = i.next().unwrap();
	assert_eq!(a.key(), "ab");
	assert_eq!(a.format(), "u");
	assert_eq!(a.raw(), b"\0\0\0\0\0\0\0\0\0\0\0\0");
	let a = i.next().unwrap();
	assert_eq!(a.key(), "ab");
	assert_eq!(a.format(), "u");
	assert_eq!(a.raw(), b"\0\0\0\0\0\0\0\x01\0\0\0\x01");
	let a = i.next().unwrap();
	assert_eq!(a.key(), "ab");
	assert_eq!(a.format(), "u");
	assert_eq!(a.raw(), b"\0\0\0\0\0\0\0\x02\0\0\0\x02");
	let a = i.next().unwrap();
	assert_eq!(a.key(), "ab");
	assert_eq!(a.format(), "u");
	assert_eq!(a.raw(), b"\0\0\0\0\0\0\0\x03\x03\0\0\x03");

	assert!(i.next().is_none());
}

#[test]
fn basic3() {
	let t = tempfile::TempDir::new().unwrap();

	{
		let w = std::fs::File::create(t.path().join("w")).unwrap();
		let w = BufWriter::new(w);

		let mut w = Writer::new(w);
		w.add_record("a", "u", b"\0\0\0\0\0\0\0\0\0\0\0\0").unwrap();
		w.add_record("a", "u", b"\0\0\0\0\0\0\0\x01\0\0\0\x01")
			.unwrap();
		w.add_record("b", "u", b"\0\0\0\0\0\0\0\x02\0\0\0\x02")
			.unwrap();
		w.add_record("b", "u", b"\0\0\0\0\0\0\0\x03\x03\0\0\x03")
			.unwrap();
		w.finish().unwrap();
	}

	let w = std::fs::File::open(t.path().join("w")).unwrap();
	let o = Reader::new(w).unwrap().left().unwrap();
	let s = o.get_range(..);
	let mut i = s.into_iter();
	let a = i.next().unwrap();
	assert_eq!(a.key(), "a");
	assert_eq!(a.format(), "u");
	assert_eq!(a.raw(), b"\0\0\0\0\0\0\0\0\0\0\0\0");
	let a = i.next().unwrap();
	assert_eq!(a.key(), "a");
	assert_eq!(a.format(), "u");
	assert_eq!(a.raw(), b"\0\0\0\0\0\0\0\x01\0\0\0\x01");
	dbg!("next");
	let a = i.next().unwrap();
	assert_eq!(a.key(), "b");
	assert_eq!(a.format(), "u");
	assert_eq!(a.raw(), b"\0\0\0\0\0\0\0\x02\0\0\0\x02");
	let a = i.next().unwrap();
	assert_eq!(a.key(), "b");
	assert_eq!(a.format(), "u");
	assert_eq!(a.raw(), b"\0\0\0\0\0\0\0\x03\x03\0\0\x03");

	assert!(i.next().is_none());
}

fn write_many<W: std::io::Write + Send>(w: &mut Writer<W>, key: &str, range: std::ops::Range<u32>) {
	for n in range {
		let mut buf = [0u8; 12];
		byteorder::BigEndian::write_u64(&mut buf[..], n as u64);
		byteorder::BigEndian::write_u32(&mut buf[8..12], n);
		w.add_record(key, "u", &buf).unwrap();
	}
}
fn write_many_u64<W: std::io::Write + Send>(
	w: &mut Writer<W>,
	key: &str,
	range: std::ops::Range<u64>,
) {
	for n in range {
		let mut buf = [0u8; 16];
		byteorder::BigEndian::write_u64(&mut buf[..], n as u64);
		byteorder::BigEndian::write_u64(&mut buf[8..16], n);
		w.add_record(key, "U", &buf).unwrap();
	}
}

#[test]
fn basic2() {
	let t = tempfile::TempDir::new().unwrap();

	{
		let w = std::fs::File::create(t.path().join("w")).unwrap();
		let w = BufWriter::new(w);

		let mut w = Writer::new(w);
		write_many(&mut w, "aa", 0..70000);
		write_many(&mut w, "aabq", 0..50000);
		write_many(&mut w, "n", 0..10000);
		w.finish().unwrap();
	}

	let w = std::fs::File::open(t.path().join("w")).unwrap();
	let o = Reader::new(w).unwrap().left().unwrap();
	o.print_info(&mut std::io::stderr()).unwrap();
	let s = o.get("aa");
	let i = s.into_iter();
	assert_eq!(i.count(), 70000);
	let s = o.get("aabq");
	let i = s.into_iter();
	assert_eq!(i.count(), 50000);
	let s = o.get("aac");
	let i = s.into_iter();
	assert_eq!(i.count(), 0);
	let s = o.get("n");
	let i = s.into_iter();
	assert_eq!(i.count(), 10000);
}

#[test]
fn basic_huge() {
	let t = tempfile::TempDir::new().unwrap();

	{
		let w = std::fs::File::create(t.path().join("w")).unwrap();
		let w = BufWriter::new(w);

		let mut w = Writer::new(w);
		for a in &["a", "b", "c", "d", "e", "f", "g"] {
			for b in &["a", "b", "c", "d", "e", "f", "g"] {
				for c in &["a", "b", "c", "d", "e", "f", "g"] {
					let n = format!("{}{}{}", a, b, c);
					write_many(&mut w, &n, 0..1000);
					if n == "abc" {
						write_many(&mut w, &n, 1000..901000);
					}
				}
			}
		}
		w.finish().unwrap();
	}

	let w = std::fs::File::open(t.path().join("w")).unwrap();
	let o = Reader::new(w).unwrap().left().unwrap();
	let s = o.get("abc");
	let i = s.into_iter();
	assert_eq!(i.count(), 901000);
}

#[test]
fn range_before() {
	let t = tempfile::TempDir::new().unwrap();

	{
		{
			let mut tx = CreateTx::new(t.path()).expect("creating tx");
			let data = "aa 2010-01-01_00:00:00 10\n\
				bb 2010-01-02_00:00:00 20\n\
				cc 2010-01-03_00:00:00 20\n\
				";

			add_from_stream(&mut tx, "u", &mut std::io::Cursor::new(data), Some("%F_%T"))
				.expect("writing");
			tx.commit_to(&t.path().join("main")).expect("committed");
		}
	}

	let w = std::fs::File::open(t.path().join("main")).unwrap();
	let o = Reader::new(w).unwrap().left().unwrap();
	let s = o.get_range(..="bb");
	assert_eq!(s.into_iter().count(), 2);
	let s = o.get_range(.."bb");
	assert_eq!(s.into_iter().count(), 1);
}

#[test]
fn multicolumn() {
	let t = tempfile::TempDir::new().unwrap();

	{
		let mut tx = CreateTx::new(t.path()).expect("creating tx");
		let data = "a 2010-01-01_00:00:00 10 20\n\
			a 2010-01-02_00:00:00 20 30\n";

		add_from_stream(
			&mut tx,
			"uu",
			&mut std::io::Cursor::new(data),
			Some("%F_%T"),
		)
		.expect("writing");
		tx.commit_to(&t.path().join("main")).expect("committed");
	}

	let w = std::fs::File::open(t.path().join("main")).unwrap();
	let o = Reader::new(w).unwrap().left().unwrap();
	let s = o.get_range("a".."z");
	let mut i = s.into_iter();

	let mut out = vec![];
	print_record(
		&i.next().expect("row1"),
		&mut out,
		PrintTimestamp::FormatString("%F_%T"),
		PrintRecordFormat::Yes,
	)
	.expect("formatting");
	std::io::Write::write_all(&mut out, b"\n").unwrap();
	print_record(
		&i.next().expect("row2"),
		&mut out,
		PrintTimestamp::FormatString("%F_%T"),
		PrintRecordFormat::Yes,
	)
	.expect("formatting");
	assert!(i.next().is_none());

	assert_eq!(
		&String::from_utf8(out).unwrap(),
		"\
			a\t2010-01-01_00:00:00\tuu\t10 20\n\
			a\t2010-01-02_00:00:00\tuu\t20 30\
		"
	);
}
#[test]
#[should_panic]
fn violate_time_order() {
	let t = tempfile::TempDir::new().unwrap();

	{
		let mut tx = CreateTx::new(t.path()).expect("creating tx");
		let data = "a 2010-01-01_00:00:00 10\n\
			a 2010-01-01_00:00:00 20\n";

		add_from_stream(&mut tx, "u", &mut std::io::Cursor::new(data), Some("%F_%T"))
			.expect("writing");
		tx.commit_to(&t.path().join("main")).expect("committed");
	}
}

#[test]
fn multicolumn_string() {
	let t = tempfile::TempDir::new().unwrap();

	let data = "\
		a\t2010-01-01_00:00:00\tss\tMany\\ words Lotsa\\ stuff\\ here\n\
		b\t2010-01-02_00:00:00\tsu\tFluffy\\ cat 42\n\
		c\t2010-01-01_00:00:00\tus\t900 It's\\ a\\ cat!\
		";
	{
		let mut tx = CreateTx::new(t.path()).expect("creating tx");

		add_from_stream_with_fmt(&mut tx, &mut std::io::Cursor::new(data), Some("%F_%T"))
			.expect("writing");
		tx.commit_to(&t.path().join("main")).expect("committed");
	}

	let w = std::fs::File::open(t.path().join("main")).unwrap();
	let o = Reader::new(w).unwrap().left().unwrap();
	let s = o.get_range("a".."z");
	let mut i = s.into_iter();

	let mut out = vec![];
	print_record(
		&i.next().expect("row1"),
		&mut out,
		PrintTimestamp::FormatString("%F_%T"),
		PrintRecordFormat::Yes,
	)
	.expect("formatting");
	std::io::Write::write_all(&mut out, b"\n").unwrap();
	print_record(
		&i.next().expect("row2"),
		&mut out,
		PrintTimestamp::FormatString("%F_%T"),
		PrintRecordFormat::Yes,
	)
	.expect("formatting");
	std::io::Write::write_all(&mut out, b"\n").unwrap();
	print_record(
		&i.next().expect("row3"),
		&mut out,
		PrintTimestamp::FormatString("%F_%T"),
		PrintRecordFormat::Yes,
	)
	.expect("formatting");
	assert!(i.next().is_none());

	assert_eq!(&String::from_utf8(out).unwrap(), data,);
}

#[test]
fn write() {
	let t = tempfile::TempDir::new().unwrap();

	{
		let w = std::fs::File::create(t.path().join("w")).unwrap();
		let w = BufWriter::new(w);

		let mut w = Writer::new(w);
		w.add_record("a", "u", b"\0\0\0\0\0\0\0\0\0\0\0\0").unwrap();
		w.finish().unwrap();
	}

	let mut w = std::fs::File::open(t.path().join("w")).unwrap();
	let o = SegmentReader::open(&mut w).unwrap().left().unwrap();
	o.print_info(&mut std::io::stderr()).unwrap();
	let _ = o.find("a").unwrap();
}

#[test]
fn database_merge1() {
	let t = tempfile::TempDir::new().unwrap();
	{
		{
			use std::io::Write;
			let mut main = std::fs::File::create(t.path().join("main")).unwrap();
			main.write_all(&[0u8]).unwrap();
		}

		let mut tx = CreateTx::new(t.path()).unwrap();
		tx.add_record("a", "U", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
			.unwrap();
		tx.add_record("a", "U", &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
			.unwrap();
		tx.add_record("c", "U", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
			.unwrap();
		tx.add_record("c", "U", &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
			.unwrap();
		tx.commit().unwrap();

		let mut tx = CreateTx::new(t.path()).unwrap();
		tx.add_record("b", "U", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
			.unwrap();
		tx.add_record("b", "U", &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
			.unwrap();
		tx.add_record("d", "U", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
			.unwrap();
		tx.add_record("d", "U", &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
			.unwrap();
		tx.commit().unwrap();
	}

	let r = DatabaseReader::new(t.path()).unwrap();
	assert_eq!(r.transaction_paths().len(), 3);
	let mut reader = r.get_range(..).into_iter();
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
#[should_panic]
fn correct_size() {
	let t = tempfile::TempDir::new().unwrap();
	{
		use std::io::Write;
		let mut main = std::fs::File::create(t.path().join("main")).unwrap();
		main.write_all(&[0u8]).unwrap();
	}
	{
		let mut tx = CreateTx::new(t.path()).unwrap();
		tx.add_record("a", "U", &[0, 0, 0, 0, 0, 0, 0, 0, 1])
			.unwrap();
		tx.commit().unwrap();
	}
}

#[test]
fn database_merge_last() {
	let t = tempfile::TempDir::new().unwrap();
	{
		{
			use std::io::Write;
			let mut main = std::fs::File::create(t.path().join("main")).unwrap();
			main.write_all(&[0u8]).unwrap();
		}

		let mut tx = CreateTx::new(t.path()).unwrap();
		tx.add_record("a", "U", &[0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0])
			.unwrap();
		tx.commit().unwrap();

		let mut tx = CreateTx::new(t.path()).unwrap();
		tx.add_record("a", "U", &[0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0])
			.unwrap();
		tx.commit().unwrap();
	}

	let r = DatabaseReader::new(t.path()).unwrap();
	let last = r.get_range(..).into_iter().next().unwrap();
	assert_eq!(last.raw()[8], 2);
}

#[test]
fn store_string1() {
	let t = tempfile::TempDir::new().unwrap();
	let data = "\
		a\t2010-01-04_00:00:00\ts\tHello\n\
		";

	{
		let mut tx = CreateTx::new(t.path()).expect("creating tx");

		add_from_stream_with_fmt(&mut tx, &mut std::io::Cursor::new(data), Some("%F_%T"))
			.expect("writing");
		tx.commit_to(&t.path().join("main")).expect("committed");
	}

	let w = std::fs::File::open(t.path().join("main")).unwrap();
	let o = Reader::new(w).unwrap().left().unwrap();
	let s = o.get_range("a".."z");
	let mut i = s.into_iter();

	let mut out = vec![];
	print_record(
		&i.next().expect("row1"),
		&mut out,
		PrintTimestamp::FormatString("%F_%T"),
		PrintRecordFormat::Yes,
	)
	.expect("formatting");
	assert!(i.next().is_none());

	assert_eq!(
		&String::from_utf8(out).unwrap(),
		"a\t2010-01-04_00:00:00\ts\tHello"
	);
}

#[test]
fn escape_invocation() {
	let t = tempfile::TempDir::new().unwrap();
	{
		let _ = std::fs::File::create(t.path().join("main")).unwrap();

		let mut tx = CreateTx::new(t.path()).unwrap();
		let mut naughty = vec![0, 0, 0, 0, 0, 0, 0, 0, 14];
		assert_eq!(14, crate::segment::SEGMENT_INVOCATION.len());
		naughty.extend_from_slice(crate::segment::SEGMENT_INVOCATION);

		tx.add_record("b", "s", &naughty).unwrap();
		tx.add_record("c", "u", &[0, 0, 0, 0, 0, 0, 0, 0, 0x42, 42, 0, 0])
			.unwrap();
		tx.commit().unwrap();
	}

	let r = DatabaseReader::new(t.path()).unwrap();
	let last = r.get_range(..).into_iter();
	assert_eq!(last.count(), 2);
}

#[test]
fn homogenic_types() {
	let t = tempfile::TempDir::new().unwrap();
	let data = "\
		a\t2010-01-01_00:00:01\tu\t42\n\
		a\t2010-01-01_00:00:02\tu\t84\n\
		a\t2010-01-01_00:00:03\tf\t32.5\n\
		a\t2010-01-01_00:00:04\ts\tHello\n\
		";

	{
		let mut tx = CreateTx::new(t.path()).expect("creating tx");

		add_from_stream_with_fmt(&mut tx, &mut std::io::Cursor::new(data), Some("%F_%T"))
			.expect("writing");
		tx.commit_to(&t.path().join("main")).expect("committed");
	}

	let w = std::fs::File::open(t.path().join("main")).unwrap();
	let o = Reader::new(w).unwrap().left().unwrap();
	let s = o.get_range("a".."z");
	let mut i = s.into_iter();

	let mut out = vec![];
	for _ in 0..4 {
		print_record(
			&i.next().expect("row1"),
			&mut out,
			PrintTimestamp::FormatString("%F_%T"),
			PrintRecordFormat::Yes,
		)
		.expect("formatting");
		std::io::Write::write_all(&mut out, b"\n").unwrap();
	}
	assert_eq!(
		&String::from_utf8(out).unwrap(),
		"\
			a\t2010-01-01_00:00:01\tu\t42\n\
			a\t2010-01-01_00:00:02\tu\t84\n\
			a\t2010-01-01_00:00:03\tf\t32.50000000000000000\n\
			a\t2010-01-01_00:00:04\ts\tHello\n\
		"
	);
}

#[test]
fn keys_split() {
	let t = tempfile::TempDir::new().unwrap();
	{
		let w = std::fs::File::create(t.path().join("w")).unwrap();
		let w = BufWriter::new(w);

		let mut w = Writer::new(w);
		write_many(&mut w, "aa", 0..70000);
		write_many_u64(&mut w, "aa", 70000..600000);
		write_many(&mut w, "aa", 600000..1000000);
		write_many_u64(&mut w, "aa", 1000000..1010000);
		write_many(&mut w, "aa", 1010000..1030000);
		write_many_u64(&mut w, "aa", 1030000..1040000);
		write_many(&mut w, "aa", 1040000..1050000);
		w.finish().unwrap();
	}
	let mut f = std::fs::File::open(&t.path().join("w")).unwrap();
	let o = SegmentReader::open(&mut f).unwrap().left().unwrap();
	{
		let f = o.first().unwrap();
		assert!(o.segment_after(&f).is_some());
	}

	let w = std::fs::File::open(t.path().join("w")).unwrap();
	let o = Reader::new(w).unwrap().left().unwrap();
	let s = o.get("aa").count();
	assert_eq!(s, 1050000);
}

#[test]
fn parallel_split1() {
	let (_t, db) = make_big_database(1000);

	let s = db.get_range(..).into_par_iter().count();
	assert_eq!(s, 491739);
}

#[test]
fn parallel_split2() {
	let (_t, db) = make_big_database(100000);

	let s = db.get_range(..).into_par_iter().count();
	assert_eq!(s, 49922574);
}

#[test]
fn parallel_split3() {
	let (_t, db) = make_big_database(100000);

	{
		let sp = db.get_range(.."aaaaa").into_par_iter().count();
		let ss = db.get_range(.."aaaaa").count();
		assert_eq!(ss, 1095);
		assert_eq!(ss, sp);
	}
	{
		let ss = db.get_range("aaaaa".."aaaaaa").into_par_iter().count();
		let sp = db.get_range("aaaaa".."aaaaaa").count();
		assert_eq!(ss, sp);
		assert_eq!(ss, 257);
	}
	{
		let ss = db.get_range("aaafaaa"..).into_par_iter().count();
		let sp = db.get_range("aaafaaa"..).count();
		assert_eq!(ss, sp);
		assert_eq!(ss, 7655209);
	}
	{
		let ss = db.get_range("aaafaaa"..).into_par_iter().count();
		let sp = db.get_range("aaafaaa"..).count();
		assert_eq!(ss, sp);
		assert_eq!(ss, 7655209);
	}
	{
		let ss = db.get_range("aaaf0aa"..).into_par_iter().count();
		let sp = db.get_range("aaaf0aa"..).count();
		assert_eq!(ss, 7656573);
		assert_eq!(ss, sp);
	}
	{
		let ss = db.get_range(.."aaaafek0").into_par_iter().count();
		let sp = db.get_range(.."aaaafek0").count();
		assert_eq!(ss, sp);
		assert_eq!(ss, 1741776);
	}
	{
		let ss = db.get_range(.."aaaaibw0").into_par_iter().count();
		let sp = db.get_range(.."aaaaibw0").count();
		assert_eq!(ss, sp);
		assert_eq!(ss, 2699702);
	}

	{
		let mut w = std::fs::File::open(_t.path().join("main")).unwrap();
		let segs = SegmentReader::open(&mut w).unwrap().left().unwrap();
		let mut seg = segs.first();
		while seg.is_some() {
			let s = seg.as_ref().unwrap();
			let n = format!("{}0", s.last_key);
			db.get_range(..n.as_str()).into_par_iter().for_each(|_| {});

			seg = segs.segment_after(s);
		}
	}
}

#[test]
fn parallel_very_slow() {
	let (_t, db) = make_big_database(1000000);

	let s = db.get_range(..).into_par_iter().count();
	assert_eq!(s, 499471998);
}

#[test]
fn high_level_reader() {
	let t = tempfile::TempDir::new().unwrap();
	let data = "\
		a\t2010-01-01_00:00:01\tu\t42\n\
		a\t2010-01-01_00:00:02\tu\t84\n\
		a\t2010-01-01_00:00:03\tu\t66\n\
		b\t2010-01-01_00:00:01\tFf\t34.0\t22.0\n\
		b\t2010-01-01_00:00:02\tFf\t3.1415\t2.7182\n\
		c\t2010-01-01_00:00:01\tss\tHello\\ World Rustacean\n\
		";

	{
		let mut tx = CreateTx::new(t.path()).expect("creating tx");

		add_from_stream_with_fmt(&mut tx, &mut std::io::Cursor::new(data), Some("%F_%T"))
			.expect("writing");
		tx.commit_to(&t.path().join("main")).expect("committed");
	}
	let r = DatabaseReader::new(t.path()).unwrap();
	let a: Vec<u64> = r.get("a").map(|m| m.value()).collect();
	assert_eq!(a, vec![42, 84, 66]);
	let a: Vec<(f64, f64)> = r.get("b").map(|m| (m.get(0), m.get(1))).collect();
	assert_eq!(
		format!("{:.4?}", a),
		"[(34.0000, 22.0000), (3.1415, 2.7182)]"
	);
	let a: Vec<(String, String)> = r.get("c").map(|m| (m.get(0), m.get(1))).collect();
	assert_eq!(
		a,
		vec![("Hello World".to_string(), "Rustacean".to_string())]
	);
}

#[test]
fn string_records() {
	let t = tempfile::TempDir::new().unwrap();
	let data = "\
		ab\t2010-01-01_00:00:01\ts\tHello1\n\
		ab\t2010-01-01_00:00:02\ts\tHello\\ World\n\
		ab\t2010-01-01_00:00:03\ts\tHello\\ Planet\n\
		ab\t2010-01-01_00:00:04\ts\tHello\\ Universe\n\
		";

	{
		let mut tx = CreateTx::new(t.path()).expect("creating tx");

		add_from_stream_with_fmt(&mut tx, &mut std::io::Cursor::new(data), Some("%F_%T"))
			.expect("writing");
		tx.commit_to(&t.path().join("main")).expect("committed");
	}
	let r = DatabaseReader::new(t.path()).unwrap();
	let a: Vec<String> = r.get("ab").map(|m| m.value()).collect();
	assert_eq!(
		&format!("{a:?}"),
		"[\"Hello1\", \"Hello World\", \"Hello Planet\", \"Hello Universe\"]"
	);
}

#[test]
fn delete_all() {
    use chrono::naive::NaiveDateTime;

    let (t, _) = make_big_database(65536);

    {
        let mut tx = CreateTx::new(t.path()).unwrap();
        tx.delete("", "", 0, u64::MAX, "%");
        tx.commit();
    }

    let db = DatabaseReader::new(t.path()).unwrap();
    assert_eq!(0, db.get_range(..).into_par_iter().count());
}

// this is a generalized form of the delete test with various flags for which
// test is active
fn configurable_delete_test(
    with_time_start: bool,
    with_time_end: bool,
    with_key_start: bool,
    with_key_end: bool,
    wildcard: &str,
    merge: bool,
    major_if_merge: bool,
) {
    use chrono::naive::NaiveDateTime;
    use either::Either::*;

    let (t, db) = make_big_database(512);
    let mut times = db
        .get_range(..)
        .into_iter()
        .map(|r| r.time())
        .collect::<Vec<_>>();
    times.sort_unstable();
    times.dedup();

    let mut keys = db
        .get_range(..)
        .into_iter()
        .map(|r| r.key().to_owned())
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    let mut keys = keys
        .into_iter()
        .collect::<Vec<_>>();

    dbg!(&times);

    let begin_time = with_time_start
        .then(|| times[times.len() / 3]);
    let end_time = with_time_end
        .then(|| times[times.len() * 2 / 3]);
    let begin_key = with_key_start
        .then(|| keys[keys.len() / 3].clone());
    let end_key = with_key_end
        .then(|| keys[keys.len() * 2 / 3].clone());

    drop(times);
    drop(keys);

    // perform deletion
    {
        let mut tx = CreateTx::new(t.path()).unwrap();
        tx.delete(
            begin_key.as_deref().unwrap_or(""),
            end_key.as_deref().unwrap_or(""),
            dbg!(begin_time.map(|t| t.timestamp_nanos() as u64).unwrap_or(0)),
            dbg!(end_time.map(|t| t.timestamp_nanos() as u64).unwrap_or(u64::MAX)),
            wildcard,
        );
        tx.commit();

        if merge {
            if major_if_merge {
                crate::compact(t.path(), true, None, "%FT%T");
            }

            else {
                crate::compact(t.path(), false, None, "%FT%T");
            }
        }
    }

    // perform read
    let db = DatabaseReader::new(t.path()).unwrap();
    let wildcard = match crate::wildcard::Wildcard::new(wildcard).as_regex() {
        Some(re) => Left(re),
        None => Right(wildcard.split("%").next().unwrap()),
    };

    for record in db.get_range(..).into_iter() {
        let time = record.time();
        match (begin_time.as_ref(), end_time.as_ref()) {
            (Some(b), Some(e)) => {
                assert!(dbg!(time) < dbg!(*b) || dbg!(*e) < time);
                assert!(!(*b <= time && time <= *e));
            },

            (Some(b), _) => {
                assert!(time < *b);
                assert!(!(*b <= time));
            },

            (_, Some(e)) => {
                assert!(*e < time);
                assert!(!(time <= *e));
            },

            // unreachable in a sense that there should be no record
            _ => unreachable!(),
        }

        let key = record.key();
        match (begin_key.as_deref(), end_key.as_deref()) {
            (Some(b), Some(e)) => {
                assert!(key < b || e < key);
                assert!(!(b <= key && key <= e));
            },

            (Some(b), _) => {
                assert!(key < b);
                assert!(!(b <= key));
            },

            (_, Some(e)) => {
                assert!(e < key);
                assert!(!(key <= e));
            },

            _ => {},
        }

        match &wildcard {
            Left(re) => assert!(re.is_match(key)),
            Right(start) => assert!(key.starts_with(start)),
        }
    };
}

// TODO: if this test can be split into individual tests using a macro, be my
// guest
#[test]
fn generalized_delete() {
    for with_time_start in vec![false, true] {
        for with_time_end in vec![false, true] {
            for with_key_start in vec![false, true] {
                for with_key_end in vec![false, true] {
                    for wildcard in vec!["%", "a%", "%a", "a%a", "%a%"] {
                        for merge in [false, true] {
                            if merge {
                                for major_if_merge in [false, true] {
                                    configurable_delete_test(
                                        with_time_start,
                                        with_time_end,
                                        with_key_start,
                                        with_key_end,
                                        wildcard,
                                        merge,
                                        major_if_merge,
                                    );
                                }
                            }

                            else {
                                configurable_delete_test(
                                    with_time_start,
                                    with_time_end,
                                    with_key_start,
                                    with_key_end,
                                    wildcard,
                                    merge,
                                    false
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
