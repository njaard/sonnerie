use crate::database_reader::DatabaseReader;
use crate::formatted::*;
use crate::segment_reader::*;
use crate::write::Writer;
use crate::CreateTx;
use crate::Reader;

use std::io::BufWriter;

use byteorder::*;

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
	let o = Reader::new(w).unwrap();
	let s = o.get("ab");
	let mut i = s.into_iter();
	let a = i.next().unwrap();
	assert_eq!(a.key(), "ab");
	assert_eq!(a.format(), "u");
	assert_eq!(a.value(), b"\0\0\0\0\0\0\0\0\0\0\0\0");
	let a = i.next().unwrap();
	assert_eq!(a.key(), "ab");
	assert_eq!(a.format(), "u");
	assert_eq!(a.value(), b"\0\0\0\0\0\0\0\x01\0\0\0\x01");
	let a = i.next().unwrap();
	assert_eq!(a.key(), "ab");
	assert_eq!(a.format(), "u");
	assert_eq!(a.value(), b"\0\0\0\0\0\0\0\x02\0\0\0\x02");
	let a = i.next().unwrap();
	assert_eq!(a.key(), "ab");
	assert_eq!(a.format(), "u");
	assert_eq!(a.value(), b"\0\0\0\0\0\0\0\x03\x03\0\0\x03");

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
	let o = Reader::new(w).unwrap();
	let s = o.get_range(..);
	let mut i = s.into_iter();
	let a = i.next().unwrap();
	assert_eq!(a.key(), "a");
	assert_eq!(a.format(), "u");
	assert_eq!(a.value(), b"\0\0\0\0\0\0\0\0\0\0\0\0");
	let a = i.next().unwrap();
	assert_eq!(a.key(), "a");
	assert_eq!(a.format(), "u");
	assert_eq!(a.value(), b"\0\0\0\0\0\0\0\x01\0\0\0\x01");
	dbg!("next");
	let a = i.next().unwrap();
	assert_eq!(a.key(), "b");
	assert_eq!(a.format(), "u");
	assert_eq!(a.value(), b"\0\0\0\0\0\0\0\x02\0\0\0\x02");
	let a = i.next().unwrap();
	assert_eq!(a.key(), "b");
	assert_eq!(a.format(), "u");
	assert_eq!(a.value(), b"\0\0\0\0\0\0\0\x03\x03\0\0\x03");

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
	let o = Reader::new(w).unwrap();
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
	let o = Reader::new(w).unwrap();
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
	let o = Reader::new(w).unwrap();
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
	let o = Reader::new(w).unwrap();
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
	let o = Reader::new(w).unwrap();
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
	let o = SegmentReader::open(&mut w).unwrap();
	o.print_info(&mut std::io::stderr()).unwrap();
	let _ = o.find(b"a").unwrap();
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
	let last = r.get_range(..).next().unwrap();
	assert_eq!(last.value()[8], 2);
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
	let o = Reader::new(w).unwrap();
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
	let o = Reader::new(w).unwrap();
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
	let o = SegmentReader::open(&mut f).unwrap();
	{
		let f = o.first().unwrap();
		assert!(o.segment_after(&f).is_some());
	}

	let w = std::fs::File::open(t.path().join("w")).unwrap();
	let o = Reader::new(w).unwrap();
	let s = o.get("aa").count();
	assert_eq!(s, 1050000);
}
