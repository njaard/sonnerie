//! Read or write formatted data to a text stream.

use crate::row_format::*;
use crate::Timestamp;
use byteorder::ByteOrder;
use escape_string::split_one;

/// Read keys from a text stream and insert it into a transaction
///
/// Parameters:
/// * `tx` - a transaction to write into
/// * `db` - the database that is type-checked against
/// * `format` - the format of each row. If each row
/// contains its own format, you can instead use [`add_from_stream_with_fmt`].
/// * `input` - a text stream to read from, the keys are formatted as
/// `label timestamp value [value ...]`. Whitespace is escaped with a backslash.
/// * `timestamp` - the strftime-like format to parse timestamps as. If `None`, use
/// epoch nanos.
/// * `nocheck` - turns off slow type checking (with `db`).
pub fn add_from_stream<R: std::io::BufRead>(
	tx: &mut crate::CreateTx,
	format: &str,
	input: &mut R,
	timestamp_format: Option<&str>,
) -> Result<(), crate::WriteFailure> {
	let row_format = parse_row_format(format);

	let mut line = String::new();
	let mut row_data = vec![]; // this is single allocation only

	while 0 != input.read_line(&mut line).unwrap() {
		let tail = line.trim_end();
		if tail.is_empty() {
			continue;
		}
		let (key, tail) = split_one(tail).unwrap();
		let (timestamp, tail) = split_one(tail).unwrap();
		let ts: Timestamp;
		if let Some(f) = timestamp_format.as_ref() {
			let n = chrono::NaiveDateTime::parse_from_str(&timestamp, f)
				.expect("parsing timestamp according to format");
			ts = n.timestamp_nanos() as Timestamp;
		} else {
			ts = timestamp.parse().expect("parsing timestamp");
		}

		row_format
			.to_stored_format(ts, tail, &mut row_data)
			.unwrap_or_else(|_| panic!("parsing values \"{}\"", tail));

		tx.add_record_raw(&key, format, &row_data)?;
		row_data.clear();
		line.clear();
	}

	Ok(())
}

/// Reads from text, each record reports its own format.
///
/// Like [`add_from_stream`] except the format string
/// comes after the timestamp
pub fn add_from_stream_with_fmt<R: std::io::BufRead>(
	tx: &mut crate::CreateTx,
	input: &mut R,
	timestamp_format: Option<&str>,
) -> Result<(), crate::WriteFailure> {
	let mut line = String::new();
	let mut row_data = vec![];

	while 0 != input.read_line(&mut line).unwrap() {
		let tail = line.trim_end();
		if tail.is_empty() {
			continue;
		}
		let (key, tail) = split_one(tail).unwrap();
		let (timestamp, tail) = split_one(tail).unwrap();
		let ts: Timestamp;
		if let Some(f) = timestamp_format.as_ref() {
			let n = chrono::NaiveDateTime::parse_from_str(&timestamp, f)
				.expect("parsing timestamp according to format");
			ts = n.timestamp_nanos() as Timestamp;
		} else {
			ts = timestamp.parse().expect("parsing timestamp");
		}

		let (format, values) = split_one(tail).unwrap();
		let row_format = parse_row_format(&format);

		row_format
			.to_stored_format(ts, values, &mut row_data)
			.unwrap();

		tx.add_record_raw(&key, &format, &row_data)?;
		row_data.clear();
		line.clear();
	}

	Ok(())
}

/// Print the record format (`uUfF`) right after the timestamp
#[derive(Debug, Copy, Clone)]
pub enum PrintRecordFormat {
	/// Do
	Yes,
	/// Or do not
	No,
}

/// Print record formats by default
impl std::default::Default for PrintRecordFormat {
	fn default() -> Self {
		PrintRecordFormat::Yes
	}
}

/// Specify how to print the timestamp
#[derive(Debug, Copy, Clone)]
pub enum PrintTimestamp<'a> {
	/// Print the timestamp as nanoseconds since the unix epoch
	Nanos,
	/// Print the timestamp as seconds since the unix epoch
	Seconds,
	/// Print the timestamp according to this `strftime` format.
	/// Refer to [`chrono`](https://docs.rs/chrono/*/chrono/format/strftime/)
	FormatString(&'a str),
}

/// Format as `%FT%T` (ISO-8601)
impl std::default::Default for PrintTimestamp<'static> {
	fn default() -> Self {
		PrintTimestamp::FormatString("%FT%T")
	}
}

/// Write a formatted record to a stream
///
/// Each row is written in the same format that [`add_from_stream`]
/// or [`add_from_stream_with_fmt`] accept, depending
/// on the options for the parameters `print_timestamp`
/// or `print_record_format`.
pub fn print_record<W: std::io::Write>(
	record: &crate::Record,
	out: &mut W,
	print_timestamp: PrintTimestamp<'_>,
	print_record_format: PrintRecordFormat,
) -> std::io::Result<()> {
	let fmt_string = record.format();
	let fmt = parse_row_format(fmt_string);
	let key = record.key();
	let ts = &record.raw()[0..8];
	let value = &record.raw()[8..];
	let ts: u64 = byteorder::BigEndian::read_u64(ts);

	write!(out, "{}\t", escape_string::escape(key))?;

	match print_timestamp {
		PrintTimestamp::Nanos => write!(out, "{}", ts)?,
		PrintTimestamp::Seconds => write!(out, "{}", ts / 1_000_000_000)?,
		PrintTimestamp::FormatString(strf) => {
			let ts = chrono::NaiveDateTime::from_timestamp(
				(ts / 1_000_000_000) as i64,
				(ts % 1_000_000_000) as u32,
			);
			write!(out, "{}", ts.format(strf))?;
		}
	}

	write!(out, "\t")?;
	match print_record_format {
		PrintRecordFormat::Yes => write!(out, "{}\t", fmt_string)?,
		PrintRecordFormat::No => {}
	}

	fmt.to_protocol_format(value, out)
}
