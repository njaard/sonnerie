
use escape_string::split_one;
use crate::row_format::*;
use byteorder::ByteOrder;

pub fn add_from_stream<R: std::io::BufRead>(
	tx: &mut crate::CreateTx,
	db: &crate::DatabaseReader,
	format: &str, input: &mut R,
	timestamp_format: Option<&str>,
	nocheck: bool,
) -> Result<(), crate::WriteFailure>
{
	let row_format = parse_row_format(format);

	let mut line = String::new();
	let mut row_data = vec!();
	let mut key_format_identified = String::new();

	while 0 != input.read_line(&mut line).unwrap()
	{
		let tail = line.trim_end();
		if tail.is_empty() { continue; }
		let (key, tail) = split_one(&tail).unwrap();
		let (timestamp, tail) = split_one(&tail).unwrap();
		let ts: Timestamp;
		if let Some(f) = timestamp_format.as_ref()
		{
			let n = chrono::NaiveDateTime::parse_from_str(&timestamp, f)
				.expect("parsing timestamp according to format");
			ts = n.timestamp_nanos() as Timestamp;
		}
		else
		{
		 ts = timestamp.parse().expect("parsing timestamp");
		}

		let (values, _tail) = split_one(&tail).unwrap();
		row_format.to_stored_format(ts, &values, &mut row_data)
			.unwrap();

		if !nocheck && key_format_identified != key
		{
			if let Some(record) = db.get(&key).next()
			{
				if record.format() != format
				{
					return Err(crate::WriteFailure::HeterogeneousFormats(
						key.to_string(),
						record.format().to_owned(),
						format.to_owned()
					));
				}
			}
			key_format_identified = key.to_string();
		}

		tx.add_record(&key, format, &row_data)?;
		row_data.clear();
		line.clear();
	}

	Ok(())
}

/// like `add_from_stream` except the format string
/// comes after the timestamp
pub fn add_from_stream_with_fmt<R: std::io::BufRead>(
	tx: &mut crate::CreateTx,
	db: &crate::DatabaseReader,
	input: &mut R,
	timestamp_format: Option<&str>,
	nocheck: bool,
) -> Result<(), crate::WriteFailure>
{

	let mut line = String::new();
	let mut row_data = vec!();
	let mut key_format_identified = String::new();

	while 0 != input.read_line(&mut line).unwrap()
	{
		let tail = line.trim_end();
		if tail.is_empty() { continue; }
		let (key, tail) = split_one(&tail).unwrap();
		let (timestamp, tail) = split_one(&tail).unwrap();
		let ts: Timestamp;
		if let Some(f) = timestamp_format.as_ref()
		{
			let n = chrono::NaiveDateTime::parse_from_str(&timestamp, f)
				.expect("parsing timestamp according to format");
			ts = n.timestamp_nanos() as Timestamp;
		}
		else
		{
		 ts = timestamp.parse().expect("parsing timestamp");
		}

		let (format, values) = split_one(&tail).unwrap();
		let row_format = parse_row_format(&format);

		row_format.to_stored_format(ts, &values, &mut row_data)
			.unwrap();

		if !nocheck && key_format_identified != key
		{
			if let Some(record) = db.get(&key).next()
			{
				if record.format() != format
				{
					return Err(crate::WriteFailure::HeterogeneousFormats(
						key.to_string(),
						record.format().to_owned(),
						format.to_string()
					));
				}
			}
			key_format_identified = key.to_string();
		}

		tx.add_record(&key, &format, &row_data)?;
		row_data.clear();
		line.clear();
	}

	Ok(())
}

pub fn print_record<W: std::io::Write>(
	record: &crate::record::OwnedRecord,
	out: &mut W,
) -> std::io::Result<()>
{
	let fmt = parse_row_format(record.format());
	let key = record.key();
	let ts = &record.value()[0..8];
	let value = &record.value()[8..];
	let ts: u64 = byteorder::BigEndian::read_u64(ts);
	let ts = chrono::NaiveDateTime::from_timestamp(
		(ts/1_000_000_000) as i64, (ts%1_000_000_000) as u32
	);

	write!(out, "{}\t{}\t", escape_string::escape(key), ts)?;

	fmt.to_protocol_format(value, out)
}

pub fn print_record_with_fmt<W: std::io::Write>(
	record: &crate::record::OwnedRecord,
	timestamp_format: &str,
	out: &mut W,
) -> std::io::Result<()>
{
	let fmt_string = record.format();
	let fmt = parse_row_format(fmt_string);
	let key = record.key();
	let ts = &record.value()[0..8];
	let value = &record.value()[8..];
	let ts: u64 = byteorder::BigEndian::read_u64(ts);
	let ts = chrono::NaiveDateTime::from_timestamp(
		(ts/1_000_000_000) as i64, (ts%1_000_000_000) as u32
	);

	write!(
		out, "{}\t{}\t{}\t",
		escape_string::escape(key),
		ts.format(timestamp_format),
		fmt_string,
	)?;

	fmt.to_protocol_format(value, out)
}

pub fn print_record_nanos<W: std::io::Write>(
	record: &crate::record::OwnedRecord,
	out: &mut W,
) -> std::io::Result<()>
{
	let fmt = parse_row_format(record.format());
	let key = record.key();
	let ts = &record.value()[0..8];
	let value = &record.value()[8..];
	let ts: u64 = byteorder::BigEndian::read_u64(ts);

	write!(out, "{}\t{}\t", escape_string::escape(key), ts)?;

	fmt.to_protocol_format(value, out)
}
