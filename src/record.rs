//! Stores a single row.

use std::sync::Arc;

pub(crate) const TIMESTAMP_SIZE: usize = 8;

use byteorder::{BigEndian, ByteOrder};

/// Stores a single timestamp for a single key of data
///
/// Internally copy-on-write so cheap to copy
pub struct Record {
	pub(crate) key_pos: usize,
	pub(crate) key_len: usize,
	pub(crate) fmt_pos: usize,
	pub(crate) fmt_len: usize,
	pub(crate) value_pos: usize,
	pub(crate) value_len: usize,
	pub(crate) data: Arc<Vec<u8>>,
}

impl std::fmt::Debug for Record {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		write!(f, "Record {{ key={} }}", self.key())
	}
}

impl Record {
	/// The key of this record.
	pub fn key(&self) -> &str {
		let d = &self.data[self.key_pos..self.key_pos + self.key_len];
		// this string was checked for utf-8 validity by key_reader
		unsafe { std::str::from_utf8_unchecked(d) }
	}

	/// The format of this record (as the single-character codes)
	pub fn format(&self) -> &str {
		let d = &self.data[self.fmt_pos..self.fmt_pos + self.fmt_len];
		// this string was checked for utf-8 validity by key_reader
		unsafe { std::str::from_utf8_unchecked(d) }
	}

	/// A decoded timestamp for this record
	pub fn time(&self) -> chrono::NaiveDateTime {
		let ts = self.timestamp_nanos();
		chrono::NaiveDateTime::from_timestamp(
			(ts / 1_000_000_000) as i64,
			(ts % 1_000_000_000) as u32,
		)
	}

	/// The raw timestamp for this record as nanoseconds
	pub fn timestamp_nanos(&self) -> u64 {
		let ts = &self.raw()[0..TIMESTAMP_SIZE];
		byteorder::BigEndian::read_u64(ts)
	}

	/// Read a single column returning possible errors
	///
	/// Errors can include an invalid data stream or incompatible types.
	///
	/// Any column can be read as long as its type can losslessly be interpreted as the
	/// requested type. For example, if the column stores a 'u' (32-bit unsigned), then
	/// it can be read into a `u32` or a `u64`. However, it's a failure to read the column
	/// as a `u32` if the column stores a `U`, even if the stored value itself can be
	/// represented in a `u32`.
	pub fn get_checked<'a, T: FromRecord<'a>>(&'a self, col: usize) -> std::io::Result<T> {
		let fmt = self.format().as_bytes();
		let mut from = &self.raw()[TIMESTAMP_SIZE..];

		if fmt.len() <= col {
			return Err(std::io::Error::new(
				std::io::ErrorKind::UnexpectedEof,
				"column out of range",
			));
		}

		for code in fmt.iter().take(col) {
			match code {
				b'i' | b'u' | b'f' => from = &from[4..],
				b'I' | b'U' | b'F' => from = &from[8..],
				b's' => {
					let (len, tail) = unsigned_varint::decode::u64(from).map_err(|e| {
						std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{:?}", e))
					})?;
					from = &tail[len as usize..];
				}
				a => {
					return Err(std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						format!("format string contains invalid '{}'", *a as char),
					))
				}
			}
		}

		let fmt_char = fmt[col];

		FromRecord::get(fmt_char, from)
	}

	/// Read a column, turning errors into panics
	///
	/// Same as [`Record::get`]`(n).unwrap()`
	pub fn get<'a, T: FromRecord<'a>>(&'a self, column: usize) -> T {
		self.get_checked(column).expect("unable to read column")
	}

	/// The value for the first column
	///
	/// same as [`Record::get`]`(0)`
	pub fn value<'a, T: FromRecord<'a>>(&'a self) -> T {
		self.get(0)
	}

	/// The encoded payload of this data. The timestamp and as 8
	/// bytes in nanoseconds, and then each column in turn
	pub fn raw(&self) -> &[u8] {
		&self.data[self.value_pos..self.value_pos + self.value_len]
	}
}

/// Implements conversions from [`Record`] columns to Rust types
pub trait FromRecord<'a>: Sized {
	fn get(fmt_char: u8, bytes: &'a [u8]) -> std::io::Result<Self>;
}

impl<'a> FromRecord<'a> for i32 {
	fn get(fmt_char: u8, bytes: &'a [u8]) -> std::io::Result<Self> {
		if fmt_char != b'i' {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("cannot decode i32 from '{}'", fmt_char as char),
			));
		}
		Ok(BigEndian::read_i32(&bytes))
	}
}

impl<'a> FromRecord<'a> for i64 {
	fn get(fmt_char: u8, bytes: &'a [u8]) -> std::io::Result<Self> {
		if fmt_char == b'i' {
			Ok(BigEndian::read_i32(&bytes) as i64)
		} else if fmt_char == b'I' {
			Ok(BigEndian::read_i64(&bytes))
		} else {
			Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("cannot decode i64 from '{}'", fmt_char as char),
			))
		}
	}
}

impl<'a> FromRecord<'a> for u32 {
	fn get(fmt_char: u8, bytes: &[u8]) -> std::io::Result<Self> {
		if fmt_char != b'u' {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("cannot decode u32 from '{}'", fmt_char as char),
			));
		}
		Ok(BigEndian::read_u32(&bytes))
	}
}

impl<'a> FromRecord<'a> for u64 {
	fn get(fmt_char: u8, bytes: &'a [u8]) -> std::io::Result<Self> {
		if fmt_char == b'u' {
			Ok(BigEndian::read_u32(&bytes) as u64)
		} else if fmt_char == b'U' {
			Ok(BigEndian::read_u64(&bytes))
		} else {
			Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("cannot decode u64 from '{}'", fmt_char as char),
			))
		}
	}
}

impl<'a> FromRecord<'a> for f32 {
	fn get(fmt_char: u8, bytes: &'a [u8]) -> std::io::Result<Self> {
		if fmt_char != b'f' {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("cannot decode f32 from '{}'", fmt_char as char),
			));
		}
		Ok(BigEndian::read_f32(&bytes))
	}
}

impl<'a> FromRecord<'a> for f64 {
	fn get(fmt_char: u8, bytes: &'a [u8]) -> std::io::Result<Self> {
		if fmt_char == b'f' {
			Ok(BigEndian::read_f32(&bytes) as f64)
		} else if fmt_char == b'F' {
			Ok(BigEndian::read_f64(&bytes))
		} else {
			Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("cannot decode f64 from '{}'", fmt_char as char),
			))
		}
	}
}

impl<'a> FromRecord<'a> for String {
	fn get(fmt_char: u8, bytes: &'a [u8]) -> std::io::Result<Self> {
		let s: &str = FromRecord::get(fmt_char, bytes)?;
		Ok(s.to_string())
	}
}

impl<'a> FromRecord<'a> for &'a str {
	fn get(fmt_char: u8, bytes: &'a [u8]) -> std::io::Result<Self> {
		if fmt_char != b's' {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("cannot decode String from '{}'", fmt_char as char),
			));
		}

		let (len, tail) = unsigned_varint::decode::u64(bytes).map_err(|e| {
			std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{:?}", e))
		})?;

		Ok(std::str::from_utf8(&tail[..len as usize])
			.map_err(|k| std::io::Error::new(std::io::ErrorKind::InvalidData, k))?)
	}
}
