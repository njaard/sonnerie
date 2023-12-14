//! Stores a single row.

use std::sync::Arc;

pub(crate) const TIMESTAMP_SIZE: usize = 8;

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};

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
		write!(f, "Record {{ key={}, t={}", self.key(), self.time())?;

		for (idx, c) in self.format().chars().enumerate() {
			match c {
				'f' => write!(f, ", {}", self.get::<f32>(idx))?,
				'F' => write!(f, ", {}", self.get::<f64>(idx))?,
				'i' => write!(f, ", {}", self.get::<i32>(idx))?,
				'I' => write!(f, ", {}", self.get::<i64>(idx))?,
				'u' => write!(f, ", {}", self.get::<u32>(idx))?,
				'U' => write!(f, ", {}", self.get::<u64>(idx))?,
				's' => write!(f, ", \"{}\"", self.get::<&str>(idx).escape_default())?,
				a => panic!("unknown format column '{a}'"),
			}
		}
		write!(f, " }}")
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
		chrono::NaiveDateTime::from_timestamp_opt(
			(ts / 1_000_000_000) as i64,
			(ts % 1_000_000_000) as u32,
		)
		.unwrap()
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

/// Implements conversions from Rust types to Sonnerie records
pub trait ToRecord {
	fn store(&self, buf: &mut Vec<u8>);
	fn format_char(&self) -> u8;
	fn size(&self) -> usize;
	fn variable_size(&self) -> bool;
}

impl ToRecord for i32 {
	fn store(&self, buf: &mut Vec<u8>) {
		buf.write_i32::<BigEndian>(*self).unwrap();
	}
	fn format_char(&self) -> u8 {
		b'i'
	}
	fn size(&self) -> usize {
		4
	}
	fn variable_size(&self) -> bool {
		false
	}
}
impl ToRecord for u32 {
	fn store(&self, buf: &mut Vec<u8>) {
		buf.write_u32::<BigEndian>(*self).unwrap();
	}
	fn format_char(&self) -> u8 {
		b'u'
	}
	fn size(&self) -> usize {
		4
	}
	fn variable_size(&self) -> bool {
		false
	}
}

impl ToRecord for i64 {
	fn store(&self, buf: &mut Vec<u8>) {
		buf.write_i64::<BigEndian>(*self).unwrap();
	}
	fn format_char(&self) -> u8 {
		b'I'
	}
	fn size(&self) -> usize {
		8
	}
	fn variable_size(&self) -> bool {
		false
	}
}
impl ToRecord for u64 {
	fn store(&self, buf: &mut Vec<u8>) {
		buf.write_u64::<BigEndian>(*self).unwrap();
	}
	fn format_char(&self) -> u8 {
		b'U'
	}
	fn size(&self) -> usize {
		8
	}
	fn variable_size(&self) -> bool {
		false
	}
}

impl ToRecord for f32 {
	fn store(&self, buf: &mut Vec<u8>) {
		buf.write_f32::<BigEndian>(*self).unwrap();
	}
	fn format_char(&self) -> u8 {
		b'f'
	}
	fn size(&self) -> usize {
		4
	}
	fn variable_size(&self) -> bool {
		false
	}
}
impl ToRecord for f64 {
	fn store(&self, buf: &mut Vec<u8>) {
		buf.write_f64::<BigEndian>(*self).unwrap();
	}
	fn format_char(&self) -> u8 {
		b'F'
	}
	fn size(&self) -> usize {
		8
	}
	fn variable_size(&self) -> bool {
		false
	}
}

impl ToRecord for &str {
	fn store(&self, buf: &mut Vec<u8>) {
		let len = self.len();
		let mut lenbuf = unsigned_varint::encode::usize_buffer();
		let lenbuf = unsigned_varint::encode::usize(len, &mut lenbuf);
		buf.extend_from_slice(lenbuf);
		buf.extend_from_slice(self.as_bytes());
	}
	fn format_char(&self) -> u8 {
		b's'
	}
	fn size(&self) -> usize {
		let mut buf = unsigned_varint::encode::usize_buffer();
		let buf = unsigned_varint::encode::usize(self.len(), &mut buf);
		buf.len() + self.len()
	}
	fn variable_size(&self) -> bool {
		true
	}
}

impl ToRecord for String {
	fn store(&self, buf: &mut Vec<u8>) {
		self.as_str().store(buf)
	}
	fn format_char(&self) -> u8 {
		self.as_str().format_char()
	}
	fn size(&self) -> usize {
		self.as_str().size()
	}
	fn variable_size(&self) -> bool {
		self.as_str().variable_size()
	}
}

/// Converts multiple-column data to the internal encoding
///
/// Create this type with [`crate::record()`]
pub trait RecordBuilder {
	#[doc(hidden)]
	fn format_str(&self, to: &mut compact_str::CompactString);
	#[doc(hidden)]
	fn variable_size(&self) -> bool;
	#[doc(hidden)]
	fn size(&self) -> usize;
	#[doc(hidden)]
	fn store(&self, buf: &mut Vec<u8>);
}

#[doc(hidden)]
pub struct BuildingRecord<Value, Tail>
where
	Tail: RecordBuilder,
	Value: ToRecord,
{
	value: Value,
	tail: Tail,
}

impl<Value, Tail> BuildingRecord<Value, Tail>
where
	Tail: RecordBuilder,
	Value: ToRecord,
{
	#[allow(clippy::should_implement_trait)]
	pub fn add<Next: ToRecord>(self, value: Next) -> BuildingRecord<Next, Self> {
		BuildingRecord { value, tail: self }
	}
}

impl<Value, Tail> RecordBuilder for BuildingRecord<Value, Tail>
where
	Tail: RecordBuilder,
	Value: ToRecord,
{
	fn format_str(&self, to: &mut compact_str::CompactString) {
		self.tail.format_str(to);
		to.push(self.value.format_char() as char);
	}
	fn variable_size(&self) -> bool {
		if self.value.variable_size() {
			return true;
		}
		self.tail.variable_size()
	}
	fn size(&self) -> usize {
		self.value.size() + self.tail.size()
	}

	fn store(&self, buf: &mut Vec<u8>) {
		self.tail.store(buf);
		self.value.store(buf);
	}
}

/// Placeholder type to mark the end of a record, when writing
///
/// Rust's generics are used to build a chain of types, [`record()`]
/// returns a `BuildingRecord<FirstColumnType,RecordBuilderEnd>`
/// where `FirstColumnType` implements [`RecordBuilder`] trait
/// for the first column value. When you use [`BuildingRecord::add()`]
/// to add an additional column, the type
/// `BuildingRecord<BuildingRecord<SecondColumnType, FirstColumnType>,RecordBuilderEnd>`
/// will be built.
pub struct RecordBuilderEnd;
impl RecordBuilder for RecordBuilderEnd {
	fn format_str(&self, _: &mut compact_str::CompactString) {}
	fn variable_size(&self) -> bool {
		false
	}
	fn size(&self) -> usize {
		0
	}
	fn store(&self, _: &mut Vec<u8>) {}
}

/// A high-level function to build records from Rust types
///
/// `record()` encodes the given value into a column, you can call `add()` on returned
/// object as many times as you want by chaining, to create multicolumn records.
///
/// The Rust type of each column is used to determine the stored format string. For example,
/// a `u32` will be stored with the format string `u`.
///
/// ```no_run
/// # let mut transaction = sonnerie::CreateTx::new(std::path::Path::new("")).unwrap();
/// transaction.add_record(
///    "key name",
///    "2010-01-01T00:00:01".parse().unwrap(),
///    sonnerie::record("Column 1").add("Column 2").add(3i32)
///  ).unwrap();
/// ```
///
/// This function performs most of its work at compile-time.
pub fn record<Rec: ToRecord>(value: Rec) -> BuildingRecord<Rec, RecordBuilderEnd> {
	BuildingRecord {
		value,
		tail: RecordBuilderEnd,
	}
}

impl RecordBuilder for &[&dyn ToRecord] {
	fn format_str(&self, fmt: &mut compact_str::CompactString) {
		self.iter().for_each(|v| fmt.push(v.format_char().into()));
	}
	fn variable_size(&self) -> bool {
		self.iter().map(|m| m.variable_size()).any(|a| a)
	}
	fn size(&self) -> usize {
		self.iter().map(|m| m.size()).sum::<usize>()
	}
	fn store(&self, buf: &mut Vec<u8>) {
		self.iter().for_each(|v| v.store(buf))
	}
}

impl RecordBuilder for [&dyn ToRecord] {
	fn format_str(&self, fmt: &mut compact_str::CompactString) {
		self.iter().for_each(|v| fmt.push(v.format_char().into()));
	}
	fn variable_size(&self) -> bool {
		self.iter().map(|m| m.variable_size()).any(|a| a)
	}
	fn size(&self) -> usize {
		self.iter().map(|m| m.size()).sum::<usize>()
	}
	fn store(&self, buf: &mut Vec<u8>) {
		self.iter().for_each(|v| v.store(buf))
	}
}

impl<const N: usize> RecordBuilder for &[&dyn ToRecord; N] {
	fn format_str(&self, fmt: &mut compact_str::CompactString) {
		self.iter().for_each(|v| fmt.push(v.format_char().into()));
	}
	fn variable_size(&self) -> bool {
		self.iter().map(|m| m.variable_size()).any(|a| a)
	}
	fn size(&self) -> usize {
		self.iter().map(|m| m.size()).sum::<usize>()
	}
	fn store(&self, buf: &mut Vec<u8>) {
		self.iter().for_each(|v| v.store(buf))
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
		Ok(BigEndian::read_i32(bytes))
	}
}

impl<'a> FromRecord<'a> for i64 {
	fn get(fmt_char: u8, bytes: &'a [u8]) -> std::io::Result<Self> {
		if fmt_char == b'i' {
			Ok(BigEndian::read_i32(bytes) as i64)
		} else if fmt_char == b'I' {
			Ok(BigEndian::read_i64(bytes))
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
		Ok(BigEndian::read_u32(bytes))
	}
}

impl<'a> FromRecord<'a> for u64 {
	fn get(fmt_char: u8, bytes: &'a [u8]) -> std::io::Result<Self> {
		if fmt_char == b'u' {
			Ok(BigEndian::read_u32(bytes) as u64)
		} else if fmt_char == b'U' {
			Ok(BigEndian::read_u64(bytes))
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
		Ok(BigEndian::read_f32(bytes))
	}
}

impl<'a> FromRecord<'a> for f64 {
	fn get(fmt_char: u8, bytes: &'a [u8]) -> std::io::Result<Self> {
		if fmt_char == b'f' {
			Ok(BigEndian::read_f32(bytes) as f64)
		} else if fmt_char == b'F' {
			Ok(BigEndian::read_f64(bytes))
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

		std::str::from_utf8(&tail[..len as usize])
			.map_err(|k| std::io::Error::new(std::io::ErrorKind::InvalidData, k))
	}
}
