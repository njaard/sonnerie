//! Decode an encoded row format.

use byteorder::{BigEndian, ByteOrder};
use escape_string::split_one;

use crate::Timestamp;

/// Decodes a row by its format. Created with [`parse_row_format`](fn.parse_row_format.html).
pub trait RowFormat {
	/// Encode the data into `dest` into the binary format that is stored.
	fn to_stored_format(&self, ts: Timestamp, from: &str, dest: &mut Vec<u8>)
		-> Result<(), String>;
	/// Decode the data into something human readable
	fn to_protocol_format(
		&self,
		from: &[u8],
		dest: &mut dyn ::std::io::Write,
	) -> ::std::io::Result<()>;
	/// The size in bytes of a row payload, including its timestamp.
	///
	/// None indicates that it has a variable-sized encoding (Strings)
	fn row_size(&self) -> Option<usize>;
}

struct RowFormatImpl {
	size: Option<usize>,
	elements: Vec<Box<dyn Element>>,
}

impl RowFormat for RowFormatImpl {
	fn to_stored_format(
		&self,
		ts: Timestamp,
		mut from: &str,
		dest: &mut Vec<u8>,
	) -> Result<(), String> {
		let at = dest.len();
		dest.reserve(at + self.row_size().unwrap_or(0) + 8);
		dest.resize(at + 8, 0);
		BigEndian::write_u64(&mut dest[at..], ts);
		for e in self.elements.iter() {
			from = e.to_stored_format(from, dest)?;
		}
		if !from.is_empty() {
			return Err("too many columns in input".to_string());
		}
		Ok(())
	}
	fn to_protocol_format(
		&self,
		mut from: &[u8],
		dest: &mut dyn ::std::io::Write,
	) -> ::std::io::Result<()> {
		let mut first = true;

		for e in self.elements.iter() {
			if !first {
				write!(dest, " ")?;
			}
			first = false;
			from = e.to_protocol_format(from, dest)?;
		}
		Ok(())
	}
	fn row_size(&self) -> Option<usize> {
		Some(self.size? + 8)
	}
}

/// convert a text-based row format description to an object
///
/// the text format is one character per term, these are the characters:
///
/// * `i` -> 32-bit signed integer
/// * `I` -> 64-bit signed integer
/// * `u` -> 32-bit unsigned integer
/// * `U` -> 64-bit unsigned integer
/// * `f` -> 32-bit unsigned float
/// * `F` -> 64-bit unsigned float
/// * `s` -> variable size string type
///
/// Potential future types:
/// * decimal
/// * large integers, floats (128 bit, 256 bit)
/// to indicate "typical size"). The typical size is useful
/// for knowing how big to make the blocks
pub fn parse_row_format(human: &str) -> Box<dyn RowFormat> {
	let human = human.as_bytes();

	let mut size = 0usize;
	let mut has_size = true;
	let mut elements: Vec<Box<dyn Element>> = vec![];
	elements.reserve(human.len());

	for t in human {
		match t {
			b'i' => {
				size += 4;
				elements.push(Box::new(ElementI32));
			}
			b'u' => {
				size += 4;
				elements.push(Box::new(ElementU32));
			}
			b'I' => {
				size += 8;
				elements.push(Box::new(ElementI64));
			}
			b'U' => {
				size += 8;
				elements.push(Box::new(ElementU64));
			}
			b'f' => {
				size += 4;
				elements.push(Box::new(ElementF32));
			}
			b'F' => {
				size += 8;
				elements.push(Box::new(ElementF64));
			}
			b's' => {
				has_size = false;
				elements.push(Box::new(ElementString));
			}
			a => {
				panic!("invalid format character '{}'", a);
			}
		}
	}

	Box::new(RowFormatImpl {
		size: if has_size { Some(size) } else { None },
		elements,
	})
}

pub fn row_format_size(human: &str) -> Option<usize> {
	let human = human.as_bytes();

	let mut size = 0usize;

	for t in human {
		match t {
			b'i' => size += 4,
			b'u' => size += 4,
			b'I' => size += 8,
			b'U' => size += 8,
			b'f' => size += 4,
			b'F' => size += 8,
			b's' => return None,
			a => {
				panic!("invalid format character '{}'", a);
			}
		}
	}

	Some(size)
}

trait Element {
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> Result<&'s str, String>;
	fn to_protocol_format<'a>(
		&self,
		from: &'a [u8],
		dest: &mut dyn ::std::io::Write,
	) -> ::std::io::Result<&'a [u8]>;
}

struct ElementI32;
impl Element for ElementI32 {
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> Result<&'s str, String> {
		let at = dest.len();
		dest.resize(at + 4, 0);
		let mut dest = &mut dest[at..];

		let (t, rest) = split_one(from).unwrap();

		let v = t
			.parse()
			.map_err(|e| format!("while parsing {}: {}", t, e))?;
		BigEndian::write_i32(&mut dest, v);

		Ok(rest)
	}
	fn to_protocol_format<'a>(
		&self,
		from: &'a [u8],
		dest: &mut dyn ::std::io::Write,
	) -> ::std::io::Result<&'a [u8]> {
		let v: i32 = BigEndian::read_i32(&from[0..4]);
		write!(dest, "{}", v)?;
		Ok(&from[4..])
	}
}

struct ElementU32;
impl Element for ElementU32 {
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> Result<&'s str, String> {
		let at = dest.len();
		dest.resize(at + 4, 0);
		let mut dest = &mut dest[at..];

		let (t, rest) = split_one(from).unwrap();

		let v = t
			.parse()
			.map_err(|e| format!("while parsing {}: {}", t, e))?;
		BigEndian::write_u32(&mut dest, v);

		Ok(rest)
	}
	fn to_protocol_format<'a>(
		&self,
		from: &'a [u8],
		dest: &mut dyn ::std::io::Write,
	) -> ::std::io::Result<&'a [u8]> {
		let v: u32 = BigEndian::read_u32(&from[0..4]);
		write!(dest, "{}", v)?;
		Ok(&from[4..])
	}
}

struct ElementI64;
impl Element for ElementI64 {
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> Result<&'s str, String> {
		let at = dest.len();
		dest.resize(at + 8, 0);
		let mut dest = &mut dest[at..];

		let (t, rest) = split_one(from).unwrap();

		let v = t
			.parse()
			.map_err(|e| format!("while parsing {}: {}", t, e))?;
		BigEndian::write_i64(&mut dest, v);

		Ok(rest)
	}
	fn to_protocol_format<'a>(
		&self,
		from: &'a [u8],
		dest: &mut dyn ::std::io::Write,
	) -> ::std::io::Result<&'a [u8]> {
		let v: i64 = BigEndian::read_i64(&from[0..8]);
		write!(dest, "{}", v)?;
		Ok(&from[8..])
	}
}

struct ElementU64;
impl Element for ElementU64 {
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> Result<&'s str, String> {
		let at = dest.len();
		dest.resize(at + 8, 0);
		let mut dest = &mut dest[at..];

		let (t, rest) = split_one(from).unwrap();

		let v = t
			.parse()
			.map_err(|e| format!("while parsing {}: {}", t, e))?;
		BigEndian::write_u64(&mut dest, v);

		Ok(rest)
	}
	fn to_protocol_format<'a>(
		&self,
		from: &'a [u8],
		dest: &mut dyn ::std::io::Write,
	) -> ::std::io::Result<&'a [u8]> {
		let v: u64 = BigEndian::read_u64(&from[0..8]);
		write!(dest, "{}", v)?;
		Ok(&from[8..])
	}
}

struct ElementF32;
impl Element for ElementF32 {
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> Result<&'s str, String> {
		let at = dest.len();
		dest.resize(at + 4, 0);
		let mut dest = &mut dest[at..];

		let (t, rest) = split_one(from).unwrap();

		let v;

		if t == "nan" {
			v = ::std::f32::NAN;
		} else {
			v = t
				.parse()
				.map_err(|e| format!("while parsing {}: {}", t, e))?;
		}
		BigEndian::write_f32(&mut dest, v);

		Ok(rest)
	}
	fn to_protocol_format<'a>(
		&self,
		from: &'a [u8],
		dest: &mut dyn ::std::io::Write,
	) -> ::std::io::Result<&'a [u8]> {
		let v: f32 = BigEndian::read_f32(&from[0..4]);
		write!(dest, "{:.17}", v)?;
		Ok(&from[4..])
	}
}

struct ElementF64;
impl Element for ElementF64 {
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> Result<&'s str, String> {
		let at = dest.len();
		dest.resize(at + 8, 0);
		let mut dest = &mut dest[at..];

		let (t, rest) = split_one(from).unwrap();

		let v;

		if t == "nan" {
			v = ::std::f64::NAN;
		} else {
			v = t
				.parse()
				.map_err(|e| format!("while parsing {}: {}", t, e))?;
		}
		BigEndian::write_f64(&mut dest, v);

		Ok(rest)
	}
	fn to_protocol_format<'a>(
		&self,
		from: &'a [u8],
		dest: &mut dyn ::std::io::Write,
	) -> ::std::io::Result<&'a [u8]> {
		let v: f64 = BigEndian::read_f64(&from[0..8]);
		write!(dest, "{:.17}", v)?;
		Ok(&from[8..])
	}
}

struct ElementString;
impl Element for ElementString {
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> Result<&'s str, String> {
		let (head, tail) = escape_string::split_one(from)
			.ok_or_else(|| format!("Unable to parse \"{}\" as backslash-escaped string", from))?;
		let mut buf = unsigned_varint::encode::u64_buffer();
		let encoded_len = unsigned_varint::encode::u64(head.len() as u64, &mut buf);
		dest.extend_from_slice(encoded_len);
		dest.extend_from_slice(head.as_bytes());
		Ok(tail)
	}
	fn to_protocol_format<'a>(
		&self,
		from: &'a [u8],
		dest: &mut dyn ::std::io::Write,
	) -> ::std::io::Result<&'a [u8]> {
		let (len, tail) = unsigned_varint::decode::u64(from).map_err(|e| {
			std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{:?}", e))
		})?;

		let s = std::str::from_utf8(&tail[0..len as usize])
			.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
		write!(dest, "{}", escape_string::escape(s))?;
		Ok(&tail[len as usize..])
	}
}
