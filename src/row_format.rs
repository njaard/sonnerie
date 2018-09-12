extern crate byteorder;
extern crate escape_string;

use self::byteorder::{ByteOrder, BigEndian};
use self::escape_string::{split_one};

pub use metadata::Timestamp;

pub trait RowFormat
{
	fn to_stored_format(&self, ts: &Timestamp, from: &str, dest: &mut Vec<u8>);
	fn to_protocol_format(&self, from: &[u8], dest: &mut ::std::io::Write)
		-> ::std::io::Result<()>;

	fn preferred_block_size(&self) -> usize
	{
		4096/(self.row_size())
	}
	fn row_size(&self) -> usize;
}


struct RowFormatImpl
{
	size: usize,
	elements: Vec<Box<Element>>,
}

impl RowFormat for RowFormatImpl
{
	fn to_stored_format(&self, ts: &Timestamp, mut from: &str, dest: &mut Vec<u8>)
	{
		let at = dest.len();
		dest.reserve(at+self.row_size());
		dest.resize(at+8, 0);
		BigEndian::write_u64(&mut dest[at..], ts.0);
		for e in self.elements.iter()
		{
			from = e.to_stored_format(from, dest);
		}
	}
	fn to_protocol_format(&self, from: &[u8], dest: &mut ::std::io::Write)
		-> ::std::io::Result<()>
	{
		let mut first = true;
		for e in self.elements.iter()
		{
			if !first
			{
				write!(dest, " ")?;
			}
			first = false;
			e.to_protocol_format(from, dest)?;
		}
		Ok(())
	}
	fn row_size(&self) -> usize
	{
		self.size+8
	}

}


/// convert a text-based row format description to an object
///
/// the text format is one character per term, these are the characters:
///
/// * i -> 32-bit signed integer
/// * I -> 64-bit signed integer
/// * u -> 32-bit unsigned integer
/// * U -> 64-bit unsigned integer
/// * f -> 32-bit unsigned float
/// * F -> 64-bit unsigned float
/// * b[number] -> a given number of bytes: "b256" 256 bytes
///
/// Potential future types:
/// * decimal
/// * large integers, floats (128 bit, 256 bit)
/// * "s" -> variable size string type (maybe followed by a number
/// to indicate "typical size"). The typical size is useful
/// for knowing how big to make the blocks
pub fn parse_row_format(human: &str) -> Box<RowFormat>
{
	let human = human.as_bytes();

	let mut size = 0usize;
	let mut elements: Vec<Box<Element>> = vec!();
	elements.reserve(human.len());

	for t in human
	{
		match t
		{
			b'i' =>
			{
				size += 4;
				elements.push( Box::new(ElementI32) );
			},
			b'u' =>
			{
				size += 4;
				elements.push( Box::new(ElementU32) );
			},
			b'I' =>
			{
				size += 8;
				elements.push( Box::new(ElementI64) );
			},
			b'U' =>
			{
				size += 8;
				elements.push( Box::new(ElementU64) );
			},
			b'f' =>
			{
				size += 4;
				elements.push( Box::new(ElementF32) );
			},
			b'F' =>
			{
				size += 8;
				elements.push( Box::new(ElementF64) );
			},
			a =>
			{
				panic!("invalid format character '{}'", a);
			}
		}
	}

	Box::new(
		RowFormatImpl
		{
			size: size,
			elements: elements,
		}
	)
}


trait Element
{
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> &'s str;
	fn to_protocol_format(&self, from: &[u8], dest: &mut ::std::io::Write)
		-> ::std::io::Result<()>;
}

struct ElementI32;
impl Element for ElementI32
{
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> &'s str
	{
		let at = dest.len();
		dest.resize(at + 4, 0);
		let mut dest = &mut dest[at..];

		let (t, rest) = split_one(from).unwrap();

		let v = t.parse()
			.map_err(|e| format!("while parsing {}: {}", t, e))
			.unwrap();
		BigEndian::write_i32(&mut dest, v);

		rest
	}
	fn to_protocol_format(&self, from: &[u8], dest: &mut ::std::io::Write)
		-> ::std::io::Result<()>
	{
		let v: i32 = BigEndian::read_i32(&from[0..4]);
		write!(dest, "{}", v)
	}
}

struct ElementU32;
impl Element for ElementU32
{
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> &'s str
	{
		let at = dest.len();
		dest.resize(at + 4, 0);
		let mut dest = &mut dest[at..];

		let (t, rest) = split_one(from).unwrap();

		let v = t.parse()
			.map_err(|e| format!("while parsing {}: {}", t, e))
			.unwrap();
		BigEndian::write_u32(&mut dest, v);

		rest
	}
	fn to_protocol_format(&self, from: &[u8], dest: &mut ::std::io::Write)
		-> ::std::io::Result<()>
	{
		let v: u32 = BigEndian::read_u32(&from[0..4]);
		write!(dest, "{}", v)
	}
}

struct ElementI64;
impl Element for ElementI64
{
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> &'s str
	{
		let at = dest.len();
		dest.resize(at + 8, 0);
		let mut dest = &mut dest[at..];

		let (t, rest) = split_one(from).unwrap();

		let v = t.parse()
			.map_err(|e| format!("while parsing {}: {}", t, e))
			.unwrap();
		BigEndian::write_i64(&mut dest, v);

		rest
	}
	fn to_protocol_format(&self, from: &[u8], dest: &mut ::std::io::Write)
		-> ::std::io::Result<()>
	{
		let v: i64 = BigEndian::read_i64(&from[0..8]);
		write!(dest, "{}", v)
	}
}

struct ElementU64;
impl Element for ElementU64
{
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> &'s str
	{
		let at = dest.len();
		dest.resize(at + 8, 0);
		let mut dest = &mut dest[at..];

		let (t, rest) = split_one(from).unwrap();

		let v = t.parse()
			.map_err(|e| format!("while parsing {}: {}", t, e))
			.unwrap();
		BigEndian::write_u64(&mut dest, v);

		rest
	}
	fn to_protocol_format(&self, from: &[u8], dest: &mut ::std::io::Write)
		-> ::std::io::Result<()>
	{
		let v: u64 = BigEndian::read_u64(&from[0..8]);
		write!(dest, "{}", v)
	}
}


struct ElementF32;
impl Element for ElementF32
{
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> &'s str
	{
		let at = dest.len();
		dest.resize(at + 4, 0);
		let mut dest = &mut dest[at..];

		let (t, rest) = split_one(from).unwrap();

		let v;

		if t == "nan"
			{ v = ::std::f32::NAN; }
		else
		{
			v = t.parse()
				.map_err(|e| format!("while parsing {}: {}", t, e))
				.unwrap();
		}
		BigEndian::write_f32(&mut dest, v);

		rest
	}
	fn to_protocol_format(&self, from: &[u8], dest: &mut ::std::io::Write)
		-> ::std::io::Result<()>
	{
		let v: f32 = BigEndian::read_f32(&from[0..4]);
		write!(dest, "{:.17}", v)
	}
}

struct ElementF64;
impl Element for ElementF64
{
	fn to_stored_format<'s>(&self, from: &'s str, dest: &mut Vec<u8>) -> &'s str
	{
		let at = dest.len();
		dest.resize(at + 8, 0);
		let mut dest = &mut dest[at..];

		let (t, rest) = split_one(from).unwrap();

		let v;

		if t == "nan"
			{ v = ::std::f64::NAN; }
		else
		{
			v = t.parse()
				.map_err(|e| format!("while parsing {}: {}", t, e))
				.unwrap();
		}
		BigEndian::write_f64(&mut dest, v);

		rest
	}
	fn to_protocol_format(&self, from: &[u8], dest: &mut ::std::io::Write)
		-> ::std::io::Result<()>
	{
		let v: f64 = BigEndian::read_f64(&from[0..8]);
		write!(dest, "{:.17}", v)
	}
}
