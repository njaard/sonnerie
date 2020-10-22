//! Read a key from a transaction file.

use crate::segment_reader::*;
use crate::record::*;
use crate::segment::*;
use byteorder::{ByteOrder,BigEndian};
use std::io::Read;
use std::ops::Bound;
use std::ops::Bound::*;
use std::rc::Rc;
use crate::Wildcard;

/// Read and filter keys from a single transaction file
pub struct Reader
{
	segments: SegmentReader,
}

impl Reader
{
	/// Open a single transaction file
	///
	/// If instead you want to read from an entire database,
	/// use [`DatabaseReader`](struct.DatabaseReader.html)
	/// which provides a similar API.
	pub fn new(mut r: std::fs::File) -> std::io::Result<Reader>
	{
		Ok(
			Reader
			{
				segments: SegmentReader::open(&mut r)?,
			}
		)
	}

	/// Get a reader for only a single key
	///
	/// Returns an object that will read all of the
	/// records for only one key.
	pub fn get<'rdr, 'k>(&'rdr self, key: &'k str)
		-> StringKeyRangeReader<'rdr, 'k, std::ops::RangeInclusive<&'k str>>
	{
		self.get_range( key ..= key )
	}

	/// Get a reader for a lexicographic range of keys
	///
	/// Use inclusive or exclusive range syntax to select a range.
	///
	/// Example: `rdr.get_range("chimpan-ay" ..= "chimpan-zee")`
	///
	/// Range queries are always efficient and readahead
	/// may occur.
	pub fn get_range<'rdr, 'k, RB>(&'rdr self, range: RB)
		-> StringKeyRangeReader<'rdr, 'k, RB>
	where
		RB: std::ops::RangeBounds<&'k str>
	{
		let mut data = vec!();
		let segment;

		match range.start_bound()
		{
			Included(v) | Excluded(v) =>
				segment = self.segments.find(v.as_bytes()),
			Unbounded =>
				segment = self.segments.first(),
		}


		if let Some(d) = segment.as_ref()
		{
			{
				// don't do posix_fadvise if we're looking up a single key
				let do_advise;
				match (range.start_bound(), range.end_bound())
				{
					(Included(v1), Included(v2)) =>
						do_advise = v1 != v2,
					_ => do_advise = true,
				}
				if do_advise
				{
					self.segments.advise(d);
				}
			}

			decode_into_with_unescaping(&mut data, d.payload);
		}

		StringKeyRangeReader
		{
			reader: self,
			range,
			decoded: Rc::new(data),
			pos: 0,
			segment: segment,
			current_key_text_len: 0,
			current_key_text_pos: 0,
			current_fmt_text_len: 0,
			current_fmt_text_pos: 0,
			current_key_data_end: 0,
			current_record_len: 0,
			current_key_record_len: None,
			_phantom: std::marker::PhantomData,
			prefix: "",
			matcher: None,
		}
	}

	/// Get a reader that filters on SQL's "LIKE"-like syntax.
	///
	/// A wildcard filter that has a fixed prefix, such as
	/// `"chimp%"` is always efficient.
	pub fn get_filter<'rdr, 'k>(&'rdr self, wildcard: &'k Wildcard)
		-> StringKeyRangeReader<'rdr, 'k, std::ops::RangeFrom<&'k str>>
	{
		let mut filter = self.get_range(wildcard.prefix() ..);
		filter.prefix = wildcard.prefix();
		filter.matcher = wildcard.as_regex();
		filter
	}

	/// Print diagnostic information about this transaction file.
	///
	/// This function is for debugging only.
	pub fn print_info<W: std::io::Write>(&self, w: &mut W)
		-> std::io::Result<()>
	{
		self.segments.print_info(w)
	}

}


pub struct StringKeyRangeReader<'rdr, 'k, RB>
where
	RB: std::ops::RangeBounds<&'k str>
{
	reader: &'rdr Reader,
	range: RB,
	decoded: Rc<Vec<u8>>,
	pos: usize,
	current_key_text_pos: usize,
	current_key_text_len: usize,
	current_fmt_text_pos: usize,
	current_fmt_text_len: usize,
	/// the size of the current record for this key (from the format string)
	current_key_record_len: Option<usize>,
	/// the size of the current record for this key (decoded or from the format string)
	current_record_len: usize,
	current_key_data_end: usize, // where the next key begins
	segment: Option<Segment<'rdr>>,
	matcher: Option<regex::Regex>,
	prefix: &'k str,
	_phantom: std::marker::PhantomData<&'k str>,
}



impl<'rdr, 'k, RB> StringKeyRangeReader<'rdr, 'k, RB>
where
	RB: std::ops::RangeBounds<&'k str>
{
	fn next_segment(&mut self)
	{
		self.pos=0;

		let s = self.reader.segments.segment_after(&self.segment.take().unwrap());
		self.segment = s;

		if let Some(s) = self.segment.as_ref()
		{
			let reuse_vec = std::mem::replace(&mut self.decoded, Rc::new(vec!()));
			let mut old_vec;
			if let Ok(maybe_old_vec) = Rc::try_unwrap(reuse_vec)
				{ old_vec = maybe_old_vec; }
			else
				{ old_vec = vec!(); }
			old_vec.clear();
			decode_into_with_unescaping(&mut old_vec, s.payload);
			self.decoded = Rc::new(old_vec);
		}
	}

	fn next_key(&mut self) -> bool
	{
		while let Some(segment) = self.segment.as_ref()
		{
			while self.pos != self.decoded.len()
			{
				let data = &self.decoded;
				let klen = BigEndian::read_u32(&data[self.pos .. self.pos+4]) as usize;
				let flen = BigEndian::read_u32(&data[self.pos+4 .. self.pos+8]) as usize;

				let pos;
				if segment.segment_version == 0x0000 { pos = self.pos+12; }
				else { pos = self.pos+8; }

				let dlen = BigEndian::read_u32(&data[pos .. pos+4]) as usize;

				let pos = pos+4;

				let key = &data[pos .. pos+klen];
				let key = std::str::from_utf8(&key)
					.expect("input data is not utf8");
				let fmt = &data[pos+klen .. pos+klen+flen];
				let fmt = std::str::from_utf8(&fmt)
					.expect("input data is not utf8");

				self.current_key_text_pos = pos;
				self.current_key_text_len = klen;
				self.current_fmt_text_pos = pos+klen;
				self.current_fmt_text_len = flen;
				let pos = pos + klen + flen;

				if let Some(len) = crate::row_format::row_format_size(fmt)
				{
					self.current_record_len = len;
					self.pos = pos;
					self.current_key_data_end = pos + dlen;
					self.current_key_record_len = Some(len);
				}
				else
				{
					let data = &data[pos .. pos+dlen];
					let (len, tail) = unsigned_varint::decode::u64(data).unwrap();
					let varint_len = data.len() - tail.len();

					self.current_record_len = len as usize;
					self.pos = pos + varint_len;
					self.current_key_data_end = pos+dlen;
					self.current_key_record_len = None;
				}


				match self.range.start_bound()
				{
					Bound::Included(&v) =>
					{
						if key < v
						{
							self.pos = self.current_key_data_end;
							continue;
						}
					},
					Bound::Excluded(&v) =>
					{
						if key <= v
						{
							self.pos = self.current_key_data_end;
							continue;
						}
					},
					Unbounded => {},
				}

				match self.range.end_bound()
				{
					Bound::Included(&v) =>
					{
						if key > v
						{
							self.pos = data.len();
							self.segment = None;
							return false;
						}
					},
					Bound::Excluded(&v) =>
					{
						if key >= v
						{
							self.pos = data.len();
							self.segment = None;
							return false;
						}
					},
					Unbounded =>
					{
						if !key.starts_with(self.prefix)
						{
							self.pos = data.len();
							self.segment = None;
							return false;
						}
					},
				}

				if let Some(regex) = self.matcher.as_ref()
				{
					if !regex.is_match(key)
					{
						self.pos = self.current_key_data_end;
						continue;
					}
				}


				return true;
			}

			self.next_segment();
		}
		false
	}

}


impl<'rdr, 'k, RB> Iterator for StringKeyRangeReader<'rdr, 'k, RB>
where
	RB: std::ops::RangeBounds<&'k str>
{
	type Item = OwnedRecord;
	fn next(&mut self) -> Option<Self::Item>
	{
		while self.segment.is_some()
		{
			if self.pos == self.current_key_data_end
			{
				if !self.next_key() { return None; }
			}

			let r =
				OwnedRecord
				{
					key_pos: self.current_key_text_pos,
					key_len: self.current_key_text_len,
					fmt_pos: self.current_fmt_text_pos,
					fmt_len: self.current_fmt_text_len,
					value_pos: self.pos,
					value_len: self.current_record_len+crate::record::TIMESTAMP_SIZE,
					data: self.decoded.clone(),
				};
			self.pos += self.current_record_len+crate::record::TIMESTAMP_SIZE;
			return Some(r);
		}
		None
	}
}


fn decode_into_with_unescaping(into: &mut Vec<u8>, from: &[u8])
{
	let mut segmented: smallvec::SmallVec<[_; 4]> = smallvec::smallvec![];
	{
		let mut start = 0;
		while let Some(pos) = twoway::find_bytes(&from[start ..], crate::segment::ESCAPE_SEGMENT_INVOCATION)
		{
			segmented.push(&from[start .. pos+start]);
			segmented.push(crate::segment::SEGMENT_INVOCATION);
			start = start + pos + crate::segment::ESCAPE_SEGMENT_INVOCATION.len();
		}
		segmented.push(&from[start ..]);
	}

	let mut reader: Option<Box<dyn Read>> = None;

	for segment in segmented
	{
		if let Some(head) = reader
		{
			reader = Some(Box::new( head.chain( std::io::Cursor::new(segment) ) ) as Box<_>);
		}
		else
		{
			reader = Some(Box::new( std::io::Cursor::new(segment) ) as Box<_>);
		}
	}

	let mut decoder = lz4::Decoder::new( reader.expect("empty segment") )
		.expect("lz4 decoding");
	decoder.read_to_end(into)
		.expect("lz4 decoding 2");
}
