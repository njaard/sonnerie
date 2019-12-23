use crate::segment_reader::*;
use crate::record::*;
use crate::segment::*;
use byteorder::{ByteOrder,BigEndian};
use std::io::Read;
use std::ops::Bound;
use std::ops::Bound::*;
use std::rc::Rc;
use crate::Wildcard;

pub struct Reader
{
	segments: SegmentReader,
}

impl Reader
{
	pub fn new(mut r: std::fs::File) -> std::io::Result<Reader>
	{
		Ok(
			Reader
			{
				segments: SegmentReader::open(&mut r)?,
			}
		)
	}

	pub fn get<'rdr, 'k>(&'rdr self, key: &'k str)
		-> StringKeyRangeReader<'rdr, 'k, std::ops::RangeInclusive<&'k str>>
	{
		self.get_range( key ..= key )
	}

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

			let mut decoder = lz4::Decoder::new( std::io::Cursor::new(d.payload ) )
				.expect("lz4 decoding");
			decoder.read_to_end(&mut data)
				.expect("lz4 decoding 2");
		}

		StringKeyRangeReader
		{
			reader: self,
			range,
			decoded: Rc::new(data),
			pos: 0,
			segment: segment,
			current_key_len: 0,
			current_key_pos: 0,
			current_fmt_len: 0,
			current_fmt_pos: 0,
			current_key_data_len: 0,
			current_key_record_len: 0,
			_phantom: std::marker::PhantomData,
			prefix: "",
			matcher: None,
		}
	}

	pub fn get_filter<'rdr, 'k>(&'rdr self, wildcard: &'k Wildcard)
		-> StringKeyRangeReader<'rdr, 'k, std::ops::RangeFrom<&'k str>>
	{
		let mut filter = self.get_range(wildcard.prefix() ..);
		filter.prefix = wildcard.prefix();
		filter.matcher = wildcard.as_regex();
		filter
	}

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
	current_key_pos: usize,
	current_key_len: usize,
	current_fmt_pos: usize,
	current_fmt_len: usize,
	current_key_record_len: usize, // the size of each record for this key
	current_key_data_len: usize, // the total size of all the records for this key
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
			let mut decoder = lz4::Decoder::new( std::io::Cursor::new( s.payload ) )
				.expect("lz4 decoding");
			decoder.read_to_end(&mut old_vec)
				.expect("lz4 decoding 2");
			self.decoded = Rc::new(old_vec);
		}
	}

	fn next_key(&mut self) -> bool
	{
		while self.segment.is_some()
		{
			while self.pos != self.decoded.len()
			{
				let data = &self.decoded;
				let klen = BigEndian::read_u32(&data[self.pos .. self.pos+4]) as usize;
				let flen = BigEndian::read_u32(&data[self.pos+4 .. self.pos+8]) as usize;
				let rlen = BigEndian::read_u32(&data[self.pos+8 .. self.pos+12]) as usize;
				let dlen = BigEndian::read_u32(&data[self.pos+12 .. self.pos+16]) as usize;

				let key = &data[self.pos+16 .. self.pos+16+klen];
				let key = std::str::from_utf8(&key)
					.expect("input data is not utf8");
				let fmt = &data[self.pos+16+klen .. self.pos+16+klen+flen];
				let _fmt = std::str::from_utf8(&fmt)
					.expect("input data is not utf8");

				self.current_key_pos = self.pos+16;
				self.current_key_len = klen;
				self.current_fmt_pos = self.pos+16+klen;
				self.current_fmt_len = flen;
				self.current_key_record_len = rlen;
				self.current_key_data_len = dlen;
				self.pos = self.pos+16 + klen + flen;

				match self.range.start_bound()
				{
					Bound::Included(&v) =>
					{
						if key < v
						{
							self.pos += self.current_key_data_len;
							continue;
						}
					},
					Bound::Excluded(&v) =>
					{
						if key <= v
						{
							self.pos += self.current_key_data_len;
							continue;
						}
					},
					Unbounded => return true,
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
						self.pos += self.current_key_data_len;
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
			if self.pos == self.current_key_pos
				+ self.current_key_data_len
				+ self.current_key_len
				+ self.current_fmt_len
			{
				if !self.next_key() { return None; }
			}

			let r =
				OwnedRecord
				{
					key_pos: self.current_key_pos,
					key_len: self.current_key_len,
					fmt_pos: self.current_fmt_pos,
					fmt_len: self.current_fmt_len,
					value_pos: self.pos,
					value_len: self.current_key_record_len,
					data: self.decoded.clone(),
				};
			self.pos += self.current_key_record_len;
			return Some(r);
		}
		None
	}
}
