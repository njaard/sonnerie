//! Read a key from a transaction file.

use crate::records::*;
use crate::segment::*;
use crate::segment_reader::*;
use crate::Wildcard;
use byteorder::{BigEndian, ByteOrder};
use std::ops::Bound;
use std::ops::Bound::*;
use std::ops::RangeBounds;
//use std::rc::Rc;
use either::Either;
use std::sync::Arc as Rc;

/// Read and filter keys from a single transaction file
pub struct Reader {
	pub(crate) segments: SegmentReader,
}

impl Reader {
	/// Open a single transaction file
	///
	/// If instead you want to read from an entire database,
	/// use [`DatabaseReader`](struct.DatabaseReader.html)
	/// which provides a similar API.
	pub fn new(mut r: std::fs::File) -> std::io::Result<Either<Reader, DeleteMarker>> {
		use Either::*;

		match SegmentReader::open(&mut r)? {
			Left(segments) => Ok(Left(Reader { segments })),
			Right(delete) => Ok(Right(delete)),
		}
	}

	/// Get a reader for only a single key
	///
	/// Returns an object that will read all of the
	/// records for only one key.
	pub fn get<'rdr, 'k>(&'rdr self, key: &'k str) -> StringKeyRangeReader<'rdr, 'k> {
		self.get_range(key..=key)
	}

	/// Get a reader for a lexicographic range of keys
	///
	/// Use inclusive or exclusive range syntax to select a range.
	///
	/// Example: `rdr.get_range("chimpan-ay" ..= "chimpan-zee")`
	///
	/// Range queries are always efficient and readahead
	/// may occur.
	pub fn get_range<'rdr, 'k>(
		&'rdr self,
		range: impl RangeBounds<&'k str> + 'k + Clone,
	) -> StringKeyRangeReader<'rdr, 'k> {
		self.get_filter_range(None, "", crate::disassemble_range_bound(range).into())
	}

	/// Get a reader that filters on SQL's "LIKE"-like syntax.
	///
	/// A wildcard filter that has a fixed prefix, such as
	/// `"chimp%"` is always efficient.
	pub fn get_filter<'rdr, 'k>(
		&'rdr self,
		wildcard: &'k Wildcard,
	) -> StringKeyRangeReader<'rdr, 'k> {
		if wildcard.is_exact() {
			self.get(wildcard.prefix())
		} else {
			let mut filter = self.get_range(wildcard.prefix()..);
			filter.prefix = wildcard.prefix();
			filter.matcher = wildcard.as_regex();
			filter
		}
	}

	pub(crate) fn get_filter_range<'rdr, 'k>(
		&'rdr self,
		matcher: Option<regex::Regex>,
		prefix: &'k str,
		range: crate::CowStringRange<'k>,
	) -> StringKeyRangeReader<'rdr, 'k> {
		let mut data = vec![];

		let segment = match range.start_bound() {
			Included(v) | Excluded(v) => self.segments.find(v),
			Unbounded => self.segments.first(),
		};

		if let Some(d) = segment.as_ref() {
			{
				// don't do posix_fadvise if we're looking up a single key
				let do_advise = match (range.start_bound(), range.end_bound()) {
					(Included(v1), Included(v2)) => v1 != v2,
					_ => true,
				};
				if do_advise {
					self.segments.advise(d);
				}
			}

			crate::segment_reader::decode_into_with_unescaping(&mut data, d.payload);
		}

		StringKeyRangeReader {
			reader: self,
			range,
			decoded: Rc::new(data),
			pos: 0,
			segment,
			current_key_text_len: 0,
			current_key_text_pos: 0,
			current_fmt_text_len: 0,
			current_fmt_text_pos: 0,
			current_key_data_end: 0,
			current_record_len: None,
			current_key_record_len: None,
			_phantom: std::marker::PhantomData,
			prefix,
			matcher,
		}
	}
	/// Print diagnostic information about this transaction file.
	///
	/// This function is for debugging only.
	pub fn print_info<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<()> {
		self.segments.print_info(w)
	}
}

/// An iterator over a range of keys
///
/// This struct implements `Iterator` and yields items of [`Record`].
pub struct StringKeyRangeReader<'rdr, 'k> {
	pub(crate) reader: &'rdr Reader,
	pub(crate) range: crate::CowStringRange<'k>,
	decoded: Rc<Vec<u8>>,
	pos: usize,
	current_key_text_pos: usize,
	current_key_text_len: usize,
	current_fmt_text_pos: usize,
	current_fmt_text_len: usize,
	/// the size of the current record for this key (from the format string)
	current_key_record_len: Option<usize>,
	/// the size of the current record for this key (decoded or from the format string)
	current_record_len: Option<usize>,
	current_key_data_end: usize, // where the next key begins
	pub(crate) segment: Option<Segment<'rdr>>,
	pub(crate) matcher: Option<regex::Regex>,
	pub(crate) prefix: &'k str,
	_phantom: std::marker::PhantomData<&'k str>,
}

impl<'rdr, 'k> StringKeyRangeReader<'rdr, 'k> {
	/// Determines the on-disk size of this range of data,
	/// which is useful for estimating progress and size.
	pub fn compressed_bytes(&self) -> usize {
		if self.segment.is_none() {
			return 0;
		}

		// find the page that's after the last I need
		let segment_after_end;
		match self.range.end_bound() {
			Bound::Included(i) => {
				let mut s = self.reader.segments.find_after(|o| o.cmp(i));
				while let Some(seg) = &s {
					if seg.last_key > i {
						break;
					}
					s = self.reader.segments.segment_after(seg);
				}
				segment_after_end = s;
			}
			Bound::Excluded(i) => {
				segment_after_end = self.reader.segments.find_after(|o| o.cmp(i));
			}
			Bound::Unbounded if self.prefix.is_empty() => {
				segment_after_end = None;
			}
			Bound::Unbounded => {
				let prefix = self.prefix;

				segment_after_end = self.reader.segments.find_after(|o| {
					let oo = &o[0..std::cmp::min(o.len(), prefix.len())];
					let c = oo.cmp(prefix);
					if c == std::cmp::Ordering::Equal && oo.len() >= prefix.len() {
						return std::cmp::Ordering::Less;
					}
					c
				});
			}
		}

		let range_bytes;
		if let Some(s) = segment_after_end {
			range_bytes = s.segment_offset - self.segment.as_ref().unwrap().segment_offset;
		} else {
			range_bytes = self.reader.segments.number_of_bytes()
				- self.segment.as_ref().unwrap().segment_offset;
		}
		range_bytes
	}

	fn next_segment(&mut self) {
		self.pos = 0;

		let s = self
			.reader
			.segments
			.segment_after(&self.segment.take().unwrap());
		self.segment = s;

		if let Some(s) = self.segment.as_ref() {
			let reuse_vec = std::mem::replace(&mut self.decoded, Rc::new(vec![]));
			let mut old_vec;
			if let Ok(maybe_old_vec) = Rc::try_unwrap(reuse_vec) {
				old_vec = maybe_old_vec;
			} else {
				old_vec = vec![];
			}
			old_vec.clear();
			crate::segment_reader::decode_into_with_unescaping(&mut old_vec, s.payload);
			self.decoded = Rc::new(old_vec);
		}
	}

	fn next_key(&mut self) -> bool {
		while let Some(segment) = self.segment.as_ref() {
			while self.pos != self.decoded.len() {
				let data = &self.decoded;
				let klen = BigEndian::read_u32(&data[self.pos..self.pos + 4]) as usize;
				let flen = BigEndian::read_u32(&data[self.pos + 4..self.pos + 8]) as usize;

				let pos = if segment.segment_version == 0x0000 {
					self.pos + 12
				} else {
					self.pos + 8
				};

				let dlen = BigEndian::read_u32(&data[pos..pos + 4]) as usize;

				let pos = pos + 4;

				let key = &data[pos..pos + klen];
				let key = std::str::from_utf8(key).expect("input data is not utf8");
				let fmt = &data[pos + klen..pos + klen + flen];
				let fmt = std::str::from_utf8(fmt).expect("input data is not utf8");

				self.current_key_text_pos = pos;
				self.current_key_text_len = klen;
				self.current_fmt_text_pos = pos + klen;
				self.current_fmt_text_len = flen;
				let pos = pos + klen + flen;

				if let Some(len) = crate::row_format::row_format_size(fmt) {
					self.current_record_len = Some(len);
					self.pos = pos;
					self.current_key_record_len = Some(len);
				} else {
					self.current_record_len = None;
					self.current_key_record_len = None;
				}

				self.pos = pos;
				self.current_key_data_end = pos + dlen;

				match self.range.start_bound() {
					Bound::Included(v) => {
						if key < v {
							self.pos = self.current_key_data_end;
							continue;
						}
					}
					Bound::Excluded(v) => {
						if key <= v {
							self.pos = self.current_key_data_end;
							continue;
						}
					}
					Unbounded => {}
				}

				match self.range.end_bound() {
					Bound::Included(v) => {
						if key > v {
							self.pos = data.len();
							self.segment = None;
							return false;
						}
					}
					Bound::Excluded(v) => {
						if key >= v {
							self.pos = data.len();
							self.segment = None;
							return false;
						}
					}
					Unbounded => {
						if !key.starts_with(self.prefix) {
							self.pos = data.len();
							self.segment = None;
							return false;
						}
					}
				}

				if let Some(regex) = self.matcher.as_ref() {
					if !regex.is_match(key) {
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

impl<'rdr, 'k> Iterator for StringKeyRangeReader<'rdr, 'k> {
	type Item = Record;
	fn next(&mut self) -> Option<Self::Item> {
		self.segment.as_ref()?;

		if self.pos == self.current_key_data_end && !self.next_key() {
			return None;
		}

		let data = self.decoded.clone();

		let current_record_len;
		if let Some(len) = self.current_record_len {
			current_record_len = len;
		} else {
			let data = &data[self.pos..];
			let (len, tail) = unsigned_varint::decode::u64(data).unwrap();
			let varint_len = data.len() - tail.len();
			self.pos += varint_len;
			current_record_len = len as usize;
		}

		let r = Record {
			key_pos: self.current_key_text_pos,
			key_len: self.current_key_text_len,
			fmt_pos: self.current_fmt_text_pos,
			fmt_len: self.current_fmt_text_len,
			value_pos: self.pos,
			value_len: current_record_len + crate::TIMESTAMP_SIZE,
			data,
		};

		self.pos += current_record_len + crate::TIMESTAMP_SIZE;
		Some(r)
	}
}
