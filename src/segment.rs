use byteorder::{BigEndian, ByteOrder};
use static_init::dynamic;

pub(crate) const SEGMENT_INVOCATION: &[u8; 14] = b"@TSDB_SEGMENT_";
pub(crate) const ESCAPE_SEGMENT_INVOCATION: &[u8; 16] = b"@TSDB_SEGMENT_\xff\xff";

#[dynamic]
static FINDER_SEGMENT_INVOCATION: memchr::memmem::Finder<'static> =
	memchr::memmem::Finder::new(SEGMENT_INVOCATION);
#[dynamic]
static FINDER_ESCAPE_SEGMENT_INVOCATION: memchr::memmem::Finder<'static> =
	memchr::memmem::Finder::new(ESCAPE_SEGMENT_INVOCATION);

pub(crate) fn find_segment_invocation(haystack: &[u8]) -> Option<usize> {
	FINDER_SEGMENT_INVOCATION.find(haystack)
}
pub(crate) fn find_escape_segment_invocation(haystack: &[u8]) -> Option<usize> {
	FINDER_ESCAPE_SEGMENT_INVOCATION.find(haystack)
}

// a segment has a fixed 16 byte invocation
// then it has the key range it contains
// then it has the compressed data
pub(crate) struct Segment<'data> {
	pub(crate) first_key: &'data str,
	pub(crate) last_key: &'data str,
	pub(crate) payload: &'data [u8],
	pub(crate) segment_offset: usize,
	pub(crate) prev_size: usize,
	pub(crate) this_key_prev: usize,
	pub(crate) segment_version: u16,
	pub(crate) stride: usize, // bytes from the start of the invocation to the next invocation
}

impl<'data> std::fmt::Debug for Segment<'data> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
		f.debug_struct("first_key")
			.field("first_key", &self.first_key)
			.field("last_key", &self.last_key)
			.field("segment_offset", &self.segment_offset)
			.field("prev_size", &self.prev_size)
			.field("this_key_prev", &self.this_key_prev)
			.field("stride", &self.stride)
			.finish()
	}
}

impl<'data> Segment<'data> {
	// read from `from` until I find a header, returning it
	// if there is one
	pub(crate) fn scan(from: &'data [u8], origin: usize) -> Option<Segment<'data>> {
		// relative_search_start makes it possible to skip invocation escape sequences
		let mut relative_search_start = 0;
		loop {
			let invocation_relative_at =
				self::FINDER_SEGMENT_INVOCATION.find(&from[relative_search_start..])?;
			let header =
				&from[relative_search_start + invocation_relative_at + SEGMENT_INVOCATION.len()..];
			if header.len() < 2 {
				// invalid
				return None;
			}
			let segment_offset = invocation_relative_at + relative_search_start + origin;

			let segment_version = BigEndian::read_u16(&header[0..2]);

			match segment_version {
				0 => {
					if header.len() < 18 {
						return None;
					}
					// the length of the first key
					let len1 = BigEndian::read_u32(&header[2..6]) as usize;
					// the length of the last key
					let len2 = BigEndian::read_u32(&header[6..10]) as usize;
					// the length of the payload
					let len3 = BigEndian::read_u32(&header[10..14]) as usize;
					// the compressed size of the previous segment
					let prev_size = BigEndian::read_u32(&header[14..18]) as usize;

					let at = 18;

					if header[at..].len() < len1 + len2 + len3 {
						return None;
					}

					let first_key = &header[at..at + len1];
					let first_key = std::str::from_utf8(first_key).expect("first_key is not utf-8");

					let at = at + len1;
					let last_key = &header[at..at + len2];
					let last_key = std::str::from_utf8(last_key).expect("last_key is not utf-8");

					let header_len = 18 + len1 + len2;
					let payload = &header[header_len..header_len + len3];

					return Some(Segment {
						first_key,
						last_key,
						payload,
						segment_offset,
						prev_size,
						this_key_prev: 0,
						segment_version,
						stride: SEGMENT_INVOCATION.len() + header_len + len3,
					});
				}

				0x0100 => {
					use unsigned_varint::decode::u32 as v32;
					let from = &header[2..];

					// the length of the first key
					let (len1, from) = v32(from).ok()?;
					// the length of the last key
					let (len2, from) = v32(from).ok()?;
					// the length of the payload
					let (len3, from) = v32(from).ok()?;
					// the compressed size of the previous segment
					let (prev_size, from) = v32(from).ok()?;
					// how many bytes we need to reverse to get to the start
					// of this key
					let (this_key_prev, from) = v32(from).ok()?;

					let len1 = len1 as usize;
					let len2 = len2 as usize;
					let len3 = len3 as usize;
					let prev_size = prev_size as usize;
					let this_key_prev = this_key_prev as usize;

					if from.len() < len1 + len2 + len3 {
						return None;
					}

					let header_len = len1 + len2 + (header.len() - from.len());
					let first_key = &from[0..len1];
					let first_key = std::str::from_utf8(first_key).expect("first_key is not utf-8");
					let last_key = &from[len1..len1 + len2];
					let last_key = std::str::from_utf8(last_key).expect("last_key is not utf-8");

					let payload = &header[header_len..header_len + len3];

					return Some(Segment {
						first_key,
						last_key,
						payload,
						segment_offset,
						prev_size,
						this_key_prev,
						segment_version,
						stride: SEGMENT_INVOCATION.len() + header_len + len3,
					});
				}

				0xffff => {
					// we found the escape character
					relative_search_start = invocation_relative_at + SEGMENT_INVOCATION.len();
					continue;
				}
				a => {
					eprintln!("warning: invalid segment version {}", a);
					return None;
				}
			}
		}
	}
}
