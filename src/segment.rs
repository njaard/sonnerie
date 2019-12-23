
use byteorder::{ByteOrder,BigEndian};

pub(crate) const SEGMENT_INVOCATION: &[u8; 16] = b"@TSDB_SEGMENT_\0\0";

// a segment has a fixed 16 byte invocation
// then it has the key range it contains
// then it has the compressed data
pub(crate) struct Segment<'data>
{
	pub(crate) first_key: &'data [u8],
	pub(crate) last_key: &'data [u8],
	pub(crate) payload: &'data [u8],
	pub(crate) pos: usize,
	pub(crate) prev_size: usize,
}


impl<'data> Segment<'data>
{
	// read from `from` until I find a header, returning it
	// if there is one
	pub(crate) fn scan(from: &'data [u8], origin: usize)
		-> Option<Segment<'data>>
	{
		let at = twoway::find_bytes(from, SEGMENT_INVOCATION);
		if at.is_none() { return None; }
		let at = at.unwrap() + SEGMENT_INVOCATION.len();

		if from[at ..].len() < 16 { return None; }

		// the length of the first key
		let len1 = BigEndian::read_u32(&from[at+0 .. at+4]) as usize;
		// the length of the last key
		let len2 = BigEndian::read_u32(&from[at+4 .. at+8]) as usize;
		// the length of the payload
		let len3 = BigEndian::read_u32(&from[at+8 .. at+12]) as usize;
		// the compressed size of the previous segment
		let prev_size = BigEndian::read_u32(&from[at+12 .. at+16]) as usize;
		let at = at + 16;

		if from[at ..].len() < len1+len2+len3 { return None; }

		let first_key = &from[at .. at + len1];

		let at = at + len1;
		let last_key = &from[at .. at + len2];

		let at = at + len2;
		let payload = &from[at .. at + len3];

		Some(
			Segment
			{
				first_key,
				last_key,
				payload,
				pos: at + origin,
				prev_size,
			}
		)
	}

}

