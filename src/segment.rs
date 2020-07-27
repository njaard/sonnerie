
use byteorder::{ByteOrder,BigEndian};

pub(crate) const SEGMENT_INVOCATION: &[u8; 14] = b"@TSDB_SEGMENT_";

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
	pub(crate) this_key_prev: usize,
}


impl<'data> Segment<'data>
{
	// read from `from` until I find a header, returning it
	// if there is one
	pub(crate) fn scan(from: &'data [u8], origin: usize)
		-> Option<Segment<'data>>
	{
		let first_len = from.len();

		let at = twoway::find_bytes(from, SEGMENT_INVOCATION);
		if at.is_none() { return None; }
		let at = at.unwrap() + SEGMENT_INVOCATION.len();


		let segment_version = BigEndian::read_u16(&from[at+0 .. at+2]) as usize;

		match segment_version
		{
			0 =>
			{
				if from[at ..].len() < 18 { return None; }
				let at = at + 2;
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
						this_key_prev: 0,
					}
				)
			},

			0x0100 =>
			{
				use unsigned_varint::decode::u32 as v32;
				let from = &from[at + 2 .. ];

				// the length of the first key
				let (len1,from) = v32(from).ok()?;
				// the length of the last key
				let (len2,from) = v32(from).ok()?;
				// the length of the payload
				let (len3,from) = v32(from).ok()?;
				// the compressed size of the previous segment
				let (prev_size,from) = v32(from).ok()?;
				// how many bytes we need to reverse to get to the start
				// of this key
				let (this_key_prev,from) = v32(from).ok()?;

				let len1 = len1 as usize;
				let len2 = len2 as usize;
				let len3 = len3 as usize;
				let prev_size = prev_size as usize;
				let this_key_prev = this_key_prev as usize;

				if from.len() < len1+len2+len3 { return None; }

				let first_key = &from[ 0 .. len1];

				let last_key = &from[len1 .. len1+len2];

				let pos = first_len - from.len() + origin + len1+len2;

				let payload = &from[len1+len2 .. len1+len2+len3];


				Some(
					Segment
					{
						first_key,
						last_key,
						payload,
						pos,
						prev_size,
						this_key_prev,
					}
				)
			},

			a =>
			{
				eprintln!("warning: invalid segment version {}", a);
				None
			},
		}

	}

}

