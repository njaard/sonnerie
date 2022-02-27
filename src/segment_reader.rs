//use byteorder::{BigEndian};

use crate::Segment;
use std::io::Seek;
use either::Either;
use std::io::Read;
use chrono::NaiveDateTime;

pub(crate) struct SegmentReader {
	map: memmap::Mmap,
	len: usize,
}

impl SegmentReader {
	pub(crate) fn open(file: &mut std::fs::File) -> std::io::Result<Either<SegmentReader, DeleteMarker>> {
        use Either::*;
        use byteorder::BigEndian;
        use byteorder::ByteOrder as _;

		let len = file.seek(std::io::SeekFrom::End(0))? as usize;
		let map = unsafe { memmap::Mmap::map(file)? };
		let reader = SegmentReader {
            map,
            len
        };

        if let Some(segment) = reader.first() {
            // read the payload of the segment and check its first few bytes
            let mut buffer = vec![];
            decode_into_with_unescaping(&mut buffer, segment.payload);

            if &buffer[0 .. 2] == "\u{007f}".as_bytes() {
                // we've found the format. now we have to write the marker by
                // parsing all fields from it
                
                // first 8 bytes being the first timestamp
                let ts_slice = &buffer[2 .. 10];
                let ts_u64 = BigEndian::read_u64(ts_slice);
                let start_ts = NaiveDateTime::from_timestamp(
                    (ts_u64 / 1_000_000_000) as i64,
                    (ts_u64 % 1_000_000_000) as u32,
                );

                // next 8 bytes being the last timestamp
                let ts_slice = &buffer[10 .. 18];
                let ts_u64 = BigEndian::read_u64(ts_slice);
                let end_ts = NaiveDateTime::from_timestamp(
                    (ts_u64 / 1_000_000_000) as i64,
                    (ts_u64 % 1_000_000_000) as u32,
                );

                // next set of bytes is a varint containing the length of the
                // wildcard
                let (wc_len, next_slice) = unsigned_varint::decode::usize(
                        &buffer[18 ..]
                    ).expect("Failed to read varint: not enough bytes");

                // read, from the next slice, the slice for the filter string
                let wildcard_slice = &next_slice[0 .. wc_len];
                let wildcard = String::from_utf8(wildcard_slice.to_vec())
                    .unwrap();

                // next set of bytes is also another varint containing the
                // length of the last key
                let (wc_len, next_slice) = unsigned_varint::decode::usize(
                        &next_slice[wc_len ..],
                    ).expect("Failed to read varint: not enough bytes");

                // read from the next slice, the slice for the last key
                let last_key_slice = &next_slice[0 .. wc_len];
                let last_key = String::from_utf8(last_key_slice.to_owned());

                let marker = DeleteMarker {
                    first_key: segment.first_key.to_owned(),
                    last_key: segment.last_key.to_owned(),
                    first_timestamp: start_ts,
                    last_timestamp: end_ts,
                    wildcard,
                };

                return Ok(Right(marker))
            }
        }

        Ok(Left(reader))
	}

	/// instructs the OS I'm going to sequentially read starting here
	pub(crate) fn advise<'s>(&self, from: &Segment<'s>) {
		use libc::{c_void, sysconf, _SC_PAGESIZE};
		let pagesize = unsafe { sysconf(_SC_PAGESIZE) as usize };

		let from = from.payload.as_ptr() as usize;
		let aligned_from = from & !(pagesize - 1);

		let end_map = self.map[self.map.len()..].as_ptr() as usize;

		let len = end_map - aligned_from;

		unsafe {
			libc::posix_madvise(
				aligned_from as *mut c_void,
				len,
				libc::POSIX_MADV_SEQUENTIAL,
			);
		}
	}

	pub(crate) fn number_of_bytes(&self) -> usize {
		self.len
	}

	pub(crate) fn print_info<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<()> {
		let mut segment = self.first();
		while let Some(s) = segment.take() {
			let fk = s.first_key;
			let lk = s.last_key;
			writeln!(
				w,
				"first_key=\"{}\", last_key=\"{}\", \
				offset={}, len={}, prev_sz={}, this_key_prev={}",
				fk,
				lk,
				s.segment_offset,
				s.payload.len(),
				s.prev_size,
				s.this_key_prev,
			)?;
			segment = self.segment_after(&s);
		}
		Ok(())
	}

	pub(crate) fn first(&self) -> Option<Segment> {
		Segment::scan(&self.map[..], 0)
	}

	pub(crate) fn scan_from(&self, pos: usize) -> Option<Segment> {
		Segment::scan(&self.map[pos..], pos)
	}

	pub(crate) fn find<'s>(&'s self, key: &str) -> Option<Segment<'s>> {
		// do a binary search for the segment that contains key
		let mut begin = 0;
		let mut end = self.len - 1;

		let data = &self.map;

		loop {
			let mut pos = (end - begin) / 2 + begin;
			let mut search_here = true;
			while search_here {
				search_here = false;
				if pos < begin + 1024 * 128 {
					pos = begin;
				}

				let segment = Segment::scan(&data[pos..], pos);

				if segment.is_none() {
					end = pos - 1;
					continue;
				}
				let segment = segment.unwrap();

				if pos == 0 && key < segment.first_key {
					return Some(segment);
				}

				if key == segment.first_key && segment.this_key_prev != 0 {
					pos = segment.segment_offset - segment.this_key_prev;
					search_here = true;
					continue;
				}

				if key >= segment.first_key && key <= segment.last_key {
					return Some(segment);
				}

				if key < segment.first_key {
					// go to a smaller index
					end = std::cmp::min(
						pos - 1,
						std::cmp::min(
							segment.segment_offset - segment.prev_size,
							// we know we can reverse past this entire key
							segment.segment_offset - segment.this_key_prev,
						),
					);
					if end < begin {
						return None;
					}
				} else if key > segment.last_key {
					begin = segment.segment_offset + segment.stride;
					if begin > end {
						return None;
					}
				} else {
					return None;
				}
			}
		}
	}

	/// do a binary search for the first segment after the one
	/// that contains `key`.
	pub(crate) fn find_after(&self, cmp: impl Fn(&str) -> std::cmp::Ordering) -> Option<Segment> {
		let mut begin = 0;
		let mut end = self.len - 1;

		let data = &self.map;

		loop {
			let mut pos = (end - begin) / 2 + begin;
			let mut search_here = true;
			while search_here {
				search_here = false;
				if pos < begin + 1024 * 128 {
					pos = begin;
				}

				let segment = Segment::scan(&data[pos..], pos);
				if segment.is_none() {
					end = pos - 1;
					continue;
				}
				let segment = segment.unwrap();

				let cmp_last_key = cmp(segment.last_key);

				if pos == 0 && cmp_last_key.is_ge() {
					return Some(segment);
				}

				let cmp_first_key = cmp(segment.first_key);

				if cmp_first_key.is_le() && cmp_last_key.is_ge() {
					return Some(segment);
				} else if cmp_last_key.is_ge() {
					end = std::cmp::min(
						pos - 1,
						std::cmp::min(
							segment.segment_offset - segment.prev_size,
							// we know we can reverse past this entire key
							segment.segment_offset - segment.this_key_prev,
						),
					);
					if end < begin {
						return self.segment_after(&segment);
					}
				} else if cmp_first_key.is_lt() {
					begin = segment.segment_offset + segment.stride;
					if begin > end {
						return Some(segment);
					}
				} else {
					unreachable!();
				}
			}
		}
	}

	pub(crate) fn segment_after<'s>(&'s self, segment: &Segment<'s>) -> Option<Segment<'s>> {
		let data = &self.map;
		let next = segment.segment_offset + segment.stride;
		Segment::scan(&data[next..], next)
	}
}

pub (crate) fn decode_into_with_unescaping(into: &mut Vec<u8>, from: &[u8]) {
	let mut segmented: smallvec::SmallVec<[_; 4]> = smallvec::smallvec![];
	{
		let mut start = 0;
		while let Some(pos) = crate::segment::find_escape_segment_invocation(&from[start..]) {
			segmented.push(&from[start..pos + start]);
			segmented.push(crate::segment::SEGMENT_INVOCATION);
			start = start + pos + crate::segment::ESCAPE_SEGMENT_INVOCATION.len();
		}
		segmented.push(&from[start..]);
	}

	let mut reader: Option<Box<dyn Read>> = None;

	for segment in segmented {
		if let Some(head) = reader {
			reader = Some(Box::new(head.chain(std::io::Cursor::new(segment))) as Box<_>);
		} else {
			reader = Some(Box::new(std::io::Cursor::new(segment)) as Box<_>);
		}
	}

	let mut decoder = lz4::Decoder::new(reader.expect("empty segment")).expect("lz4 decoding");
	decoder.read_to_end(into).expect("lz4 decoding 2");
}

pub struct DeleteMarker {
    pub first_key: String,
    pub last_key: String,
    pub first_timestamp: NaiveDateTime,
    pub last_timestamp: NaiveDateTime,
    pub wildcard: String,
}
