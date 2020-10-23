//use byteorder::{BigEndian};

use crate::Segment;
use std::io::Seek;

pub(crate) struct SegmentReader {
	map: memmap::Mmap,
	len: usize,
}

impl SegmentReader {
	pub(crate) fn open(file: &mut std::fs::File) -> std::io::Result<SegmentReader> {
		let len = file.seek(std::io::SeekFrom::End(0))? as usize;
		let map = unsafe { memmap::Mmap::map(file)? };
		Ok(SegmentReader { map, len })
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

	pub(crate) fn print_info<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<()> {
		let mut segment = self.first();
		while let Some(s) = segment.take() {
			let fk = String::from_utf8_lossy(s.first_key);
			let lk = String::from_utf8_lossy(s.last_key);
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

	pub(crate) fn first<'s>(&'s self) -> Option<Segment<'s>> {
		Segment::scan(&self.map[..], 0)
	}

	pub(crate) fn find<'s>(&'s self, key: &[u8]) -> Option<Segment<'s>> {
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

	pub(crate) fn segment_after<'s>(&'s self, segment: &Segment<'s>) -> Option<Segment<'s>> {
		let data = &self.map;
		let next = segment.segment_offset + segment.stride;
		Segment::scan(&data[next..], next)
	}
}
