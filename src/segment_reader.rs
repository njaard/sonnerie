//use byteorder::{BigEndian};

use crate::Segment;
use std::io::Seek;

pub struct SegmentReader
{
	map: memmap::Mmap,
	len: usize,
}

impl SegmentReader
{
	pub fn open(file: &mut std::fs::File)
		-> std::io::Result<SegmentReader>
	{
		let len = file.seek(std::io::SeekFrom::End(0))? as usize;
		let map = unsafe { memmap::Mmap::map(file)? };
		Ok(SegmentReader
		{
			map,
			len,
		})
	}

	/// instructs the OS I'm going to sequentially read starting here
	pub(crate) fn advise<'s>(&self, from: &Segment<'s>)
	{
		use libc::{_SC_PAGESIZE, sysconf, c_void};
		let pagesize = unsafe { sysconf(_SC_PAGESIZE) as usize };

		let from = from.payload.as_ptr() as usize;
		let aligned_from = from & !(pagesize-1);

		let end_map = self.map[ self.map.len() ..].as_ptr() as usize;

		let len = end_map-aligned_from;

		unsafe
		{
			libc::posix_madvise(
				aligned_from as *mut c_void,
				len,
				libc::POSIX_MADV_SEQUENTIAL,
			);
		}
	}

	pub fn print_info<W: std::io::Write>(&self, w: &mut W)
		-> std::io::Result<()>
	{
		let mut segment = self.first();
		while let Some(s) = segment.take()
		{
			let fk = String::from_utf8_lossy(s.first_key);
			let lk = String::from_utf8_lossy(s.last_key);
			writeln!(
				w,
				"first_key=\"{}\", last_key=\"{}\", \
				offset={}, len={}, prev_sz={}",
				fk, lk, s.pos, s.payload.len(), s.prev_size
			)?;
			segment = self.segment_after(&s);
		}
		Ok(())
	}

	pub(crate) fn first<'s>(&'s self) -> Option<Segment<'s>>
	{
		Segment::scan(&self.map[..], 0)
	}

	pub(crate) fn find<'s>(&'s self, key: &[u8]) -> Option<Segment<'s>>
	{
		// do a binary search for the segment that contains key
		let mut begin = 0;
		let mut end = self.len-1;

		let data = &self.map;

		loop
		{
			let mut pos = (end-begin)/2 + begin;
			if pos < begin+1024*128
			{
				pos = begin;
			}

			let segment = Segment::scan(&data[ pos ..], pos);
			if segment.is_none()
			{
				end = pos-1;
				continue;
			}
			let segment = segment.unwrap();

			if key >= segment.first_key && key <= segment.last_key
				{ return Some(segment); }

			if pos == 0 && key < segment.first_key
				{ return Some(segment); }

			if key < segment.first_key
			{ // go to a smaller index
				end = std::cmp::min(
					pos-1,
					segment.pos - segment.prev_size + crate::SEGMENT_INVOCATION.len()
				);
				if end < begin { return None; }
			}
			else if key > segment.last_key
			{ // go to a larger index
				if begin == segment.pos { return None; }
				begin = segment.pos + segment.payload.len();
				if begin > end { return None; }
			}
			else
			{
				return None;
			}
		}
	}

	pub(crate) fn segment_after<'s>(&'s self, segment: &Segment<'s>)
		-> Option<Segment<'s>>
	{
		let data = &self.map;
		Segment::scan(
			&data[segment.pos+segment.payload.len() ..],
			segment.pos+segment.payload.len()
		)
	}
}

