extern crate range;
extern crate antidote;

use intrusive_collections::{RBTree, RBTreeLink, KeyAdapter, Bound, Adapter};
use intrusive_collections::rbtree::{CursorMut, Link};

use self::range::Range;

use self::antidote::RwLock;

use block_file::BlockFile;

/// Manage the contents of the write-ahead-log file.
///
/// also maintain a copy of it in memory
/// the memory-based one is gradually flushed to disk
/// and is removed from memory when written to disk
///
/// old Wal files are removed when the block data is
/// fsynced



#[derive(Debug,Default)]
struct WalPart
{
	link: RBTreeLink,
	offset: usize,
	data: Vec<u8>,
	// what's the oldest generation of this block
	min_generation: u64,
}


intrusive_adapter!(WalPartAdapter = Box<WalPart>: WalPart { link: RBTreeLink });

impl<'a> KeyAdapter<'a> for WalPartAdapter
{
	type Key = usize;
	fn get_key(&self, x: &'a WalPart) -> Self::Key { x.offset }
}

impl WalPart
{
	fn len(&self) -> usize { self.data.len() }
}

unsafe impl Sync for WalPart {}

type PartBTree = RBTree<WalPartAdapter>;

unsafe fn cursor_get_mut<'a, 'b, A: Adapter<Link = Link> + 'a>(
	cursor: &'a CursorMut<'a, A>
) -> Option<&'b mut A::Value>
{
	let c = cursor.get();
	::std::mem::transmute::<
		Option<&'a A::Value>,
		Option<&'b mut A::Value>
	>(c)
}


// todo: optimization: keep old 'data' Vecs around so we
// we don't have to reallocate them

#[derive(Clone)]
struct Segment<'a>
{
	offset: usize,
	data: &'a [u8],
}


unsafe fn merge_next(
	current: *const WalPart, wal: &MemoryWal, block_file: &BlockFile
) -> *const WalPart
{
	let afterptr: *const WalPart;
	let in_flight_ptr: *const WalPart;

	{
		// lock in_flight first, because the reader locks it first too
		let mut in_flight = wal.in_flight.write();
		let mut src = wal.parts.write();

		let mut cursor =
			if current.is_null()
				{ src.front_mut() }
			else
				{ src.cursor_mut_from_ptr(current) };

		{
			let after = cursor.peek_next();
			afterptr =
				if let Some(after) = after.get()
					{ after as *const WalPart }
				else
					{ ::std::ptr::null() };
		}

		let boxed = cursor.remove();
		if let Some(b) = boxed.as_ref()
		{
			in_flight_ptr = &**b as *const WalPart;
		}
		else
		{
			in_flight_ptr  = ::std::ptr::null()
		}
		*in_flight = boxed;
	}

	if !in_flight_ptr.is_null()
	{
		let ref in_flight = *in_flight_ptr;
		block_file.write(in_flight.offset as u64, &in_flight.data);
	}

	afterptr

}

// integrate the memory wal into the "blocks" file.
// after that occurs, Db does an fsync on blocks
// and can delete the on-disk wal files.
pub fn merge(
	wal: &MemoryWal,
	block_file: &BlockFile,
)
{
	let mut position = ::std::ptr::null();

	loop
	{
		position = unsafe { merge_next(position, wal, block_file) };
		if position.is_null()
			{ break; }
	}
}


#[derive(Debug)]
pub struct MemoryWal
{
	parts: RwLock<PartBTree>,
	// TODO: in_flight should be made a vector, because
	// we want BlockFile to deal it BufWriter objects
	// and not File directly
	in_flight: RwLock<Option<Box<WalPart>>>,
}

impl MemoryWal
{
	pub fn new() -> Self
	{
		Self
		{
			parts: RwLock::new(RBTree::new(WalPartAdapter::new())),
			in_flight: RwLock::new(None),
		}
	}

	pub fn write(&self, offset: usize, data: &[u8])
	{
		let mut s = Segment{ offset: offset, data: data };

		let mut parts = self.parts.write();

		while s.data.len()!=0
		{
			let nextseg = Self::write_at_start(&mut parts, &s);
			let nextseg = Self::extend_middle(&mut parts, &nextseg);
			::std::mem::replace(&mut s, nextseg);
		}
	}

	fn apply_part(part: &WalPart, offset: usize, data: &mut [u8])
		-> bool
	{
		let part_pos = part.offset;
		let ref part_data = part.data;

		let rdata = Range::new(offset, data.len());
		let rpart = Range::new(part_pos, part_data.len());

		if let Some(rov) = rpart.intersect(&rdata)
		{
			let dstov = rov.offset - offset;
			let srcov = rov.offset - part_pos;
			data[dstov .. dstov+rov.length]
				.copy_from_slice(&part_data[srcov .. srcov+rov.length]);
			true
		}
		else
		{
			false
		}
	}

	pub fn read(&self, offset: usize, data: &mut [u8])
	{
		let in_flight_part = self.in_flight.read();
		if let Some(part) = in_flight_part.as_ref()
		{
			Self::apply_part(&*part, offset, data);
		}

		let parts = self.parts.read();

		{ // get the previous parts
			let mut overlay_parts =
				parts.upper_bound(
					Bound::Excluded(&(offset+data.len()))
				);

			while let Some(part) = overlay_parts.get()
			{
				if !Self::apply_part(part, offset, data)
					{ break; }
				overlay_parts.move_prev();
			}
		}

		{ // get the following parts
			let mut overlay_parts =
				parts.lower_bound(
					Bound::Excluded(&offset)
				);

			while let Some(part) = overlay_parts.get()
			{
				if !Self::apply_part(part, offset, data)
					{ break; }
				overlay_parts.move_next();
			}
		}
	}

	// replace a part with the first part of `s`
	// or create a new part, up until that block ends
	fn write_at_start<'a>(parts: &mut PartBTree, s: &Segment<'a>)
		-> Segment<'a>
	{
		let rdata = Range::new(s.offset, s.data.len());
		if let Some(bpart) = Self::before_mut(parts, s.offset+s.data.len())
		{
			let rbefore = Range::new(bpart.offset, bpart.len());

			if let Some(rov) = rbefore.intersect(&rdata)
			{
				let o = rov.offset - bpart.offset;
				bpart.data[o .. o+rov.length]
					.copy_from_slice(&s.data[0..rov.length]);

				return Segment
				{
					offset: s.offset + rov.length,
					data: &s.data[rov.length..],
				};
			}
			else if bpart.offset + bpart.data.len() == s.offset
			{
				return s.clone();
			}
		}

		{
			let len_to_create;

			if let Some(apart) = Self::after_mut(parts, s.offset)
			{
				let rafter = Range::new(apart.offset, apart.len());

				if let Some(rov) = rafter.intersect(&rdata)
				{
					len_to_create = s.data.len() - rov.length;
				}
				else
				{
					len_to_create = s.data.len();
				}
			}
			else
			{
				len_to_create = s.data.len();
			}

			let p = WalPart
			{
				link: RBTreeLink::new(),
				offset: s.offset,
				data: s.data[0..len_to_create].to_vec(),
				min_generation: 0,
			};

			parts.insert(Box::new(p));

			return Segment
			{
				offset: s.offset + len_to_create,
				data: &s.data[len_to_create..],
			};
		}
	}

	fn extend_middle<'a>(parts: &mut PartBTree, s: &Segment<'a>)
		-> Segment<'a>
	{
		if s.data.len() == 0 { return s.clone(); }

		let rdata = Range::new(s.offset, s.data.len());

		let amount_to_extend_by;

		if let Some(apart) = Self::after(parts, s.offset)
		{
			let rafter = Range::new(apart.offset, apart.len());

			if let Some(rov) = rafter.intersect(&rdata)
			{
				amount_to_extend_by = rov.length;
			}
			else
			{
				amount_to_extend_by = s.data.len();
			}
		}
		else
		{
			amount_to_extend_by = s.data.len();
		}

		if let Some(bpart) = Self::before_mut(parts, s.offset)
		{
			bpart.data.extend_from_slice(&s.data[0..amount_to_extend_by]);
			Segment
			{
				offset: s.offset + amount_to_extend_by,
				data: &s.data[amount_to_extend_by..]
			}
		}
		else
		{
			s.clone()
		}

	}

	fn before_mut(parts: &mut PartBTree, position: usize)
		-> Option<&mut WalPart>
	{
		let before_part = parts
			.upper_bound_mut(Bound::Excluded(&position));
		unsafe { cursor_get_mut(&before_part) }
	}
	fn after_mut(parts: &mut PartBTree, position: usize)
		-> Option<&mut WalPart>
	{
		let after_part = parts
			.lower_bound_mut(Bound::Included(&position));
		unsafe { cursor_get_mut(&after_part) }
	}
	fn after(parts: &PartBTree, position: usize)
		-> Option<&WalPart>
	{
		let after_part = parts
			.lower_bound(Bound::Included(&position));
		after_part.get()
	}
}

#[cfg(test)]
mod tests
{
	extern crate tempfile;
	use ::wal::MemoryWal;
	use ::block_file::BlockFile;
	use ::wal::merge;

	fn r(w: &MemoryWal, pos: usize, len: usize) -> Vec<u8>
	{
		let mut b = vec![0u8; len];
		w.read(pos, &mut b);
		b.to_vec()
	}

	fn blockfile() -> (tempfile::TempDir, BlockFile)
	{
		let tmp = tempfile::TempDir::new().unwrap();
		let b = BlockFile::new(&tmp.path().join("bl"));
		(tmp, b)
	}

	#[test]
	fn wal_to_disk()
	{
		let w = MemoryWal::new();
		w.write(0, b"abc");
		w.write(3, b"def");
		w.write(9, b"XXX");

		let (_tmp, b) = blockfile();
		merge(&w, &b);
		let mut out = [0u8; 12];
		b.read(0, &mut out);
		assert_eq!(&out, b"abcdef\0\0\0XXX");
	}

	#[test]
	fn rw1()
	{
		let w = MemoryWal::new();
		w.write(0, b"abc");
		w.write(3, b"def");
		w.write(9, b"XXX");
		assert_eq!(r(&w, 0, 12), b"abcdef\0\0\0XXX");
	}
	#[test]
	fn rw2()
	{
		let w = MemoryWal::new();
		w.write(10, b"abc");
		w.write(13, b"def");
		w.write(19, b"XXX");
		assert_eq!(r(&w, 10, 12), b"abcdef\0\0\0XXX");
	}
	#[test]
	fn rw3()
	{
		let w = MemoryWal::new();
		w.write(10, b"abc");
		w.write(11, b"lmn");
		assert_eq!(r(&w, 10, 4), b"almn");
	}
	#[test]
	fn rw4()
	{
		let w = MemoryWal::new();
		w.write(20, b"abc");
		w.write(10, b"lmn");
		assert_eq!(r(&w, 10, 13), b"lmn\0\0\0\0\0\0\0abc");
	}
	#[test]
	fn wal_overlap()
	{
		let w = MemoryWal::new();
		w.write(0, b"abcd");
		w.write(0, b"defg");
		assert_eq!(r(&w, 0, 4), b"defg");
		w.write(0, b"hijkl");
		assert_eq!(r(&w, 0, 5), b"hijkl");
		w.write(1, b"bcdefg");
		assert_eq!(r(&w, 0, 7), b"hbcdefg");
	}

}
