//! Stores a single row.

use std::rc::Rc;

/// Store the data for a record.
///
/// This object is cheaply copied because it is
/// internally reference counted.
pub struct OwnedRecord
{
	pub(crate) key_pos: usize,
	pub(crate) key_len: usize,
	pub(crate) fmt_pos: usize,
	pub(crate) fmt_len: usize,
	pub(crate) value_pos: usize,
	pub(crate) value_len: usize,
	pub(crate) data: Rc<Vec<u8>>,
}

impl OwnedRecord
{
	/// The key of this record.
	pub fn key(&self) -> &str
	{
		let d = &self.data[self.key_pos .. self.key_pos+self.key_len];
		unsafe
		{
			std::str::from_utf8_unchecked(&d)
		}
	}

	/// The format of this record (as the single-character codes)
	pub fn format(&self) -> &str
	{
		let d = &self.data[self.fmt_pos .. self.fmt_pos+self.fmt_len];
		unsafe
		{
			std::str::from_utf8_unchecked(&d)
		}
	}

	/// The encoded payload of this data. Use [`row_format`](../row_format/)
	/// to decode it.
	pub fn value(&self) -> &[u8]
	{
		&self.data[self.value_pos .. self.value_pos+self.value_len]
	}
}


impl std::fmt::Debug for OwnedRecord
{
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result
	{
		write!(f, "Record {{ key={} }}", self.key())
	}
}
