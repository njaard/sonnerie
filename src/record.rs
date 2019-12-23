use std::rc::Rc;

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
  pub fn key(&self) -> &str
  {
    let d = &self.data[self.key_pos .. self.key_pos+self.key_len];
    unsafe
    {
      std::str::from_utf8_unchecked(&d)
    }
  }

  pub fn format(&self) -> &str
  {
    let d = &self.data[self.fmt_pos .. self.fmt_pos+self.fmt_len];
    unsafe
    {
      std::str::from_utf8_unchecked(&d)
    }
  }

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
