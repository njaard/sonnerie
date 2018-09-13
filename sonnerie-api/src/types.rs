
use ::std::io::Write;

/// This trait is implemented for the integer types
/// that Sonnerie supports.
///
/// Sonnerie's API accepts this type in places
/// where any column value is acceptable. (The
/// server checks for compatibility)
pub trait FromValue
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>;
}

impl FromValue for f64
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>
	{
		write!(to, "{:.17}", self)?;
		Ok(())
	}
}

impl FromValue for f32
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>
	{
		write!(to, "{:.17}", self)?;
		Ok(())
	}
}

impl FromValue for u64
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>
	{
		write!(to, "{}", self)?;
		Ok(())
	}
}

impl FromValue for u32
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>
	{
		write!(to, "{}", self)?;
		Ok(())
	}
}

impl FromValue for i64
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>
	{
		write!(to, "{}", self)?;
		Ok(())
	}
}

impl FromValue for i32
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>
	{
		write!(to, "{}", self)?;
		Ok(())
	}
}

/// This is a reference to a column's value.
/// You can call `from` to convert it to a
/// concrete type.
pub struct Column<'v>
{
	pub(crate) serialized: &'v str,
}

impl<'v> Column<'v>
{
	/// Convert to an inferred concrete (numeric) type
	///
	/// Or return None if the conversion could not be done.
	pub fn from_checked<Type: ToValue>(&self)
		-> Option<Type>
	{
		Type::from_checked(self.serialized)
	}

	/// Convert to an inferred concrete (numeric) type
	///
	/// Panic if the type cannot be converted
	pub fn from<Type: ToValue>(&self)
		-> Type
	{
		self.from_checked().unwrap()
	}
	pub(crate) fn copy(&self) -> OwnedColumn
	{
		OwnedColumn
		{
			serialized: self.serialized.to_owned(),
		}
	}
}

impl<'v> ::std::fmt::Display for Column<'v>
{
	fn fmt(&self, f: &mut ::std::fmt::Formatter)
		-> ::std::fmt::Result
	{
		write!(f, "{}", self.serialized)
	}
}


/// Same as `Column`, except can be moved
/// and is heavier weight.
#[derive(Debug,Clone)]
pub struct OwnedColumn
{
	pub(crate) serialized: String,
}

impl ::std::fmt::Display for OwnedColumn
{
	fn fmt(&self, f: &mut ::std::fmt::Formatter)
		-> ::std::fmt::Result
	{
		write!(f, "{}", self.serialized)
	}
}

impl OwnedColumn
{
	pub fn from_checked<Type: ToValue>(&self)
		-> Option<Type>
	{
		Type::from_checked(&self.serialized)
	}
	pub fn from<Type: ToValue>(&self)
		-> Type
	{
		self.from_checked().unwrap()
	}
}

pub trait ToValue: Sized
{
	fn from_checked(serialifrzed: &str)
		-> Option<Self>;
	fn from(serialized: &str) -> Self
	{
		Self::from_checked(serialized).unwrap()
	}
}

impl ToValue for f64
{
	fn from_checked(serialized: &str)
		-> Option<Self>
	{
		serialized.parse().ok()
	}
}

impl ToValue for f32
{
	fn from_checked(serialized: &str)
		-> Option<Self>
	{
		serialized.parse().ok()
	}
}

impl ToValue for u64
{
	fn from_checked(serialized: &str)
		-> Option<Self>
	{
		serialized.parse().ok()
	}
}

impl ToValue for u32
{
	fn from_checked(serialized: &str)
		-> Option<Self>
	{
		serialized.parse().ok()
	}
}


impl ToValue for i64
{
	fn from_checked(serialized: &str)
		-> Option<Self>
	{
		serialized.parse().ok()
	}
}

impl ToValue for i32
{
	fn from_checked(serialized: &str)
		-> Option<Self>
	{
		serialized.parse().ok()
	}
}
