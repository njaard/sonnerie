
use ::std::io::Write;

/// This trait is implemented for the integer types
/// that Sonnerie supports.
pub trait ToValue
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>;
}

impl ToValue for f64
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>
	{
		write!(to, "{:.17}", self)?;
		Ok(())
	}
}

impl ToValue for f32
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>
	{
		write!(to, "{:.17}", self)?;
		Ok(())
	}
}

impl ToValue for u64
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>
	{
		write!(to, "{}", self)?;
		Ok(())
	}
}

impl ToValue for u32
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>
	{
		write!(to, "{}", self)?;
		Ok(())
	}
}

impl ToValue for i64
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>
	{
		write!(to, "{}", self)?;
		Ok(())
	}
}

impl ToValue for i32
{
	fn serialize(&self, to: &mut Write)
		-> ::std::io::Result<()>
	{
		write!(to, "{}", self)?;
		Ok(())
	}
}

