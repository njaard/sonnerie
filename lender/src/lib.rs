use std::ops::{Deref,DerefMut};
use std::sync::Arc;

struct LenderInternal<T>
{
	thing: T,
}

pub struct Lender<T>
{
	internal: Arc<LenderInternal<T>>
}

impl<T> Lender<T>
{
	pub fn new(thing: T) -> Self
	{
		Self
		{
			internal: Arc::new(LenderInternal
			{
				thing,
			})
		}
	}
	pub fn try_get(&self) -> Option<&T>
	{
		if Arc::strong_count(&self.internal) == 1
		{
			Some(&unsafe { &*Arc::as_ptr(&self.internal) }.thing)
		}
		else { None }
	}

	pub fn get(&self) -> &T { self.try_get().unwrap() }

	pub fn try_get_mut(&mut self) -> Option<&mut T>
	{
		if Arc::strong_count(&self.internal) == 1
		{
			Some(&mut unsafe { &mut *(Arc::as_ptr(&self.internal) as *mut LenderInternal<T>) } .thing )
		}
		else { None }
	}

	pub fn get_mut(&mut self) -> &mut T { self.try_get_mut().unwrap() }

	pub fn to_borrower(&mut self) -> Borrower<T>
	{
		Borrower
		{
			lender: self.internal.clone(),
		}
	}
}


pub struct Borrower<T>
{
	lender: Arc<LenderInternal<T>>,
}

impl<T> Deref for Borrower<T>
{
	type Target = T;
	fn deref(&self) -> &T
	{
		&unsafe { &*Arc::as_ptr(&self.lender) }.thing
	}
}

impl<T> DerefMut for Borrower<T>
{
	fn deref_mut(&mut self) -> &mut T
	{
		&mut unsafe { &mut *(Arc::as_ptr(&self.lender) as *mut LenderInternal<T>) } .thing
	}
}

#[cfg(test)]
mod tests
{
	use crate::Lender;
	#[test]
	fn test1()
	{
		let mut lender = Lender::new(format!("hello"));
		let b = lender.to_borrower();
		assert!(lender.try_get().is_none());
		assert_eq!(&*b, "hello");
		drop(b);
		assert!(lender.try_get().is_some());
	}
}

