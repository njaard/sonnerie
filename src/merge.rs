
use std::collections::BinaryHeap;
use core::cmp::Ordering;
use std::rc::Rc;

struct Next<Source, Record>
{
	source: Source,
	current_record: Option<Rc<Record>>,
}

struct NextKey<Record>
{
	current_record: Rc<Record>,
	source_index: usize,
	compare_record: Box<dyn Fn(&Record, &Record)->Ordering>,
}

impl<Record> Ord for NextKey<Record>
{
	fn cmp(&self, other: &Self) -> Ordering
	{
		(self.compare_record)(&self.current_record, &other.current_record)
			.reverse()
			.then_with( || self.source_index.cmp(&other.source_index))
	}
}

impl<Record> PartialOrd for NextKey<Record>
{
	fn partial_cmp(&self, other: &Self) -> Option<Ordering>
	{
		Some(self.cmp(other))
	}
}

impl<Record> PartialEq for NextKey<Record>
{
	fn eq(&self, other: &Self) -> bool
	{
		(self.compare_record)(&self.current_record, &other.current_record)
			== Ordering::Equal
			&& (other.source_index == self.source_index)
	}
}

impl<Record> Eq for NextKey<Record> {}

/// merge various iterators into the lowest value,
/// choosing the last item as a tie-breaker
pub struct Merge<Source, Record>
where Source: Iterator<Item=Record>,
{
	sources: Vec<Next<Source, Record>>,
	sorter: BinaryHeap<NextKey<Record>>,
}

impl<Source, Record> Merge<Source, Record>
where Source: Iterator<Item=Record>,
{
	pub fn new<CompareRecord>(
		mut sources: Vec<Source>,
		compare_record: CompareRecord,
	) -> Self
	where
		CompareRecord: Fn(&Record, &Record) -> Ordering + Clone + 'static
	{
		let compare_record = Box::new(compare_record);

		let sources: Vec<_> =
			sources.drain(..)
				.filter_map(
					|mut src|
					{
						let current_record = src.next()?;
						Some(Next
						{
							source: src,
							current_record: Some(Rc::new(current_record)),
						})
					}
				)
				.collect();

		let mut sorter = BinaryHeap::with_capacity(sources.len());

		for (idx,src) in sources.iter().enumerate()
		{
			sorter.push(
				NextKey
				{
					source_index: idx,
					current_record: src.current_record.as_ref().unwrap().clone(),
					compare_record: compare_record.clone()
				}
			);
		}

		Self
		{
			sources,
			sorter,
		}
	}

	// continue to read next items until the next item read
	// won't match `current`.
	fn discard_repetitions(&mut self, current: &Record)
	{
		loop
		{
			{
				let next = self.sorter.peek();
				if next.is_none() { break; }
				let next = next.unwrap();

				match (next.compare_record)(current, &next.current_record)
				{
					Ordering::Less =>
					{
						break;
					}, // done
					Ordering::Greater => panic!("ordering violation"),
					Ordering::Equal => {}, // consume `next`
				}
			}

			let mut next = self.sorter.pop().unwrap();

			let source = &mut self.sources[next.source_index];
			let succ_record = source.source.next();
			if let Some(succ_record) = succ_record
			{
				assert!(
					(next.compare_record)(&next.current_record, &succ_record)
						!= Ordering::Greater
				);

				next.current_record = Rc::new(succ_record);
				source.current_record = Some(next.current_record.clone());
				self.sorter.push(next);
			}
		}
	}
}


impl<Source, Record> Iterator for Merge<Source, Record>
where Source: Iterator<Item=Record>,
	Record: std::fmt::Debug
{
	type Item = Record;

	fn next(&mut self) -> Option<Self::Item>
	{
		let mut next = self.sorter.pop()?;
		let source = &mut self.sources[next.source_index];

		let succ_record = source.source.next();
		if let Some(succ_record) = succ_record
		{
			assert!(
				(next.compare_record)(&next.current_record, &succ_record)
					!= Ordering::Greater
			);

			let item = source.current_record.take()
				.expect("current record is null");
			next.current_record = Rc::new(succ_record);
			source.current_record = Some(next.current_record.clone());
			self.sorter.push(next);

			let cur = Rc::try_unwrap(item).unwrap();
			self.discard_repetitions(&cur);

			Some(cur)
		}
		else
		{
			drop(next);
			// we don't push this source_index back onto self.sources
			let cur = source.current_record
				.take()
				.map(|item| Rc::try_unwrap(item).unwrap());
			self.discard_repetitions(&cur.as_ref().unwrap());
			cur
		}
	}
}

#[cfg(test)]
mod tests
{
	#[test]
	fn merge1()
	{
		let a = [1u32,2,3,4,5].iter().cloned();
		let b = [1,3,5,8,10].iter().cloned();
		let merged = crate::merge::Merge::new(vec![a,b], |a,b| a.cmp(b));
		let merged: Vec<_> = merged.collect();
		assert_eq!(merged, vec![1u32,2,3,4,5,8,10]);
	}

	#[test]
	fn merge_with_key()
	{
		let a = [1u32,2,3,4,5].iter().rev().cloned();
		let b = [1,3,5,8,10].iter().rev().cloned();
		let merged = crate::merge::Merge::new(vec![a,b], |a,b| a.cmp(b).reverse());
		let mut merged: Vec<_> = merged.collect();
		merged.reverse();
		assert_eq!(merged, vec![1u32,2,3,4,5,8,10]);
	}

	#[test] #[should_panic]
	fn merge_check_sorting()
	{
		let a = [1u32,2,3,4,5].iter().cloned();
		let b = [1,3,5,8,10].iter().cloned();
		let merged = crate::merge::Merge::new(vec![a,b], |a,b| a.cmp(b).reverse());
		let _: Vec<_> = merged.collect();
	}

	#[test]
	fn merge_str()
	{
		let a = ["a","a"].iter().cloned();
		let b = ["b","b"].iter().cloned();
		let mut merged = crate::merge::Merge::new(vec![a,b], |a,b| a.cmp(b));
		assert_eq!(merged.next().unwrap(), "a");
		assert_eq!(merged.next().unwrap(), "b");
		assert_eq!(merged.next(), None);
	}

}

