use core::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

struct NextRecord<Source, Record>
where
	Source: Iterator<Item = Record>,
{
	source: Source,
	source_index: usize,
	tx_id: usize,
	current_record: Option<Record>,
	compare_record: Arc<Box<dyn Fn(&Record, &Record) -> Ordering + Send + Sync>>,
}

impl<Source: Iterator<Item = Record>, Record> Ord for NextRecord<Source, Record> {
	fn cmp(&self, other: &Self) -> Ordering {
		(self.compare_record)(
			self.current_record.as_ref().unwrap(),
			other.current_record.as_ref().unwrap(),
		)
		.reverse()
		.then_with(|| self.source_index.cmp(&other.source_index))
	}
}

impl<Source: Iterator<Item = Record>, Record> PartialOrd for NextRecord<Source, Record> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl<Source: Iterator<Item = Record>, Record> PartialEq for NextRecord<Source, Record> {
	fn eq(&self, other: &Self) -> bool {
		(self.compare_record)(
			self.current_record.as_ref().unwrap(),
			other.current_record.as_ref().unwrap(),
		) == Ordering::Equal
			&& (other.source_index == self.source_index)
	}
}

impl<Source: Iterator<Item = Record>, Record> Eq for NextRecord<Source, Record> {}

/// merge various iterators into the lowest value,
/// choosing the last item as a tie-breaker
pub struct Merge<Source, Record>
where
	Source: Iterator<Item = Record>,
{
	sorter: BinaryHeap<NextRecord<Source, Record>>,
	most_recent: Option<NextRecord<Source, Record>>,
}

impl<Source, Record> Merge<Source, Record>
where
	Source: Iterator<Item = Record>,
	Record: std::fmt::Debug,
{
	pub fn new<CompareRecord>(
		orig_sources: Vec<(usize, Source)>,
		compare_record: CompareRecord,
	) -> Self
	where
		CompareRecord: Fn(&Record, &Record) -> Ordering + 'static + Send + Sync,
	{
		let compare_record: Box<dyn Fn(&Record, &Record) -> Ordering + Send + Sync> =
			Box::new(compare_record);
		let compare_record = Arc::new(compare_record);

		let mut sorter = BinaryHeap::with_capacity(orig_sources.len());

		for (idx, (tx_id, mut src)) in orig_sources.into_iter().enumerate() {
			if let Some(rec) = src.next() {
				sorter.push(NextRecord {
					source: src,
					source_index: idx,
					tx_id,
					current_record: Some(rec),
					compare_record: compare_record.clone(),
				});
			}
		}

		Self {
			sorter,
			most_recent: None,
		}
	}

	// continue to read next items until the next item read
	// won't match `current`.
	// Must be called while `current`'s source is not in the heap
	fn discard_repetitions(&mut self, current: &Record) {
		loop {
			{
				let next = self.sorter.peek();
				if next.is_none() {
					break;
				}
				let next = next.unwrap();

				match (next.compare_record)(current, next.current_record.as_ref().unwrap()) {
					Ordering::Less => {
						break;
					} // done
					Ordering::Greater => panic!("ordering violation"),
					Ordering::Equal => {} // consume `next`
				}
			}

			let mut best = self.sorter.pop().unwrap();
			best.current_record = None; // drop current_record before asking for the next one

			let succ_record = best.source.next();
			if let Some(succ_record) = succ_record {
				best.current_record = Some(succ_record);
				self.sorter.push(best);
			} else {
				// `best` doesn't get put on the heap if it has no `following value`
			}
		}
	}
}

impl<Source, Record> Iterator for Merge<Source, Record>
where
	Source: Iterator<Item = Record>,
	Record: std::fmt::Debug,
{
	type Item = (usize, Record);

	fn next(&mut self) -> Option<Self::Item> {
		// refill the most recent one
		if let Some(mut most_recent) = self.most_recent.take() {
			let tx_id = most_recent.tx_id;
			if let Some(current) = most_recent.source.next() {
				// we short-circuit putting `current` on the heap again by testing the current top of the heap

				if let Some(next) = self.sorter.peek() {
					match (most_recent.compare_record)(
						&current,
						next.current_record.as_ref().unwrap(),
					) {
						Ordering::Equal
							if most_recent.source_index.cmp(&next.source_index).is_lt() =>
						{
							// Not the latest version of this record
						}
						Ordering::Less => {
							// short circuit completed
							self.most_recent = Some(most_recent);
							return Some((tx_id, current));
						} // done
						Ordering::Greater => {}
						Ordering::Equal => self.discard_repetitions(&current),
					}
				} else {
					// short circuit completed
					self.most_recent = Some(most_recent);
					return Some((tx_id, current));
				}

				most_recent.current_record = Some(current);
				self.sorter.push(most_recent);
			}
		}

		let mut best = self.sorter.pop()?;
		let tx_id = best.tx_id;

		let item = best.current_record.take().expect("current record is null");

		self.discard_repetitions(&item);

		self.most_recent = Some(best);

		Some((tx_id, item))
	}
}

#[cfg(test)]
mod tests {
	use std::rc::Rc;
	#[test]
	fn merge1() {
		let a = [1u32, 2, 3, 4, 5].iter().cloned();
		let b = [1, 3, 5, 8, 10].iter().cloned();
		let merged = crate::merge::Merge::new(vec![(0, a), (1, b)], |a, b| a.cmp(b));
		let merged: Vec<_> = merged.map(|(_, x)| x).collect();
		assert_eq!(merged, vec![1u32, 2, 3, 4, 5, 8, 10]);
	}

	#[test]
	fn merge_with_key() {
		let a = [1u32, 2, 3, 4, 5].iter().rev().cloned();
		let b = [1, 3, 5, 8, 10].iter().rev().cloned();
		let merged = crate::merge::Merge::new(vec![(0, a), (1, b)], |a, b| a.cmp(b).reverse());
		let mut merged: Vec<_> = merged.map(|(_, x)| x).collect();
		merged.reverse();
		assert_eq!(merged, vec![1u32, 2, 3, 4, 5, 8, 10]);
	}

	#[test]
	#[should_panic]
	fn merge_check_sorting() {
		let a = [1u32, 2, 3, 4, 5].iter().cloned();
		let b = [1, 3, 5, 8, 10].iter().cloned();
		let merged = crate::merge::Merge::new(vec![(0, a), (1, b)], |a, b| a.cmp(b).reverse());
		let _: Vec<_> = merged.map(|(_, x)| x).collect();
	}

	#[test]
	fn merge_str() {
		let a = ["a", "b"].iter().cloned();
		let b = ["a", "c"].iter().cloned();
		let mut merged =
			crate::merge::Merge::new(vec![(0, a), (1, b)], |a, b| a.cmp(b)).map(|(_, x)| x);
		assert_eq!(merged.next().unwrap(), "a");
		assert_eq!(merged.next().unwrap(), "b");
		assert_eq!(merged.next().unwrap(), "c");
		assert_eq!(merged.next(), None);
	}
	#[test]
	fn merge_last_reader() {
		let a = [("b", 1), ("c", 1), ("d", 1), ("e", 1)].iter().cloned();
		let b = [("c", 2)].iter().cloned();
		let mut merged =
			crate::merge::Merge::new(vec![(0, a), (1, b)], |a, b| a.0.cmp(&b.0)).map(|(_, x)| x);
		//assert_eq!(merged.next().unwrap(), ("a",2));
		assert_eq!(merged.next().unwrap(), ("b", 1));
		assert_eq!(merged.next().unwrap(), ("c", 2));
		assert_eq!(merged.next().unwrap(), ("d", 1));
		assert_eq!(merged.next().unwrap(), ("e", 1));
		assert_eq!(merged.next(), None);
	}
	#[test]
	fn merge_count_owns() {
		let first = Rc::new(0);
		let a = vec![first.clone(), Rc::new(1)];
		let mut merged =
			crate::merge::Merge::new(vec![(0, a.into_iter())], |a, b| a.cmp(b)).map(|(_, x)| x);
		assert_eq!(Rc::strong_count(&first), 2);
		let m = merged.next().unwrap();
		assert_eq!(Rc::strong_count(&m), 2);
		eprintln!("{}", m);
		assert_eq!(Rc::strong_count(&merged.next().unwrap()), 1);
		assert_eq!(Rc::strong_count(&first), 2);
		assert_eq!(merged.next(), None);
	}
}
