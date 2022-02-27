use crate::record::Record;
use crate::*;
use ::rayon::iter::plumbing::*;
use ::rayon::prelude::*;

struct RecordProducer<'k> {
	reader: DatabaseKeyReader<'k>,
}

impl<'k> ParallelIterator for DatabaseKeyReader<'k> {
	type Item = (usize, Record);

	fn drive_unindexed<C>(self, consumer: C) -> C::Result
	where
		C: UnindexedConsumer<Self::Item>,
	{
		bridge_unindexed(RecordProducer { reader: self }, consumer)
	}
}

impl<'k> UnindexedProducer for RecordProducer<'k> {
	type Item = (usize, Record);

	fn split(self) -> (RecordProducer<'k>, Option<RecordProducer<'k>>) {
		if let Some((first, second)) = self.reader.split() {
			(
				RecordProducer { reader: first },
				Some(RecordProducer { reader: second }),
			)
		} else {
			(
				RecordProducer {
					reader: self.reader,
				},
				None,
			)
		}
	}

	fn fold_with<F>(self, folder: F) -> F
	where
		F: Folder<Self::Item>,
	{
		folder.consume_iter(self.reader.into_iter())
	}
}
