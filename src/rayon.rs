#[cfg(feature = "by-key")]
use crate::bykey::*;
use crate::Record;
use crate::*;
use ::rayon::iter::plumbing::*;
use ::rayon::prelude::*;

struct RecordProducer<'k> {
	reader: DatabaseRecordReader<'k>,
}

impl<'k> ParallelIterator for DatabaseRecordReader<'k> {
	type Item = Record;

	fn drive_unindexed<C>(self, consumer: C) -> C::Result
	where
		C: UnindexedConsumer<Self::Item>,
	{
		bridge_unindexed(RecordProducer { reader: self }, consumer)
	}
}

impl<'k> UnindexedProducer for RecordProducer<'k> {
	type Item = Record;

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
		folder.consume_iter(self.reader)
	}
}

#[cfg(feature = "by-key")]
struct KeyProducer<'k> {
	reader: DatabaseKeyReader<'k>,
}

#[cfg(feature = "by-key")]
impl<'k> ParallelIterator for DatabaseKeyReader<'k> {
	type Item = KeyRecordReader<'k>;

	fn drive_unindexed<C>(self, consumer: C) -> C::Result
	where
		C: UnindexedConsumer<Self::Item>,
	{
		bridge_unindexed(KeyProducer { reader: self }, consumer)
	}
}

#[cfg(feature = "by-key")]
impl<'k> UnindexedProducer for KeyProducer<'k> {
	type Item = KeyRecordReader<'k>;

	fn split(self) -> (KeyProducer<'k>, Option<KeyProducer<'k>>) {
		if let Some((first, second)) = self.reader.split() {
			(
				KeyProducer { reader: first },
				Some(KeyProducer { reader: second }),
			)
		} else {
			(
				KeyProducer {
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
		folder.consume_iter(self.reader)
	}
}
