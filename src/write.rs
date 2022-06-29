use antidote::{Condvar, Mutex};
use byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use crossbeam::channel;
use std::io::Write;
use std::sync::Arc;

pub(crate) const SEGMENT_SIZE_GOAL: usize = 1024 * 1024;
const SEGMENT_SIZE_EXTRA: usize = 1024 * 1024 + 1024 * 32;

pub(crate) struct Writer<W: Write + Send + 'static> {
	writer_state: Option<Arc<Mutex<WriterState<W>>>>,
	/// the last key that was added
	last_key: String,
	/// the format string that was previously added
	last_format: String,
	/// the first key currently stored in `current_segment_data`
	first_segment_key: String,
	/// the last key currently stored in `current_segment_data` (updated when `last_segment_key()` is called)
	last_segment_key: String,
	/// the data for the segment; data for the current key doesn't get put here until it's finished, so we want large
	/// keys to have a chance to overflow into a new segment
	current_segment_data: Vec<u8>,
	/// data for the current key (`last_key`) that hasn't been flushed into a segment yet
	current_key_data: Vec<u8>,
	/// the most recent timestamp (used for ensuring ordering)
	current_timestamp: crate::Timestamp,
	/// Used for verifying that the records comply with their format, if None, then they are variable (string) sized
	current_record_size: Option<usize>,
	/// these threads actually do the LZ4-ing
	worker_threads: Option<channel::Sender<WorkerMessage>>,
	thread_handles: Vec<std::thread::JoinHandle<std::io::Result<()>>>,
	/// a counter to keep each thread writing its output in the right order
	thread_ordering: usize,
}

struct WriterState<W: Write + Send> {
	counter: usize,
	prev_size: u32,
	writer: W,
	stored_size_last_key: u32,
	last_key: Vec<u8>,
}

struct Header {
	first_key: Vec<u8>,
	last_key: Vec<u8>,
}

struct WorkerMessage {
	counter: usize,
	header: Header,   // not to compress
	payload: Vec<u8>, // to compress
}

/// A reason a write could not be completed
#[derive(Debug)]
pub enum WriteFailure {
	/// The key `second` does not come lexicographically after `first`, but they were added in that order
	KeyOrderingViolation{ first: String, second: String },
	/// The timestamp `second` does not come chronologically after `first`, but they were added in that order, in the same key (`key`)
	TimeOrderingViolation{ first: chrono::NaiveDateTime, second: chrono::NaiveDateTime, key: String },
	/// The size of data was not expected
	IncorrectLength(usize),
	/// An IO error from the OS
	IOError(std::io::Error),
}

impl From<std::io::Error> for WriteFailure {
	fn from(e: std::io::Error) -> Self {
		WriteFailure::IOError(e)
	}
}

impl<W: Write + Send> Writer<W> {
	fn new_internal(writer: W, disable_compression: bool) -> Writer<W> {
		let num_worker_threads = 4usize;

		let writer_state = WriterState {
			counter: 0,
			prev_size: 0,
			writer,
			stored_size_last_key: 0,
			last_key: vec![],
		};

		let writer_state = Arc::new(Mutex::new(writer_state));
		let writer_notifier = Arc::new(Condvar::new());

		let mut thread_handles = Vec::with_capacity(num_worker_threads);

		let (send, recv) = channel::bounded(num_worker_threads * 4);
		for _ in 0..num_worker_threads {
			let writer_state = writer_state.clone();
			let writer_notifier = writer_notifier.clone();
			let recv = recv.clone();
			let h = std::thread::spawn(move || {
				worker_thread(recv, &writer_state, &writer_notifier, disable_compression)
			});
			thread_handles.push(h);
		}

		Writer {
			writer_state: Some(writer_state),
			last_key: String::new(),
			last_format: String::new(),
			first_segment_key: String::new(),
			last_segment_key: String::new(),
			current_key_data: Vec::with_capacity(SEGMENT_SIZE_EXTRA),
			current_segment_data: Vec::with_capacity(SEGMENT_SIZE_EXTRA),
			current_timestamp: 0,
			worker_threads: Some(send),
			thread_handles,
			thread_ordering: 0,
			current_record_size: None,
		}
	}
	pub fn new(writer: W) -> Writer<W> {
		Self::new_internal(writer, false)
	}

	fn new_key_begin(&mut self, key: &str, format: &str) {
		self.last_key.replace_range(.., key);
		self.last_format.replace_range(.., format);

		self.current_key_data
			.write_u32::<BigEndian>(key.len() as u32)
			.unwrap();
		self.current_key_data
			.write_u32::<BigEndian>(format.len() as u32)
			.unwrap();
		// key data length, filled in later
		self.current_key_data.write_u32::<BigEndian>(0).unwrap();
		self.current_key_data.write_all(key.as_bytes()).unwrap();
		self.current_key_data.write_all(format.as_bytes()).unwrap();

		self.current_record_size =
			crate::row_format::row_format_size(format).map(|m| m + crate::TIMESTAMP_SIZE);
	}

	/// copy the data for the current key into `current_segment_data`
	fn flush_current_key(&mut self) {
		if !self.current_key_data.is_empty() {
			// fill in key data length
			let l = self.current_key_data.len() as u32
				- 12 - self.last_key.len() as u32
				- self.last_format.len() as u32;
			BigEndian::write_u32(&mut self.current_key_data[8..12], l);
			self.current_segment_data
				.extend_from_slice(&self.current_key_data);
			self.current_key_data.clear();
		}
		self.last_segment_key = self.last_key.clone();
	}

	pub(crate) fn add_record_base(
		&mut self,
		key: &str,
		timestamp: crate::Timestamp,
		format: &str,
		serialize_values: impl FnOnce(&mut Vec<u8>),
	) -> std::result::Result<(), WriteFailure> {
		if self.current_key_data.is_empty() {
			// this is the first key ever seen
			self.new_key_begin(key, format);
			self.first_segment_key.replace_range(.., key);
		} else {
			if key.as_bytes() < self.last_key.as_bytes() {
				return Err(WriteFailure::KeyOrderingViolation{
					second: key.to_string(),
					first: self.last_key.clone(),
				});
			}

			if key.as_bytes() == self.last_key.as_bytes() && timestamp <= self.current_timestamp {
				let first = chrono::NaiveDateTime::from_timestamp(
					(self.current_timestamp / 1_000_000_000) as i64,
					(self.current_timestamp % 1_000_000_000) as u32,
				);
				let second = chrono::NaiveDateTime::from_timestamp(
					(timestamp / 1_000_000_000) as i64,
					(timestamp % 1_000_000_000) as u32,
				);
				return Err(WriteFailure::TimeOrderingViolation{
					key: key.to_string(),
					first,
					second,
				});
			}

			if key != self.last_key || format != self.last_format {
				self.flush_current_key();
				self.new_key_begin(key, format);
			}

			if self.current_segment_data.len() + self.current_key_data.len() >= SEGMENT_SIZE_GOAL
				&& !self.current_segment_data.is_empty()
			{
				self.store_current_segment()?;
				self.first_segment_key.replace_range(.., key);
			}
		}

		self.current_timestamp = timestamp;
		serialize_values(&mut self.current_key_data);

		Ok(())
	}

	pub(crate) fn add_record(
		&mut self,
		key: &str,
		timestamp: crate::Timestamp,
		values: impl crate::RecordBuilder,
	) -> std::result::Result<(), WriteFailure> {
		let mut fmt = compact_str::CompactString::default();
		values.format_str(&mut fmt);

		let expected_size = values.size();
		let variable_size = values.variable_size();

		self.add_record_base(
			key,
			timestamp,
			&fmt,
			|buf|
			{
				if variable_size
				{
					let mut lenbuf = unsigned_varint::encode::usize_buffer();
					let o = unsigned_varint::encode::usize(expected_size, &mut lenbuf);
					buf.write_all(&o).unwrap();
				}

				let before_len = buf.len();

				buf.write_u64::<BigEndian>(timestamp).unwrap();
				values.store(buf);

				if before_len + expected_size + 8 != buf.len() {
					panic!("ToRecord didn't produce data of a valid size (this is a bug, report it): expected={expected_size}, actual={}", buf.len()-before_len);
				}
			}
		)
	}

	pub(crate) fn add_record_raw(
		&mut self,
		key: &str,
		format: &str,
		data: &[u8],
	) -> std::result::Result<(), WriteFailure> {
		let timestamp = BigEndian::read_u64(&data[0..8]);
		let constant_size =
			crate::row_format::row_format_size(format).map(|m| m + crate::TIMESTAMP_SIZE);

		let mut lenbuf = unsigned_varint::encode::usize_buffer();
		let var_len = if let Some(sz) = constant_size {
			if data.len() != sz {
				return Err(WriteFailure::IncorrectLength(sz));
			}
			&[]
		} else {
			// subtract 8, for the timestamp
			unsigned_varint::encode::usize(data.len() - 8, &mut lenbuf)
		};

		self.add_record_base(key, timestamp, format, |buf| {
			buf.write_all(&var_len).unwrap();
			buf.write_all(&data).unwrap();
		})
	}

	/// send the current segment to a worker thread to get written
	pub fn store_current_segment(&mut self) -> std::io::Result<()> {
		let header = Header {
			first_key: self.first_segment_key.as_bytes().to_owned(),
			last_key: self.last_segment_key.as_bytes().to_owned(),
		};

		let payload = std::mem::replace(
			&mut self.current_segment_data,
			Vec::with_capacity(SEGMENT_SIZE_EXTRA),
		);

		let message = WorkerMessage {
			counter: self.thread_ordering,
			header,
			payload,
		};
		self.thread_ordering += 1;

		self.worker_threads
			.as_ref()
			.unwrap()
			.send(message)
			.expect("failed to send data to worker");
		self.current_segment_data.clear();
		Ok(())
	}

	pub(crate) fn finish(mut self) -> std::io::Result<W> {
		self.fin()?;
		// destructure the entire writer_state to get
		// the tasty cream-filled `Write` inside
		let e = Arc::try_unwrap(self.writer_state.take().expect("no writer_state???"));
		if let Ok(k) = e {
			Ok(k.into_inner().writer)
		} else {
			panic!("someone is still holding on on the writer_state");
		}
	}

	fn fin(&mut self) -> std::io::Result<()> {
		// only have a key span segments if it's REALLY necessary
		if !self.current_key_data.is_empty() {
			self.flush_current_key();
		}

		if !self.current_segment_data.is_empty() {
			self.store_current_segment()?;
		}

		self.worker_threads.take(); // close the Sender

		for th in self.thread_handles.drain(..) {
			th.join().expect("thread can't be joined")?;
		}
		Ok(())
	}
}

impl<W: Write + Send> Drop for Writer<W> {
	fn drop(&mut self) {
		self.fin().expect("failed to commit transaction");
	}
}

fn worker_thread<W: Write + Send>(
	recv: channel::Receiver<WorkerMessage>,
	writer_state: &Mutex<WriterState<W>>,
	writer_notifier: &Condvar,
	disable_compression: bool,
) -> std::io::Result<()> {
	for message in recv {
		let WorkerMessage {
			counter,
			header,
			payload,
		} = message;

		let compressed;

		if disable_compression {
			compressed = payload;
		} else {
			let mut encoder = lz4::EncoderBuilder::new().level(9).build(vec![]).unwrap();
			encoder.write_all(&payload)?;
			let (c, e) = encoder.finish();
			e?;
			compressed = c;
		}

		let mut segmented: smallvec::SmallVec<[_; 4]> = smallvec::smallvec![];
		{
			let mut start = 0;
			while let Some(pos) = crate::segment::find_segment_invocation(&compressed[start..]) {
				segmented.push(&compressed[start..pos + start]);
				segmented.push(crate::segment::ESCAPE_SEGMENT_INVOCATION);
				start = start + pos + crate::segment::SEGMENT_INVOCATION.len();
			}
			segmented.push(&compressed[start..]);
		}

		let mut wl = writer_state.lock();
		while counter != wl.counter {
			wl = writer_notifier.wait(wl);
		}

		fn wv(vec: &mut impl Write, data: u32) -> std::io::Result<()> {
			let mut buf = unsigned_varint::encode::u32_buffer();
			let o = unsigned_varint::encode::u32(data, &mut buf);
			vec.write_all(o)
		}

		let this_key_prev;
		if wl.last_key == header.first_key {
			this_key_prev = wl.stored_size_last_key;
		} else {
			this_key_prev = 0;
			wl.stored_size_last_key = 0;
		}

		let wrote_size;
		{
			let ps = wl.prev_size;
			let mut bc = WriteCounter::new(&mut wl.writer);

			bc.write_all(crate::segment::SEGMENT_INVOCATION)?;
			bc.write_u16::<BigEndian>(0x0100)?;

			let ee = |e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e);

			wv(&mut bc, header.first_key.len().try_into().map_err(ee)?)?;
			wv(&mut bc, header.last_key.len().try_into().map_err(ee)?)?;
			wv(&mut bc, compressed.len().try_into().map_err(ee)?)?;
			wv(&mut bc, ps)?;
			wv(&mut bc, this_key_prev)?;

			bc.write_all(&header.first_key)?;
			bc.write_all(&header.last_key)?;

			for segment in segmented {
				bc.write_all(segment)
					.expect("failed to write compressed data");
			}
			wrote_size = bc.count().try_into().map_err(ee)?;
		}
		if header.last_key == header.first_key {
			wl.stored_size_last_key += wrote_size;
		} else {
			wl.stored_size_last_key = wrote_size;
		}
		wl.last_key = header.last_key;
		wl.counter = counter + 1;
		wl.prev_size = wrote_size;
		writer_notifier.notify_all();
	}
	Ok(())
}

/// counts bytes written to a Write
struct WriteCounter<W: Write> {
	count: usize,
	inner: W,
}

impl<W: Write> WriteCounter<W> {
	fn new(inner: W) -> Self {
		Self { count: 0, inner }
	}
	fn count(&self) -> usize {
		self.count
	}
}

impl<W: Write> Write for WriteCounter<W> {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		self.count += buf.len();
		self.inner.write(buf)
	}
	fn flush(&mut self) -> std::io::Result<()> {
		self.inner.flush()
	}
}

#[test]
fn near_boundary() {
	// when a segment is about to overflow, no portion of the overflowing key should appear in it
	// (all of it should go in the successive segment)
	let q = "qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq";
	let mut w = Writer::new_internal(vec![], true);
	w.current_key_data = vec![0x42u8; SEGMENT_SIZE_GOAL - 40];
	w.first_segment_key = "a".to_string();
	w.last_segment_key = "a".to_string();
	w.add_record_raw(q, "f", b"012345671234").unwrap();
	w.add_record_raw("r", "f", b"012345671234").unwrap();
	let v = w.finish().unwrap();
	assert_eq!(memchr::memmem::find_iter(&v, q).count(), 2);
}
