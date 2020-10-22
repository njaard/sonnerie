use byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use crossbeam::channel;
use parking_lot::{Condvar, Mutex};
use std::io::Write;
use std::sync::Arc;

const SEGMENT_SIZE_GOAL: usize = 1024 * 128;
const SEGMENT_SIZE_EXTRA: usize = 1024 * 128 + 1024 * 32;

pub(crate) struct Writer<W: Write + Send + 'static> {
	writer_state: Option<Arc<Mutex<WriterState<W>>>>,
	last_key: String,
	last_format: String,
	first_segment_key: String,
	last_segment_key: String,
	current_segment_data: Vec<u8>,
	current_key_data: Vec<u8>,
	current_timestamp: [u8; 8],
	worker_threads: Option<channel::Sender<WorkerMessage>>,
	current_record_size: Option<usize>,
	thread_handles: Vec<std::thread::JoinHandle<std::io::Result<()>>>,
	// a counter to keep each thread writing its output in the right order
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
	/// The key (`.0`) came lexicographically before key `.1`.
	OrderingViolation(String, String),
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
	pub fn new(writer: W) -> Writer<W> {
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
			let h =
				std::thread::spawn(move || worker_thread(recv, &writer_state, &writer_notifier));
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
			current_timestamp: [0; 8],
			worker_threads: Some(send),
			thread_handles,
			thread_ordering: 0,
			current_record_size: None,
		}
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
			crate::row_format::row_format_size(format).map(|m| m + crate::record::TIMESTAMP_SIZE);
	}

	fn flush_current_key(&mut self) {
		if !self.current_key_data.is_empty() {
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

	pub(crate) fn add_record(
		&mut self,
		key: &str,
		format: &str,
		data: &[u8],
	) -> std::result::Result<(), WriteFailure> {
		// this is the first key ever seen
		if self.current_key_data.is_empty() {
			self.new_key_begin(key, format);
			self.first_segment_key.replace_range(.., key);
			if let Some(sz) = self.current_record_size {
				if data.len() != sz {
					return Err(WriteFailure::IncorrectLength(sz));
				}
			}
		} else {
			if key.as_bytes() < self.last_key.as_bytes() {
				return Err(WriteFailure::OrderingViolation(
					key.to_string(),
					self.last_key.clone(),
				));
			}

			if key.as_bytes() == self.last_key.as_bytes()
				&& data[0..8] <= self.current_timestamp[..]
			{
				return Err(WriteFailure::OrderingViolation(
					key.to_string(),
					self.last_key.clone(),
				));
			}

			if key != self.last_key || format != self.last_format {
				self.flush_current_key();
				self.new_key_begin(key, format);
			}
			if let Some(sz) = self.current_record_size {
				if data.len() != sz {
					return Err(WriteFailure::IncorrectLength(sz));
				}
			}

			if self.current_segment_data.len() + self.current_key_data.len() >= SEGMENT_SIZE_GOAL {
				// only have a key span segments if it's REALLY necessary
				if self.current_key_data.len() > 128 {
					self.flush_current_key();
					self.new_key_begin(key, format);
				}

				// the segment is full, flush it
				self.store_current_segment()?;
				self.first_segment_key.replace_range(.., key);
			}
		}

		self.current_timestamp.copy_from_slice(&data[0..8]);

		if self.current_record_size.is_none() {
			let mut buf = unsigned_varint::encode::u32_buffer();
			// subtract 8, for the timestamp
			let o = unsigned_varint::encode::u32(data.len() as u32 - 8, &mut buf);
			self.current_key_data.write_all(&o).unwrap();
		}

		self.current_key_data.write_all(data).unwrap();
		Ok(())
	}

	/// send the current segment to a worker thread to get written
	pub(crate) fn store_current_segment(&mut self) -> std::io::Result<()> {
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
) -> std::io::Result<()> {
	for message in recv {
		let WorkerMessage {
			counter,
			header,
			payload,
		} = message;

		let mut encoder = lz4::EncoderBuilder::new().level(9).build(vec![]).unwrap();
		encoder.write_all(&payload)?;
		let (compressed, e) = encoder.finish();
		e?;

		let mut segmented: smallvec::SmallVec<[_; 4]> = smallvec::smallvec![];
		{
			let mut start = 0;
			while let Some(pos) =
				twoway::find_bytes(&compressed[start..], crate::segment::SEGMENT_INVOCATION)
			{
				segmented.push(&compressed[start..pos + start]);
				segmented.push(crate::segment::ESCAPE_SEGMENT_INVOCATION);
				start = start + pos + crate::segment::SEGMENT_INVOCATION.len();
			}
			segmented.push(&compressed[start..]);
		}

		let mut wl = writer_state.lock();
		while counter != wl.counter {
			writer_notifier.wait(&mut wl);
		}

		fn wv(vec: &mut impl Write, data: u32) -> std::io::Result<()> {
			let mut buf = unsigned_varint::encode::u32_buffer();
			let o = unsigned_varint::encode::u32(data, &mut buf);
			vec.write_all(&o)
		};

		let this_key_prev;
		if wl.last_key == header.first_key {
			this_key_prev = wl.stored_size_last_key;
		} else {
			this_key_prev = 0;
			wl.stored_size_last_key = 0;
		}

		let wrote_size;
		{
			use std::convert::TryInto;
			let ps = wl.prev_size;
			let mut bc = WriteCounter::new(&mut wl.writer);

			bc.write_all(crate::segment::SEGMENT_INVOCATION)?;
			bc.write_u16::<BigEndian>(0x0100)?;

			let ee = |e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e);

			wv(
				&mut bc,
				header.first_key.len().try_into().map_err(|e| ee(e))?,
			)?;
			wv(
				&mut bc,
				header.last_key.len().try_into().map_err(|e| ee(e))?,
			)?;
			wv(&mut bc, compressed.len().try_into().map_err(|e| ee(e))?)?;
			wv(&mut bc, ps)?;
			wv(&mut bc, this_key_prev)?;

			bc.write_all(&header.first_key)?;
			bc.write_all(&header.last_key)?;

			for segment in segmented {
				bc.write_all(&segment)
					.expect("failed to write compressed data");
			}
			wrote_size = bc.count().try_into().map_err(|e| ee(e))?;
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
