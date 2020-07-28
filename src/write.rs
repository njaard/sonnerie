use byteorder::{ByteOrder, BigEndian, WriteBytesExt};
use std::io::Write;
use std::sync::Arc;
use parking_lot::{Condvar,Mutex};
use crossbeam::channel;

const SEGMENT_SIZE_GOAL: usize = 1024*128;
const SEGMENT_SIZE_EXTRA: usize = 1024*128 + 1024*32;

pub(crate) struct Writer<W: Write+Send+'static>
{
	writer_state: Option<Arc<Mutex<WriterState<W>>>>,
	last_key: String,
	last_format: String,
	first_segment_key: String,
	current_segment_data: Vec<u8>,
	current_key_data: Vec<u8>,
	current_key_record_len: usize,
	current_timestamp: [u8; 8],
	worker_threads: Option<channel::Sender<WorkerMessage>>,
	thread_handles: Vec<std::thread::JoinHandle<std::io::Result<()>>>,
	// a counter to keep each thread writing its output in the right order
	thread_ordering: usize,
}

struct WriterState<W: Write+Send>
{
	counter: usize,
	prev_size: u32,
	writer: W,
}

struct WorkerMessage
{
	counter: usize,
	header: Vec<u8>, // not to compress
	payload: Vec<u8>, // to compress
}

/// A reason a write could not be completed
#[derive(Debug)]
pub enum WriteFailure
{
	/// The key (`.0`) came lexicographically before key `.1`.
	OrderingViolation(String, String),
	/// The formats must must for a single key.
	HeterogeneousFormats(String, String, String),
	/// An IO error from the OS
	IOError(std::io::Error),
}

impl From<std::io::Error> for WriteFailure
{
	fn from(e: std::io::Error) -> Self
	{
		WriteFailure::IOError(e)
	}
}

impl<W: Write+Send> Writer<W>
{
	pub fn new(writer: W)
		-> Writer<W>
	{
		let num_worker_threads = 4usize;

		let writer_state =
			WriterState
			{
				counter: 0,
				prev_size: 0,
				writer,
			};

		let writer_state = Arc::new(Mutex::new(writer_state));
		let writer_notifier = Arc::new(Condvar::new());

		let mut thread_handles = Vec::with_capacity(num_worker_threads);

		let (send, recv) = channel::bounded(num_worker_threads*4);
		for _ in 0..num_worker_threads
		{
			let writer_state = writer_state.clone();
			let writer_notifier = writer_notifier.clone();
			let recv = recv.clone();
			let h = std::thread::spawn(
				move ||
					worker_thread(recv, &writer_state, &writer_notifier)
			);
			thread_handles.push(h);
		}


		Writer
		{
			writer_state: Some(writer_state),
			last_key: String::new(),
			last_format: String::new(),
			first_segment_key: String::new(),
			current_key_data: Vec::with_capacity(SEGMENT_SIZE_EXTRA),
			current_segment_data: Vec::with_capacity(SEGMENT_SIZE_EXTRA),
			current_key_record_len: 0,
			current_timestamp: [0; 8],
			worker_threads: Some(send),
			thread_handles,
			thread_ordering: 0,
		}
	}

	pub(crate) fn add_record(&mut self, key: &str, format: &str, data: &[u8])
		-> std::result::Result<(), WriteFailure>
	{
		// this is the first key ever seen
		if self.current_key_data.is_empty()
		{
			self.last_key.replace_range(.., key);
			self.last_format.replace_range(.., format);
			self.first_segment_key.replace_range(.., key);
			self.current_key_record_len = data.len();

			self.current_key_data.write_u32::<BigEndian>(key.len() as u32)
				.unwrap();
			self.current_key_data.write_u32::<BigEndian>(format.len() as u32)
				.unwrap();
			self.current_key_data.write_u32::<BigEndian>(self.current_key_record_len as u32)
				.unwrap();
			// key data length, filled in later
			self.current_key_data.write_u32::<BigEndian>(0)
				.unwrap();
			self.current_key_data.write_all(key.as_bytes()).unwrap();
			self.current_key_data.write_all(format.as_bytes()).unwrap();

			self.current_timestamp.copy_from_slice(&data[0..8]);
		}
		else
		{
			// we don't break keys into multiple segments
			if self.last_key != key
			{
				if key.as_bytes() < self.last_key.as_bytes()
				{
					return Err(WriteFailure::OrderingViolation(
						key.to_string(),
						self.last_key.clone(),
					));
				}

				// set key data length for previous key
				{
					let l = self.current_key_data.len() as u32
						- 16 - self.last_key.len() as u32
						- self.last_format.len() as u32;
					BigEndian::write_u32(&mut self.current_key_data[12..16], l);
				}
				// maybe flush the last segment, if it's full
				if ((self.current_segment_data.len()+self.current_key_data.len()) >>4)
					 >= SEGMENT_SIZE_GOAL
				{
					self.store_current_segment()?;
					self.first_segment_key.replace_range(.., key);
					assert_eq!(self.current_segment_data.len(), 0);
					std::mem::swap(&mut self.current_segment_data, &mut self.current_key_data);
				}
				else
				{
					self.current_segment_data.extend_from_slice(&self.current_key_data);
					self.current_key_data.clear();
				}
				self.last_key.replace_range(.., key);
				self.last_format.replace_range(.., format);
				self.current_key_record_len = data.len();
				self.current_key_data.write_u32::<BigEndian>(key.len() as u32)
					.unwrap();
				self.current_key_data.write_u32::<BigEndian>(format.len() as u32)
					.unwrap();
				self.current_key_data.write_u32::<BigEndian>(self.current_key_record_len as u32)
					.unwrap();
				self.current_key_data.write_u32::<BigEndian>(0)
					.unwrap();
				self.current_key_data.write_all(key.as_bytes()).unwrap();
				self.current_key_data.write_all(format.as_bytes()).unwrap();
			}
			else
			{
				if self.last_format != format
				{
					return Err(WriteFailure::HeterogeneousFormats(
						key.to_string(),
						self.last_format.to_string(),
						format.to_string(),
					));
				}
				if key.as_bytes() == self.last_key.as_bytes()
					 && data[0 .. 8] <= self.current_timestamp[..]
				{
					return Err(WriteFailure::OrderingViolation(
						key.to_string(),
						self.last_key.clone(),
					));
				}
			}
		}
		self.current_timestamp.copy_from_slice(&data[0..8]);


		assert_eq!(self.current_key_record_len, data.len());
		self.current_key_data.write_all(data).unwrap();
		Ok(())
	}

	/// send the current segment to a worker thread to get written
	pub(crate) fn store_current_segment(&mut self) -> std::io::Result<()>
	{
		let mut header = vec!();
		header.write_all(crate::segment::SEGMENT_INVOCATION)?;
		header.write_u32::<BigEndian>(self.first_segment_key.len() as u32)?;
		header.write_u32::<BigEndian>(self.last_key.len() as u32)?;
		header.write_u32::<BigEndian>(0u32)?; // compressed data size (filled by worker thread)
		header.write_u32::<BigEndian>(0u32)?; // prev_size (filled by worker thread)
		header.write_all(&self.first_segment_key.as_bytes())?;
		header.write_all(&self.last_key.as_bytes())?;

		let payload = std::mem::replace(
			&mut self.current_segment_data,
			Vec::with_capacity(SEGMENT_SIZE_EXTRA)
		);

		let message =
			WorkerMessage
			{
				counter: self.thread_ordering,
				header,
				payload,
			};
		self.thread_ordering += 1;

		self.worker_threads
			.as_ref().unwrap()
			.send(message).expect("failed to send data to worker");
		Ok(())
	}

	pub(crate) fn finish(mut self)
		-> std::io::Result<W>
	{
		self.fin()?;
		// destructure the entire writer_state to get
		// the tasty cream-filled `Write` inside
		let e = Arc::try_unwrap(
			self.writer_state
				.take()
				.expect("no writer_state???")
		);
		if let Ok(k) = e
		{
			Ok(k.into_inner().writer)
		}
		else
		{
			panic!("someone is still holding on on the writer_state");
		}
	}

	fn fin(&mut self)
		-> std::io::Result<()>
	{
		if !self.current_key_data.is_empty()
		{
			// set key data length for previous key
			{
				let l = self.current_key_data.len() as u32
					- 16 - self.last_key.len() as u32
					- self.last_format.len() as u32;
				BigEndian::write_u32(&mut self.current_key_data[12..16], l);
			}
			self.current_segment_data.extend_from_slice(&self.current_key_data);
			self.current_key_data.clear();
		}

		if !self.current_segment_data.is_empty()
		{
			self.store_current_segment()?;
		}

		self.worker_threads.take(); // close the Sender

		for th in self.thread_handles.drain(..)
			{ th.join().expect("thread can't be joined")?; }
		Ok(())
	}
}

impl<W: Write+Send> Drop for Writer<W>
{
	fn drop(&mut self)
	{
		self.fin().expect("failed to commit transaction");
	}
}

fn worker_thread<W: Write+Send>(
	recv: channel::Receiver<WorkerMessage>,
	writer_state: &Mutex<WriterState<W>>,
	writer_notifier: &Condvar,
) -> std::io::Result<()>
{
	for message in recv
	{
		let WorkerMessage { counter, mut header, payload }
			= message;

		let mut encoder = lz4::EncoderBuilder::new()
			.level(9)
			.build(vec!())
			.unwrap();
		encoder.write_all(&payload)?;
		let (compressed, e) = encoder.finish();
		e?;

		BigEndian::write_u32(&mut header[16+8 .. 16+8+4], compressed.len() as u32);

		let mut wl = writer_state.lock();
		while counter != wl.counter
		{
			writer_notifier.wait(&mut wl);
		}
		BigEndian::write_u32(&mut header[16+8+4 .. 16+8+8], wl.prev_size);

		wl.writer.write_all(&header)
			.expect("failed to write header data");
		wl.writer.write_all(&compressed)
			.expect("failed to write compressed data");
		wl.counter = counter+1;
		wl.prev_size = compressed.len() as u32+32;
		writer_notifier.notify_all();
	}
	Ok(())
}
