//! This is a simple client API for Sonnerie, a timeseries database.
//!
//! It lets you do a variety of insertions and reads.
//!
//! # Example
//!
//! ```no_run
//! extern crate sonnerie_api;
//! fn main() -> std::io::Result<()>
//! {
//!     let stream = std::net::TcpStream::connect("localhost:5599")?;
//!     let mut client = sonnerie_api::Client::new(stream)?;
//!     // read a series (a read transaction is automatically created and closed)
//!     let _: Vec<(sonnerie_api::NaiveDateTime,f64)> =
//!         client.read_series("fibonacci")?;
//!     // start a write transaction
//!     client.begin_write()?;
//!     client.add_value(
//!         "fibonacci",
//!         &"2018-01-06T00:00:00".parse().unwrap(),
//!         13.0,
//!     )?;
//!     // save the transaction
//!     client.commit()?;
//!     Ok(())
//! }
//! ```

extern crate chrono;
extern crate escape_string;

use std::io::{BufReader,BufWriter,BufRead,Write,Read};
use std::io::{Result, ErrorKind, Error};
use std::fmt;

const NANO: u64 = 1_000_000_000;

use escape_string::{escape,split_one};

use std::cell::{Cell,RefCell};

mod types;
use types::ToValue;


/// Error for when client could not understand the server
pub struct ProtocolError
{
	remote_err: String,
}

impl ProtocolError
{
	fn new(e: String) -> ProtocolError
	{
		ProtocolError
		{
			remote_err: e
		}
	}
}

impl std::error::Error for ProtocolError
{ }

impl std::fmt::Display for ProtocolError
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
	{
		write!(f, "sonnerie remote: {}", self.remote_err)
	}
}
impl std::fmt::Debug for ProtocolError
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
	{
		write!(f, "sonnerie remote: {}", self.remote_err)
	}
}


pub use chrono::NaiveDateTime;

/// Sonnerie Client API
pub struct Client
{
	writer: RefCell<Box<Write>>,
	reader: RefCell<Box<BufRead>>,
	in_tx: Cell<bool>,
	writing: Cell<bool>,
}

struct TransactionLock<'c>
{
	c: &'c Client,
	need_rollback: bool,
}

impl<'c> TransactionLock<'c>
{
	fn read(c: &'c Client)
		-> Result<TransactionLock<'c>>
	{
		if !c.in_tx.get()
			{ c.begin_read()?; }
		Ok(TransactionLock
		{
			c: c,
			need_rollback: !c.in_tx.get(),
		})
	}
}

impl<'c> Drop for TransactionLock<'c>
{
	fn drop(&mut self)
	{
		if self.need_rollback
		{
			let mut w = self.c.writer.borrow_mut();
			let _ = writeln!(&mut w,"rollback");
			let _ = w.flush();
			let mut error = String::new();
			let _ = self.c.reader.borrow_mut().read_line(&mut error);
		}
	}
}

impl Client
{
	/// Create a Sonnerie client from a reader/writer stream.
	///
	/// This is useful if you want to connect to Sonnerie
	/// via a Unix Domain Socket tunnelled through SSH.
	///
	/// Failure may be caused by Sonnerie not sending its protocol "Hello"
	/// on connection.
	pub fn from_streams<R: 'static + Read, W: 'static + Write>(
		reader: R, writer: W
	) -> Result<Client>
	{
		let mut reader = BufReader::new(reader);
		let writer = BufWriter::new(writer);

		let mut intro = String::new();
		reader.read_line(&mut intro)?;
		if intro != "Greetings from Sonnerie\n"
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				Box::new(ProtocolError::new(intro)),
			));
		}

		Ok(
			Client
			{
				writer: RefCell::new(Box::new(writer)),
				reader: RefCell::new(Box::new(reader)),
				in_tx: Cell::new(false),
				writing: Cell::new(false),
			}
		)
	}

	/// Use a specific TCP connection to make a connection.
	pub fn new(connection: std::net::TcpStream)
		-> Result<Client>
	{
		Self::from_streams(
			connection.try_clone()?,
			connection
		)
	}

	/// Start a read transaction.
	///
	/// End the transaction with [`commit()`](#method.commit)
	/// or [`rollback()`](#method.rollback), which
	/// are both the same for a read transaction.
	///
	/// Read-only functions will automatically close and open
	/// a transaction, but calling this function allows you to not
	/// see changes made over the life if your transaction.
	///
	/// Transactions may not be nested.
	pub fn begin_read(&self)
		-> Result<()>
	{
		assert!(!self.in_tx.get());

		let mut w = self.writer.borrow_mut();
		let mut r = self.reader.borrow_mut();
		writeln!(&mut w, "begin read")?;
		w.flush()?;
		let mut error = String::new();
		r.read_line(&mut error)?;
		check_error(&mut error)?;
		self.in_tx.set(true);
		self.writing.set(true);

		Ok(())
	}

	/// Create a writing transaction.
	///
	/// You must call this function before any calling any
	/// write functions. Write transactions are not made
	/// automatiicaly, to prevent you from accidentally making many
	/// small transactions, which are relatively slow.
	///
	/// You must call [`commit()`](#method.commit) for the transactions to be saved.
	/// You may also explicitly call [`rollback()`](#method.rollback) to discard your changes.
	///
	/// Transactions may not be nested.
	pub fn begin_write(&self)
		-> Result<()>
	{
		assert!(!self.in_tx.get());

		let mut w = self.writer.borrow_mut();
		let mut r = self.reader.borrow_mut();
		writeln!(&mut w, "begin write")?;
		w.flush()?;
		let mut error = String::new();
		r.read_line(&mut error)?;
		check_error(&mut error)?;
		self.in_tx.set(true);
		self.writing.set(true);

		Ok(())
	}

	/// Read all the values in a specific series.
	///
	/// Fails if the series does not exist, but returns an empty
	/// Vec if the series does exist and is simply empty.
	pub fn read_series(
		&mut self,
		name: &str,
	) -> Result<Vec<(NaiveDateTime, f64)>>
	{
		let from = NaiveDateTime::from_timestamp(0,0);
		let to = max_time();
		self.read_series_range(name, &from, &to)
	}

	/// Read values within a range of timestamps in a specific series.
	///
	/// Fails if the series does not exist, but returns an empty
	/// Vec if no samples were contained in that range.
	///
	/// * `first_time` is the first timestamp to begin reading from
	/// * `last_time` is the last timestamp to read (inclusive)
	pub fn read_series_range(
		&mut self,
		name: &str,
		first_time: &NaiveDateTime,
		last_time: &NaiveDateTime,
	) -> Result<Vec<(NaiveDateTime, f64)>>
	{
		let _maybe = TransactionLock::read(self)?;

		let mut w = self.writer.borrow_mut();
		let mut r = self.reader.borrow_mut();
		writeln!(
			&mut w,
			"read {} {} {}",
			escape(name),
			format_time(first_time),
			format_time(last_time),
		)?;
		w.flush()?;
		let mut res = vec!();
		let mut out = String::new();
		loop
		{
			out.clear();
			r.read_line(&mut out)?;
			check_error(&mut out)?;

			let out = out.trim_right();
			if out.len() == 0 { break; }

			let space = out.find('\t')
				.ok_or_else(
					|| Error::new(
						ErrorKind::InvalidData,
						ProtocolError::new(out.to_string()),
					)
				)?;

			let ts = parse_time(&out[ 0 .. space ])?;
			let val: f64 = out[space+1 ..].parse()
				.map_err(
					|e|
						Error::new(
							ErrorKind::InvalidData,
							ProtocolError::new(
								format!("failed to parse value: {}, '{}'", e, &out[space+1 ..])
							),
						)
				)?;
			res.push( (ts, val) );
		}

		Ok(res)
	}

	/// Discard and end the current transaction.
	///
	/// Same as `drop`, except you can see errors
	pub fn rollback(&self) -> Result<()>
	{
		assert!(self.in_tx.get());

		let mut w = self.writer.borrow_mut();
		let mut r = self.reader.borrow_mut();
		writeln!(&mut w, "rollback")?;
		w.flush()?;
		let mut error = String::new();
		r.read_line(&mut error)?;
		check_error(&mut error)?;
		self.in_tx.set(false);
		self.writing.set(false);
		Ok(())
	}

	/// Save and end the current transaction.
	///
	/// This must be called for any changes by a write transaction
	/// (that started by [`begin_write()`](#method.begin_write)) to be recorded.
	///
	/// In a read-only transaction, this is the same as [`rollback()`](#method.rollback).
	pub fn commit(&self) -> Result<()>
	{
		assert!(self.in_tx.get());
		let mut w = self.writer.borrow_mut();
		let mut r = self.reader.borrow_mut();
		writeln!(&mut w, "commit")?;
		w.flush()?;
		let mut out = String::new();
		r.read_line(&mut out)?;
		check_error(&mut out)?;
		self.in_tx.set(false);
		self.writing.set(false);
		Ok(())
	}

	fn check_write_tx(&self) -> Result<()>
	{
		if !self.in_tx.get()
		{
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"not in a transaction".to_string()
			));
		}
		if !self.writing.get()
		{
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"transaction is read only".to_string()
			));
		}
		Ok(())
	}

	/// Ensures a series by the given name already exists.
	///
	/// Does not fail if the series already exists.
	///
	/// You must call [`begin_write()`](#method.begin_write) prior to this function.
	pub fn create_series(&mut self, name: &str, format: &str)
		-> Result<()>
	{
		self.check_write_tx()?;

		let mut w = self.writer.borrow_mut();
		let mut r = self.reader.borrow_mut();
		writeln!(
			&mut w,
			"create {} {}",
			escape(name),
			escape(format),
		)?;
		w.flush()?;
		let mut out = String::new();
		r.read_line(&mut out)?;
		check_error(&mut out)?;

		Ok(())
	}

	/// Adds a single value to a series
	///
	/// Fails if a value at the given timestamp already exists.
	///
	/// * `series_name` is the name of the series, as created by
	/// [`create_series`](#method.create_series).
	/// * `time` is the point in time to add the sample, which
	/// must be unique (and also must be after all other timestamps
	/// in this series, until this feature is added which should be soon).
	/// * `value` is the sample to insert at this timepoint.
	///
	/// You must call [`begin_write()`](#method.begin_write) prior to this function.
	pub fn add_value<V: ToValue>(
		&mut self,
		series_name: &str,
		time: &NaiveDateTime,
		value: V,
	) -> Result<()>
	{
		use std::ops::DerefMut;
		self.check_write_tx()?;
		let mut w = self.writer.borrow_mut();
		let mut r = self.reader.borrow_mut();
		write!(
			&mut w,
			"add1 {} {} ",
			escape(series_name),
			format_time(time),
		)?;
		value.serialize(w.deref_mut())?;
		writeln!(&mut w, "")?;
		w.flush()?;
		let mut error = String::new();
		r.read_line(&mut error)?;
		check_error(&mut error)?;
		Ok(())
	}

	pub fn add_row_raw(
		&mut self,
		series_name: &str,
		time: &NaiveDateTime,
		row: &str,
	) -> Result<()>
	{
		self.check_write_tx()?;
		let mut w = self.writer.borrow_mut();
		let mut r = self.reader.borrow_mut();
		writeln!(
			&mut w,
			"add1 {} {} {}",
			escape(series_name),
			format_time(time),
			row,
		)?;
		w.flush()?;
		let mut error = String::new();
		r.read_line(&mut error)?;
		check_error(&mut error)?;
		Ok(())
	}

	/// Efficiently add many samples into a timeseries.
	///
	/// The timestamps must be sorted ascending.
	///
	/// * `series_name` is the series to insert the values into.
	/// * `src` is the iterator to read values from.
	///
	/// ```no_run
	/// # let stream = std::net::TcpStream::connect("localhost:5599").unwrap();
	/// # let mut client = sonnerie_api::Client::new(stream).unwrap();
	/// # let ts1: sonnerie_api::NaiveDateTime = "2015-01-01".parse().unwrap();
	/// # let ts2: sonnerie_api::NaiveDateTime = "2015-01-01".parse().unwrap();
	/// # let ts3: sonnerie_api::NaiveDateTime = "2015-01-01".parse().unwrap();
	/// # let ts4: sonnerie_api::NaiveDateTime = "2015-01-01".parse().unwrap();
	/// client.add_values_from(
	///     "fibonacci",
	///     [(ts1, 1.0), (ts2, 1.0), (ts3, 2.0), (ts3, 3.0)].iter().cloned()
	/// );
	/// ```
	///
	/// You must call [`begin_write()`](#method.begin_write) prior to this function.
	pub fn add_rows_from(
		&mut self,
		series_name: &str,
		src: &[ (NaiveDateTime, &[&ToValue]) ] ,
	) -> Result<()>
	{
		self.check_write_tx()?;
		let mut w = self.writer.borrow_mut();
		let mut r = self.reader.borrow_mut();
		writeln!(
			&mut w,
			"add {}",
			escape(series_name),
		)?;
		let mut error = String::new();

		for (t,r) in src
		{
			write!(
				&mut w,
				"{}",
				format_time(&t),
			)?;

			for v in *r
			{
				v.serialize(w.as_mut())?;
			}
			writeln!(&mut w, "")?;
		}

		w.flush()?;
		r.read_line(&mut error)?;
		check_error(&mut error)?;

		Ok(())
	}

	/// Read all values from many series
	///
	/// Selects many series with an SQL-like "LIKE" operator
	/// and dumps values from those series.
	///
	/// * `like` is a string with `%` as a wildcard. For example,
	/// `"192.168.%"` selects all series whose names start with
	/// `192.168.`. If the `%` appears near the end, then the
	/// query is very efficient.
	/// * `results` is a function which receives each value.
	///
	/// The values are always generated first for each series
	/// in ascending order and then each timestamp in ascending order.
	/// (In other words, each series gets its own group of samples
	/// before moving to the following series).
	pub fn dump<F>(
		&mut self,
		like: &str,
		results: F,
	) -> Result<()>
		where F: FnMut(&str, NaiveDateTime, f64)
			-> ::std::result::Result<(), String>
	{
		let from = NaiveDateTime::from_timestamp(0,0);
		let to = max_time();
		self.dump_range(like, &from, &to, results)
	}

	/// Read many values from many series
	///
	/// Selects many series with an SQL-like "LIKE" operator
	/// and dumps values from those series.
	///
	/// * `like` is a string with `%` as a wildcard. For example,
	/// `"192.168.%"` selects all series whose names start with
	/// `192.168.`. If the `%` appears in the end, then the
	/// query is very efficient.
	/// * `first_time` is the first timestamp for which to print
	/// all values per series.
	/// * `last_time` is the last timestamp (inclusive) to print
	/// all values per series.
	/// * `results` is a function which receives each value.
	///
	/// The values are always generated first for each series
	/// in ascending order and then each timestamp in ascending order.
	/// (In other words, each series gets its own group of samples
	/// before moving to the following series).
	pub fn dump_range<F>(
		&mut self,
		like: &str,
		first_time: &NaiveDateTime,
		last_time: &NaiveDateTime,
		mut results: F,
	) -> Result<()>
		where F: FnMut(&str, NaiveDateTime, f64)
			-> ::std::result::Result<(), String>
	{
		let _maybe = TransactionLock::read(self)?;
		let mut w = self.writer.borrow_mut();
		let mut r = self.reader.borrow_mut();
		writeln!(
			&mut w,
			"dump {} {} {}",
			escape(like),
			format_time(first_time),
			format_time(last_time),
		)?;
		w.flush()?;
		let mut out = String::new();
		loop
		{
			out.clear();
			r.read_line(&mut out)?;
			check_error(&mut out)?;

			let (series_name, remainder) = split_one(&out)
				.ok_or_else(||
					Error::new(
						ErrorKind::InvalidData,
						ProtocolError::new(format!("reading series name")),
					)
				)?;
			if series_name.len() == 0 { break; }
			let (ts, remainder) = split_one(&remainder)
				.ok_or_else(||
					Error::new(
						ErrorKind::InvalidData,
						ProtocolError::new(format!("reading timestamp")),
					)
				)?;
			let (val, _) = split_one(&remainder)
				.ok_or_else(||
					Error::new(
						ErrorKind::InvalidData,
						ProtocolError::new(format!("reading value")),
					)
				)?;

			let ts = parse_time(&ts)?;

			let val: f64 = val.parse()
				.map_err(
					|e|
						Error::new(
							ErrorKind::InvalidData,
							ProtocolError::new(
								format!("failed to parse value: {}, '{}'", e, val)
							),
						)
				)?;
			results(&series_name, ts, val)
				.map_err(
					|e|
						Error::new(
							ErrorKind::Other,
							ProtocolError::new(format!("{:?}", e)),
						)
				)?;
		}
		Ok(())
	}
}

impl Drop for Client
{
	fn drop(&mut self)
	{
		if self.in_tx.get()
		{
			let _ = self.rollback();
		}
	}
}


fn format_time(t: &NaiveDateTime) -> u64
{
	t.timestamp() as u64 * NANO
		+ (t.timestamp_subsec_nanos() as u64)
}

fn parse_time(text: &str) -> Result<NaiveDateTime>
{
	let ts: u64 = text.parse()
		.map_err(
			|e|
				Error::new(
					ErrorKind::InvalidData,
					ProtocolError::new(
						format!("failed to parse timestamp: {}, '{}'", e, text)
					),
				)
		)?;
	let ts = NaiveDateTime::from_timestamp(
		(ts/NANO) as i64,
		((ts%NANO) * NANO) as u32
	);
	Ok(ts)
}

/// The maximum timestamp allowed by Sonnerie.
///
/// 2^64-1 nanoseconds since the Unix Epoch. The minimum timestamp is 0,
/// or the Unix Epoch exactly.
pub fn max_time() -> NaiveDateTime
{
	let max = std::u64::MAX;
	NaiveDateTime::from_timestamp((max/NANO) as i64, (max%NANO) as u32)
}

fn check_error(l: &mut String) -> Result<()>
{
	if l.starts_with("error")
	{
		Err(Error::new(
			ErrorKind::Other,
			std::mem::replace(l, String::new()),
		))
	}
	else
	{
		Ok(())
	}
}

