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
//!     // start a write transaction
//!     client.begin_write()?;
//!     client.create_series("fibonacci", "u")?;
//!     client.add_value(
//!         "fibonacci",
//!         &"2018-01-06T00:00:00".parse().unwrap(),
//!         13.0,
//!     )?;
//!     let results: Vec<(sonnerie_api::NaiveDateTime, Vec<sonnerie_api::OwnedColumn>)> =
//!         client.read_series("fibonacci")?;
//!     for row in &results
//!     {
//!         // interpret each column as an integer
//!         for col in &row.1 { let _: u32 = col.from(); }
//!     }
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

use escape_string::{escape, split_one};

use std::cell::{Cell,RefCell,RefMut};

mod types;

pub use types::FromValue;
pub use types::ToValue;
pub use types::OwnedColumn;
pub use types::Column;


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

	/// Read values within a range of timestamps in a specific series.
	///
	/// Fails if the series does not exist, but returns an empty
	/// Vec if no samples were contained in that range.
	///
	/// * `first_time` is the first timestamp to begin reading from
	/// * `last_time` is the last timestamp to read (inclusive)
	/// * `to` is a callback function which receives each row
	pub fn read_series_range_to<F>(
		&mut self,
		name: &str,
		first_time: &NaiveDateTime,
		last_time: &NaiveDateTime,
		mut to: F
	) -> Result<()>
		where F: FnMut(NaiveDateTime, &[Column])
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
		let mut out = String::new();
		loop
		{
			out.clear();
			r.read_line(&mut out)?;
			check_error(&mut out)?;

			let (ts, mut remainder) = split_one(&out)
				.ok_or_else(||
					Error::new(
						ErrorKind::InvalidData,
						ProtocolError::new(format!("reading timestamp")),
					)
				)?;
			if ts.is_empty() { break; }

			let ts = parse_time(&ts)?;

			// TODO: reuse allocations for split_columns and columns
			let mut split_columns = vec!();
			while !remainder.is_empty()
			{
				let s = split_one(remainder);
				if s.is_none()
				{
					return Err(Error::new(
						ErrorKind::InvalidData,
						ProtocolError::new(format!("reading columns")),
					));
				}
				let s = s.unwrap();
				split_columns.push( s.0 );
				remainder = s.1;
			}

			let mut columns = vec!();
			for c in &split_columns
			{
				columns.push( Column { serialized: c } );
			}

			to( ts, &columns );
		}

		Ok(())
	}

	/// Read all the values in a specific series.
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
	) -> Result<Vec<(NaiveDateTime, Vec<OwnedColumn>)>>
	{
		let mut out = vec!();
		self.read_series_range_to(
			name,
			first_time, last_time,
			|ts, cols|
			{
				let r = cols.iter().map( |e| e.copy() ).collect();
				out.push((ts,r));
			}
		)?;
		Ok(out)
	}

	/// Read all the values in a specific series.
	///
	/// Fails if the series does not exist, but returns an empty
	/// Vec if the series does exist and is simply empty.
	pub fn read_series(
		&mut self,
		name: &str,
	) -> Result<Vec<(NaiveDateTime, Vec<OwnedColumn>)>>
	{
		let from = NaiveDateTime::from_timestamp(0,0);
		let to = max_time();
		self.read_series_range(name, &from, &to)
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

	/// Read the format for a series
	///
	/// The string returned is the same specified as `format`
	/// in [`create_series()`](#method.create_series).
	///
	/// Fails if the series doesn't exist.
	pub fn format(&self, series: &str) -> Result<String>
	{
		let _maybe = TransactionLock::read(self)?;
		let mut w = self.writer.borrow_mut();
		let mut r = self.reader.borrow_mut();
		writeln!(&mut w, "format {}", escape(series))?;
		w.flush()?;
		let mut out = String::new();
		r.read_line(&mut out)?;
		check_error(&mut out)?;
		let (fmt, _) = split_one(&out)
			.ok_or_else( ||
				Error::new(
					ErrorKind::InvalidData,
					ProtocolError::new(format!("parsing response to format: \"{}\"", out)),
				)
			)?;
		Ok(fmt.to_string())
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
	/// Fails if the preexisting series has a different format,
	/// but otherwise does not fail.
	///
	/// `format` is a string, one character per column that defines
	/// how each sample in your time series is stored.
	///
	/// The permitted characters are:
	/// * `f` - a 32 bit float (f32)
	/// * `F` - a 64 bit float (f64)
	/// * `u` - a 32 bit unsigned integer (u32)
	/// * `U` - a 64 bit unsigned integer (u64)
	/// * `i` - a 32 bit signed integer (i32)
	/// * `I` - a 64 bit signed integer (i64)
	///
	/// For example, "`FFii`" stores a 4 column record with two 64-bit floats
	/// and two 32-bit signed integers.
	///
	/// Reading and writing to this series requires you to provide types
	/// that are compatible with the format string.
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
	/// Fails if this series's format doesn't have exactly one
	/// column, and its type cannot be interpreted as compatible.
	///
	/// * `series_name` is the name of the series, as created by
	/// [`create_series`](#method.create_series).
	/// * `time` is the point in time to add the sample, which
	/// must be unique (and also must be after all other timestamps
	/// in this series, until this feature is added which should be soon).
	/// * `value` is the sample to insert at this timepoint, and is interpreted
	/// according to the format for the series's format.
	///
	/// You must call [`begin_write()`](#method.begin_write) prior to this function.
	pub fn add_value<V: FromValue>(
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

	/// Insert data that is parsed from a string
	///
	/// * `series_name` is the name of the series, as created by
	/// [`create_series`](#method.create_series).
	/// * `time` is the point in time to add the sample, which
	/// must be unique (and also must be after all other timestamps
	/// in this series, until this feature is added which should be soon).
	/// * `row` is a space-delimited string whose values are parsed
	/// by column according to the series's format.
	///
	/// This function panics if it the row contains a newline character.
	///
	/// You must call [`begin_write()`](#method.begin_write) prior to this function.
	pub fn add_row_raw(
		&mut self,
		series_name: &str,
		time: &NaiveDateTime,
		row: &str,
	) -> Result<()>
	{
		if row.find('\n').is_some()
			{ panic!("row contains non-permitted data"); }

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
	/// Returns an object that can accept each row.
	/// The timestamps must be sorted ascending.
	///
	/// ```no_run
	/// # let stream = std::net::TcpStream::connect("localhost:5599").unwrap();
	/// # let mut client = sonnerie_api::Client::new(stream).unwrap();
	/// # let ts1: sonnerie_api::NaiveDateTime = "2015-01-01".parse().unwrap();
	/// # let ts2: sonnerie_api::NaiveDateTime = "2015-01-01".parse().unwrap();
	/// # let ts3: sonnerie_api::NaiveDateTime = "2015-01-01".parse().unwrap();
	/// # let ts4: sonnerie_api::NaiveDateTime = "2015-01-01".parse().unwrap();
	/// {
	///     // add rows with one column
	///     let mut adder = client.add_rows("fibonacci").unwrap();
	///     adder.row(&ts1, &[&1.0]);
	///     adder.row(&ts2, &[&1.0]);
	///     adder.row(&ts3, &[&2.0]);
	///     adder.row(&ts3, &[&3.0]);
	/// }
	///
	/// {
	///     // add rows with two columns (in this case, a float and an integer)
	///     let mut adder = client.add_rows("san-francisco:temp-and-humidity").unwrap();
	///     adder.row(&ts1, &[&25.0, &45]);
	///     adder.row(&ts2, &[&24.5, &48]);
	///     adder.row(&ts3, &[&24.2, &49]);
	///     adder.row(&ts3, &[&23.9, &49]);
	/// }
	/// ```
	///
	/// You must call [`begin_write()`](#method.begin_write) prior to this function.
	pub fn add_rows<'s>(
		&'s mut self,
		series_name: &str,
	) -> Result<RowAdder<'s>>
	{
		self.check_write_tx()?;
		let mut w = self.writer.borrow_mut();
		let r = self.reader.borrow_mut();
		writeln!(
			&mut w,
			"add {}",
			escape(series_name),
		)?;

		let r =
		RowAdder
		{
			r: r,
			w: w,
			done: false,
		};

		Ok(r)
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
	/// Specify the types of the parameters to `results`, due to
	/// [a Rust compiler bug](https://github.com/rust-lang/rust/issues/41078).
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
		where F: FnMut(&str, NaiveDateTime, &[Column])
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
	/// Specify the types of the parameters to `results`, due to
	/// [a Rust compiler bug](https://github.com/rust-lang/rust/issues/41078).
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
		where F: FnMut(&str, NaiveDateTime, &[Column])
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
			if series_name.is_empty() { break; }
			let (ts, mut remainder) = split_one(&remainder)
				.ok_or_else(||
					Error::new(
						ErrorKind::InvalidData,
						ProtocolError::new(format!("reading timestamp")),
					)
				)?;

			// TODO: reuse allocations for split_columns and columns
			let mut split_columns = vec!();
			while !remainder.is_empty()
			{
				let s = split_one(remainder);
				if s.is_none()
				{
					return Err(Error::new(
						ErrorKind::InvalidData,
						ProtocolError::new(format!("reading columns")),
					));
				}
				let s = s.unwrap();
				split_columns.push( s.0 );
				remainder = s.1;
			}

			let mut columns = vec!();
			for c in &split_columns
			{
				columns.push( Column { serialized: c } );
			}

			let ts = parse_time(&ts)?;

			results(&series_name, ts, &columns);
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
		(ts%NANO) as u32
	);
	Ok(ts)
}

/// A function returned by [`Client::add_rows`](struct.Client.html#method.add_rows).
pub struct RowAdder<'client>
{
	w: RefMut<'client, Box<Write>>,
	r: RefMut<'client, Box<BufRead>>,
	done: bool,
}

impl<'client> RowAdder<'client>
{
	/// Add a single row
	///
	/// Panics on error. Call [`row_checked`](#method.row)
	/// in order to test for failures.
	pub fn row(&mut self, t: &NaiveDateTime, cols: &[&FromValue])
	{
		self.row_checked(t, cols).unwrap();
	}


	pub fn row_checked(&mut self, t: &NaiveDateTime, cols: &[&FromValue])
		-> Result<()>
	{
		write!(&mut self.w, "{} ", format_time(t))?;
		for v in cols.iter()
		{
			v.serialize(self.w.as_mut())?;
		}
		writeln!(&mut self.w, "")?;

		Ok(())
	}

	/// Explicitly end the transaction, testing for errors
	///
	/// Calling this function is optional, you can just
	/// let the object go out of scope, but this function
	/// allows you to check for errors.
	pub fn finish(mut self) -> Result<()>
	{
		self.finish_ref()
	}

	fn finish_ref(&mut self) -> Result<()>
	{
		let mut error = String::new();
		self.done = true;
		self.w.flush()?;
		self.r.read_line(&mut error)?;
		check_error(&mut error)?;

		Ok(())
	}
}

impl<'client> Drop for RowAdder<'client>
{
	fn drop(&mut self)
	{
		if !self.done
		{
			self.finish_ref().unwrap();
		}
	}
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

