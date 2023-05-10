use ::rayon::prelude::*;
use chrono::{NaiveDate, NaiveDateTime};
use clap::{Parser, Subcommand};
use sonnerie::{formatted, *};
use std::ffi::OsString;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Opt {
	/// Store data here in this directory. Create a "main" file here first.
	#[clap(short, long)]
	dir: PathBuf,

	#[clap(subcommand)]
	command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
	/// Adds records.
	Add {
		#[clap(short, long)]
		format: String,

		/// Instead of nanoseconds since the epoch, use this strftime format.
		#[clap(long)]
		timestamp_format: Option<String>,
	},
	/// Deletes records.
	Delete {
		/// Select the keys to print out, "%" is the wildcard.
		#[clap(required_unless_present_any = ["before_key", "after_key", "before_time", "after_time", "time"])]
		filter: Option<String>,

		/// Delete values after (and including) this key.
		#[clap(long)]
		after_key: Option<String>,

		/// Delete values before (but not including) this key.
		#[clap(long)]
		before_key: Option<String>,

		/// Delete values after (and including) this time.
		#[clap(long, conflicts_with = "time")]
		after_time: Option<EasyNaiveDateTime>,

		/// Delete values before (but not including) this time
		/// (in ISO-9601 format, date, seconds, or nanosecond precision).
		#[clap(long, conflicts_with = "time")]
		before_time: Option<EasyNaiveDateTime>,

		/// Delete values at exactly this time (in ISO-9601 format, date, seconds, or nanosecond precision).
		#[clap(long, conflicts_with = "time")]
		time: Option<EasyNaiveDateTime>,
	},
	/// Merges transactions.
	Compact {
		/// Compact everything into a new main database.
		#[clap(short = 'M', long)]
		major: bool,

		/// Run this command, writing compacted data as if by "read"
		/// into the process's stdin, and reading its stdout as if by "add".
		/// This is useful for removing or modifying data.
		///
		/// It is recommended to backup the database first
		/// (or make hard links of the files). You probably want to
		/// use this with --major to get the entire database.
		#[clap(long)]
		gegnum: Option<OsString>,

		/// With --gegnum, instead of nanoseconds since the epoch, use this strftime format.
		#[clap(long, requires = "gegnum")]
		timestamp_format: Option<String>,
	},
	/// Reads records.
	Read {
		/// Select the keys to print out, "%" is the wildcard.
		#[clap(required_unless_present_any = ["before_key", "after_key"])]
		filter: Option<String>,

		/// Output the line format after the timestamp for each record.
		#[clap(long)]
		print_format: bool,

		/// Instead of "%F %T", output in this strftime format.
		#[clap(long, default_value = "%F %T")]
		timestamp_format: String,

		/// Print timestamps as nanoseconds since the unix epoch.
		#[clap(long, conflicts_with = "timestamp_format")]
		timestamp_nanos: bool,

		/// Print timestamps as seconds since the unix epoch (rounded down if necessary).
		#[clap(
			long,
			conflicts_with = "timestamp_format",
			conflicts_with = "timestamp_nanos"
		)]
		timestamp_seconds: bool,

		/// Read values before (but not including) this key.
		#[clap(long, conflicts_with = "filter")]
		before_key: Option<String>,

		/// Read values after (and including) this key.
		#[clap(long, conflicts_with = "filter")]
		after_key: Option<String>,

		/// Read values before (but not including) this time
		/// (in ISO-9601 format, date, seconds, or nanosecond precision).
		#[clap(long)]
		before_time: Option<EasyNaiveDateTime>,

		/// Read values after (and including) this time, as --before-time.
		#[clap(long)]
		after_time: Option<EasyNaiveDateTime>,

		/// Run several of this command in parallel, piping a portion of the results into each.
		/// Keys are never divided between two commands.
		#[clap(long)]
		parallel: Option<String>,
	},
}

fn main() -> std::io::Result<()> {
	let opt = Opt::parse();

	match opt.command {
		Command::Add {
			format,
			timestamp_format,
		} => add(&opt.dir, &format, timestamp_format.as_deref()),
		Command::Compact {
			major,
			gegnum,
			timestamp_format,
		} => compact(
			&opt.dir,
			major,
			gegnum.as_deref(),
			timestamp_format.as_deref(),
		)
		.expect("compacting"),
		Command::Delete {
			filter,
			after_key,
			before_key,
			after_time,
			before_time,
			time,
		} => {
			let mut after_time = after_time.map(|d| d.0);
			let mut before_time = before_time.map(|d| d.0);

			if let Some(time) = time {
				after_time = Some(time.0);
				before_time = Some(time.0 + chrono::Duration::nanoseconds(1));
			}

			delete(
				&opt.dir,
				after_key.as_deref(),
				before_key.as_deref(),
				after_time,
				before_time,
				filter.as_deref(),
			);
		}
		Command::Read {
			filter,
			print_format,
			timestamp_format,
			timestamp_nanos,
			timestamp_seconds,
			before_key,
			after_key,
			before_time,
			after_time,
			parallel,
		} => {
			let after_time = after_time.map(|t| t.0.timestamp_nanos() as u64);
			let before_time = before_time.map(|t| t.0.timestamp_nanos() as u64);

			let stdout = std::io::stdout();
			let mut stdout = std::io::BufWriter::new(stdout.lock());
			let db = DatabaseReader::new(&opt.dir)?;

			let print_record_format = if print_format {
				formatted::PrintRecordFormat::Yes
			} else {
				formatted::PrintRecordFormat::No
			};
			let print_timestamp = if timestamp_nanos {
				formatted::PrintTimestamp::Nanos
			} else if timestamp_seconds {
				formatted::PrintTimestamp::Seconds
			} else {
				formatted::PrintTimestamp::FormatString(&timestamp_format)
			};

			macro_rules! filter_parallel {
				($filter:expr) => {{
					let filter = $filter;

					use std::io::BufWriter;
					use std::process::*;

					let shell = &std::env::var_os("SHELL").unwrap_or("sh".into());

					struct CheckOnDrop(Child, BufWriter<ChildStdin>);
					impl Drop for CheckOnDrop {
						fn drop(&mut self) {
							self.1.flush().unwrap();
							let s = self.0.wait().unwrap();
							if !s.success() {
								panic!("parallel worker failed");
							}
						}
					}

					let subproc = || {
						let mut child = Command::new(shell)
							.arg("-c")
							.arg(parallel.as_ref().unwrap())
							.stdin(Stdio::piped())
							.spawn()
							.unwrap();
						let stdout = BufWriter::new(child.stdin.take().unwrap());
						(child, stdout)
					};

					filter
						.into_par_iter()
						.for_each_init(subproc, |(_, out), record| {
							let ts = record.timestamp_nanos();
							if let Some(after_time) = after_time {
								if ts < after_time {
									return;
								}
							}
							if let Some(before_time) = before_time {
								if ts >= before_time {
									return;
								}
							}
							formatted::print_record(
								&record,
								out,
								print_timestamp,
								print_record_format,
							)
							.expect("failed to write to subprocess");
							writeln!(out, "").expect("failed to write to subprocess");
						});
				}};
			}
			macro_rules! filter {
				($filter:expr) => {{
					for record in $filter {
						let ts = record.timestamp_nanos();
						if let Some(after_time) = after_time {
							if ts < after_time {
								continue;
							}
						}
						if let Some(before_time) = before_time {
							if ts >= before_time {
								continue;
							}
						}
						formatted::print_record(
							&record,
							&mut stdout,
							print_timestamp,
							print_record_format,
						)?;
						writeln!(&mut stdout, "")?;
					}
				}};
			}

			if parallel.is_some() {
				match (after_key.as_deref(), before_key.as_deref(), filter) {
					(Some(a), None, None) => filter_parallel!(db.get_range(a..)),
					(None, Some(b), None) => filter_parallel!(db.get_range(..b)),
					(Some(a), Some(b), None) => filter_parallel!(db.get_range(a..b)),
					(None, None, Some(filter)) => {
						let w = Wildcard::new(&filter);
						filter_parallel!(db.get_filter(&w));
					}
					_ => unreachable!(),
				}
			} else {
				match (after_key.as_deref(), before_key.as_deref(), filter) {
					(Some(a), None, None) => filter!(db.get_range(a..)),
					(None, Some(b), None) => filter!(db.get_range(..b)),
					(Some(a), Some(b), None) => filter!(db.get_range(a..b)),
					(None, None, Some(filter)) => {
						let w = Wildcard::new(&filter);
						filter!(db.get_filter(&w));
					}
					_ => unreachable!(),
				}
			}
		}
	}

	Ok(())
}

// this prepares the database reader and creates a CreateTx, which then passes
// its information to add_from_stream
//
// add_from_stream parses the timestamp and the key then uses
// row_format::to_stored_format to obtain a bytewise interpretation of the
// payload, which is then passed into CreateTx::add_record
//
// delete's approach is to copy what add_from_stream does and call
// CreateTx::add_record with a prepared bare payload
fn add(dir: &Path, fmt: &str, ts_format: Option<&str>) {
	let _db = DatabaseReader::new(dir).expect("opening db");
	let mut tx = CreateTx::new(dir).expect("creating tx");

	let stdin = std::io::stdin();
	let mut stdin = stdin.lock();

	formatted::add_from_stream(&mut tx, fmt, &mut stdin, ts_format).expect("adding value");
	tx.commit().expect("failed to commit transaction");
}

// delete prepares a payload, as detailed by the specification
// then delete passes the payload into CreateTx::add_record which requires a
// key and format. CreateTx records the key which is set into the first_key and
// last_key of the segment header. the format is for the format of the row in
// the compressed payload. the payload is the payload
fn delete(
	dir: &Path,
	first_key: Option<&str>,
	last_key: Option<&str>,
	after_time: Option<NaiveDateTime>,
	before_time: Option<NaiveDateTime>,
	filter: Option<&str>,
) {
	let mut tx = CreateTx::new(dir).expect("creating tx");

	let after_time = after_time.map(|t| t.timestamp_nanos() as u64).unwrap_or(0);
	let before_time = before_time
		.map(|t| t.timestamp_nanos() as u64)
		.unwrap_or(u64::MAX);

	tx.delete(
		first_key.unwrap_or(""),
		last_key.unwrap_or(""),
		after_time,
		before_time,
		filter.unwrap_or("%"),
	)
	.expect("deleting rows");
	tx.commit().expect("failed to commit transaction");
}

fn compact(
	dir: &Path,
	major: bool,
	gegnum: Option<&std::ffi::OsStr>,
	ts_format: Option<&str>,
) -> Result<(), crate::WriteFailure> {
	use fs2::FileExt;

	let lock = File::create(dir.join(".compact"))?;
	lock.lock_exclusive()?;

	let db = if major {
		DatabaseReader::new(dir)?
	} else {
		DatabaseReader::without_main_db(dir)?
	};
	let db = std::sync::Arc::new(db);

	let mut compacted = CreateTx::new(dir)?;

	if let Some(gegnum) = gegnum {
		let mut child = std::process::Command::new("/bin/sh")
			.arg("-c")
			.arg(gegnum)
			.stdin(std::process::Stdio::piped())
			.stdout(std::process::Stdio::piped())
			.spawn()
			.expect("unable to run --gegnum process");

		let childinput = child.stdin.take().expect("process had no stdin");
		let mut childinput = std::io::BufWriter::new(childinput);

		let ts_format_cloned = ts_format.map(|m| m.to_owned());

		// a thread that reads from "db" and writes to the child
		let reader_db = db.clone();
		let reader_thread = std::thread::spawn(move || -> std::io::Result<()> {
			let timestamp_format = if let Some(ts_format) = &ts_format_cloned {
				formatted::PrintTimestamp::FormatString(ts_format)
			} else {
				formatted::PrintTimestamp::Nanos
			};

			let reader = reader_db.get_range(..);
			for record in reader {
				formatted::print_record(
					&record,
					&mut childinput,
					timestamp_format,
					formatted::PrintRecordFormat::Yes,
				)?;
				writeln!(&mut childinput)?;
			}
			Ok(())
		});

		let childoutput = child.stdout.take().expect("process had no stdout");
		let mut childoutput = std::io::BufReader::new(childoutput);
		formatted::add_from_stream_with_fmt(&mut compacted, &mut childoutput, ts_format)?;

		reader_thread
			.join()
			.expect("failed to join subprocess writing thread")
			.expect("child writer failed");
		let result = child.wait()?;
		if !result.success() {
			panic!("child process failed: cancelling compact");
		}
	} else {
		{
			let ps = db.transaction_paths();
			if ps.len() == 1 && ps[0].file_name().expect("filename") == "main" {
				eprintln!("nothing to do");
				return Ok(());
			}
		}
		// create the new transaction after opening the database reader
		let reader = db.get_range(..);
		let mut n = 0u64;
		for record in reader {
			compacted.add_record_raw(record.key(), record.format(), record.raw())?;
			n += 1;
		}
		eprintln!("compacted {} records", n);
	}

	sonnerie::_purge_compacted_files(compacted, dir, &db, major).expect("failure compacting");

	Ok(())
}

#[derive(Debug, Clone)]
struct EasyNaiveDateTime(NaiveDateTime);

impl FromStr for EasyNaiveDateTime {
	type Err = &'static str;

	fn from_str(t: &str) -> Result<Self, Self::Err> {
		if let Ok(k) = NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S.f") {
			Ok(EasyNaiveDateTime(k))
		} else if let Ok(k) = NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S") {
			Ok(EasyNaiveDateTime(k))
		} else if let Ok(k) = NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S.f") {
			Ok(EasyNaiveDateTime(k))
		} else if let Ok(k) = NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S") {
			Ok(EasyNaiveDateTime(k))
		} else if let Ok(k) = NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S.f") {
			Ok(EasyNaiveDateTime(k))
		} else if let Ok(k) = NaiveDate::parse_from_str(t, "%Y-%m-%d") {
			Ok(EasyNaiveDateTime(k.and_hms_opt(0, 0, 0).unwrap()))
		} else {
			Err("invalid date and time")
		}
	}
}
