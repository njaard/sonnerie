use ::rayon::prelude::*;
use chrono::{NaiveDate, NaiveDateTime};
use sonnerie::formatted;
use sonnerie::*;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() -> std::io::Result<()> {
	use clap::{Arg, SubCommand};
	let matches
		= clap::App::new("sonnerie")
			.version("0.6.4")
			.author("Charles Samuels <kalle@eventures.vc>")
			.about("A compressed timeseries database")
			.arg(Arg::with_name("dir")
				.long("dir")
				.short("d")
				.help("store data here in this directory. Create a \"main\" file here first.")
				.required(true)
				.takes_value(true)
			)
			.subcommand(
				SubCommand::with_name("add")
					.about("adds records")
					.arg(Arg::with_name("format")
						.short("f")
						.long("format")
						.takes_value(true)
						.required(true)
					)
					.arg(Arg::with_name("timestamp-format")
						.long("timestamp-format")
						.help("instead of nanoseconds since the epoch, use this strftime format")
						.takes_value(true)
					)
			)
            .subcommand(
                SubCommand::with_name("delete")
                    .about("deletes records")
					.arg(Arg::with_name("filter")
						.help("select the keys to print out, \"%\" is the wildcard")
						.takes_value(true)
						.required_unless_one(&["before-key", "after-key"])
					)
					.arg(Arg::with_name("before-key")
						.long("before-key")
						.help("delete values before (but not including) this key")
						.takes_value(true)
					)
					.arg(Arg::with_name("after-key")
						.long("after-key")
						.help("delete values after (and including) this key")
						.takes_value(true)
					)
					.arg(Arg::with_name("before-time")
						.long("before-time")
						.help("delete values before (but not including) this time (in ISO-9601 format, date, seconds, or nanosecond precision)")
						.takes_value(true)
					)
					.arg(Arg::with_name("after-time")
						.long("after-time")
						.help("delete values after (and including) this time, as --before-time")
						.takes_value(true)
					)
            )
			.subcommand(
				SubCommand::with_name("compact")
					.about("merge transactions")
					.arg(Arg::with_name("major")
						.short("M")
						.long("major")
						.help("compact everything into a new main database")
					)
					.arg(Arg::with_name("gegnum")
						.long("gegnum")
						.help("Run this command, writing compacted data as if by \"read\" \
							into the process's stdin, and reading its stdout as if by \"add\". \
							This is useful for removing or modifying data. \
							It is recommended to backup the database first \
							(or make hard links of the files). You probably want to \
							use this with --major to get the entire database.")
						.takes_value(true)
					)
					.arg(Arg::with_name("timestamp-format")
						.long("timestamp-format")
						.help("with --gegnum, instead of nanoseconds since the epoch, use this strftime format")
						.takes_value(true)
						.requires("gegnum")
						.takes_value(true)
					)
			)
			.subcommand(
				SubCommand::with_name("read")
					.about("reads records")
					.arg(Arg::with_name("filter")
						.help("select the keys to print out, \"%\" is the wildcard")
						.takes_value(true)
						.required_unless_one(&["before-key", "after-key"])

					)
					.arg(Arg::with_name("print-format")
						.long("print-format")
						.help("Output the line format after the timestamp for each record")
					)
					.arg(Arg::with_name("timestamp-format")
						.long("timestamp-format")
						.help("instead of \"%F %T\", output in this strftime format")
						.takes_value(true)
					)
					.arg(Arg::with_name("timestamp-nanos")
						.long("timestamp-nanos")
						.help("Print timestamps as nanoseconds since the unix epoch")
						.conflicts_with("timestamp-format")
					)
					.arg(Arg::with_name("timestamp-seconds")
						.long("timestamp-seconds")
						.help("Print timestamps as seconds since the unix epoch (rounded down if necessary)")
						.conflicts_with("timestamp-format")
						.conflicts_with("timestamp-nanos")
					)
					.arg(Arg::with_name("before-key")
						.long("before-key")
						.help("read values before (but not including) this key")
						.takes_value(true)
						.conflicts_with("filter")
					)
					.arg(Arg::with_name("after-key")
						.long("after-key")
						.help("read values after (and including) this key")
						.takes_value(true)
						.conflicts_with("filter")
					)
					.arg(Arg::with_name("before-time")
						.long("before-time")
						.help("read values before (but not including) this time (in ISO-9601 format, date, seconds, or nanosecond precision)")
						.takes_value(true)
					)
					.arg(Arg::with_name("after-time")
						.long("after-time")
						.help("read values after (and including) this time, as --before-time")
						.takes_value(true)
					)
					.arg(Arg::with_name("parallel")
						.long("parallel")
						.help("Run several of this command in parallel, piping a portion of the results into each. Keys are never divided between two commands.")
						.takes_value(true)
					)
			)
			.get_matches();

	let dir = matches.value_of_os("dir").expect("--dir");
	let dir = std::path::Path::new(dir);

	if let Some(matches) = matches.subcommand_matches("add") {
		let format = matches.value_of("format").unwrap();
		let ts_format = matches.value_of("timestamp-format");
		add(dir, format, ts_format);
	} else if let Some(matches) = matches.subcommand_matches("compact") {
		let gegnum = matches.value_of_os("gegnum");
		let ts_format = matches.value_of("timestamp-format").unwrap_or("%FT%T");

		compact(dir, matches.is_present("major"), gegnum, ts_format).expect("compacting");
	} else if let Some(matches) = matches.subcommand_matches("delete") {
        let filter = matches.value_of("filter");
        let before_key = matches.value_of("before-key");
        let after_key = matches.value_of("after-key");
        let before_time = matches.value_of("before_time");
        let after_time = matches.value_of("before_time");
		let ts_format = matches.value_of("timestamp-format").unwrap_or("%FT%T");

        delete(
            dir,
            after_key,
            before_key,
            before_time,
            after_time,
            filter,
            ts_format,
        );
    } else if let Some(matches) = matches.subcommand_matches("read") {
		let print_format = matches.is_present("print-format");
		let timestamp_format = matches.value_of("timestamp-format").unwrap_or("%F %T");
		let timestamp_nanos = matches.is_present("timestamp-nanos");
		let timestamp_seconds = matches.is_present("timestamp-seconds");

		let after_key = matches.value_of("after-key");
		let before_key = matches.value_of("before-key");

		fn parse_time(t: &str) -> Option<NaiveDateTime> {
			if let Ok(k) = NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S.f") {
				Some(k)
			} else if let Ok(k) = NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S") {
				Some(k)
			} else if let Ok(k) = NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S.f") {
				Some(k)
			} else if let Ok(k) = NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S") {
				Some(k)
			} else if let Ok(k) = NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S.f") {
				Some(k)
			} else if let Ok(k) = NaiveDate::parse_from_str(t, "%Y-%m-%d") {
				Some(k.and_hms(0, 0, 0))
			} else {
				None
			}
		}

		let after_time = matches
			.value_of("after-time")
			.map(|t| parse_time(t).expect("parsing after-time").timestamp_nanos() as u64);
		let before_time = matches.value_of("before-time").map(|t| {
			parse_time(t)
				.expect("parsing before-time")
				.timestamp_nanos() as u64
		});
		let filter = matches.value_of("filter");

		let stdout = std::io::stdout();
		let mut stdout = std::io::BufWriter::new(stdout.lock());
		let db = DatabaseReader::new(dir)?;

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
			formatted::PrintTimestamp::FormatString(timestamp_format)
		};

		macro_rules! filter_parallel {
			($filter:expr) => {{
				let filter = $filter;

				use std::io::BufWriter;
				use std::process::*;

				let ref shell = std::env::var_os("SHELL").unwrap_or("sh".into());

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
						.arg(matches.value_of_os("parallel").unwrap())
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
						formatted::print_record(&record, out, print_timestamp, print_record_format)
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

		if matches.is_present("parallel") {
			match (after_key, before_key, filter) {
				(Some(a), None, None) => filter_parallel!(db.get_range(a..)),
				(None, Some(b), None) => filter_parallel!(db.get_range(..b)),
				(Some(a), Some(b), None) => filter_parallel!(db.get_range(a..b)),
				(None, None, Some(filter)) => {
					let w = Wildcard::new(filter);
					filter_parallel!(db.get_filter(&w));
				}
				_ => unreachable!(),
			}
		} else {
			match (after_key, before_key, filter) {
				(Some(a), None, None) => filter!(db.get_range(a..)),
				(None, Some(b), None) => filter!(db.get_range(..b)),
				(Some(a), Some(b), None) => filter!(db.get_range(a..b)),
				(None, None, Some(filter)) => {
					let w = Wildcard::new(filter);
					filter!(db.get_filter(&w));
				}
				_ => unreachable!(),
			}
		}
	} else {
		eprintln!("A command must be specified (read, add, compact, delete)");
		std::process::exit(1);
	}

	Ok(())
}

fn add(dir: &Path, fmt: &str, ts_format: Option<&str>) {
	let _db = DatabaseReader::new(dir).expect("opening db");
	let mut tx = CreateTx::new(dir).expect("creating tx");

	let stdin = std::io::stdin();
	let mut stdin = stdin.lock();

	formatted::add_from_stream(&mut tx, fmt, &mut stdin, ts_format).expect("adding value");
	tx.commit().expect("failed to commit transaction");
}

fn delete(
    dir: &Path,
    first_key: Option<&str>,
    last_key: Option<&str>,
    before_time: Option<&str>,
    after_time: Option<&str>,
    filter: Option<&str>,
    ts_format: &str,
) {
    let mut tx = CreateTx::new(dir).expect("creating tx");

    let ts_converter = |time: &str, ts_format: &str| {
        chrono::NaiveDateTime::parse_from_str(time, ts_format)
            .expect("parsing timestamp according to format")
            .timestamp_nanos() as Timestamp
    };

    let before_time = before_time
        .map(|bt| ts_converter(bt, ts_format))
        .unwrap_or(u64::MAX);
    let after_time = after_time
        .map(|at| ts_converter(at, ts_format))
        .unwrap_or(0);

    tx.delete(
        first_key.unwrap_or(""),
        last_key.unwrap_or(""),
        before_time,
        after_time,
        filter.unwrap_or("%"),
    ).expect("deleting rows");
    tx.commit().expect("failed to commit transaction");
}

fn compact(
	dir: &Path,
	major: bool,
	gegnum: Option<&std::ffi::OsStr>,
	ts_format: &str,
) -> Result<(), crate::WriteFailure> {
	use fs2::FileExt;

	let lock = File::create(dir.join(".compact"))?;
	lock.lock_exclusive()?;

	let db;
	if major {
		db = DatabaseReader::new(dir)?;
	} else {
		db = DatabaseReader::without_main_db(dir)?;
	}
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

		let ts_format_copy = ts_format.to_owned();
		// a thread that reads from "db" and writes to the child
		let reader_db = db.clone();
		let reader_thread = std::thread::spawn(move || -> std::io::Result<()> {
			let timestamp_format = formatted::PrintTimestamp::FormatString(&ts_format_copy);
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
		formatted::add_from_stream_with_fmt(&mut compacted, &mut childoutput, Some(ts_format))?;

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
			compacted.add_record(record.key(), record.format(), record.raw())?;
			n += 1;
		}
		eprintln!("compacted {} records", n);
	}

	if major {
		compacted
			.commit_to(&dir.join("main"))
			.expect("failed to replace main database");
	} else {
		compacted
			.commit()
			.expect("failed to commit compacted database");
	}

	for txfile in db.transaction_paths() {
		if txfile.file_name().expect("filename in txfile") == "main" {
			continue;
		}
		if let Err(e) = std::fs::remove_file(&txfile) {
			eprintln!("warning: failed to remove {:?}: {}", txfile, e);
		}
	}

	Ok(())
}
