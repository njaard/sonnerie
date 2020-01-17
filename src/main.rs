use sonnerie::formatted;
use std::path::Path;
use std::fs::File;
use sonnerie::*;
use std::io::Write;

fn main()
	-> std::io::Result<()>
{
	use clap::{SubCommand,Arg};
	let matches
		= clap::App::new("sonnerie")
			.version("0.5")
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
					.arg(Arg::with_name("unsafe-nocheck")
						.long("unsafe-nocheck")
						.help("suppress the format coherency check (makes insertions faster)")
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
					.arg(Arg::with_name("unsafe-nocheck")
						.long("unsafe-nocheck")
						.help("suppress the format coherency check (makes insertions faster)")
						.requires("gegnum")
					)
			)
			.subcommand(
				SubCommand::with_name("read")
					.about("reads records")
					.arg(Arg::with_name("filter")
						.help("select the keys to print out, \"%\" is the wildcard")
						.takes_value(true)
						.required(true)
					)
			)
			.get_matches();

	let dir = matches.value_of_os("dir").expect("--dir");
	let dir = std::path::Path::new(dir);

	if let Some(matches) = matches.subcommand_matches("add")
	{
		let format = matches.value_of("format").unwrap();
		let nocheck = matches.is_present("unsafe-nocheck");
		let ts_format = matches.value_of("timestamp-format");
		add(&dir, format, ts_format, nocheck);
	}
	else if let Some(matches) = matches.subcommand_matches("compact")
	{
		let gegnum = matches.value_of_os("gegnum");
		let ts_format = matches.value_of("timestamp-format").unwrap_or("%FT%T");
		let nocheck = matches.is_present("unsafe-nocheck");

		compact(
			&dir,
			matches.is_present("major"),
			gegnum,
			ts_format,
			nocheck,
		).expect("compacting");
	}
	else if let Some(matches) = matches.subcommand_matches("read")
	{
		let filter = matches.value_of("filter").unwrap();
		let filter = Wildcard::new(filter);

		let stdout = std::io::stdout();
		let mut stdout = std::io::BufWriter::new(stdout.lock());

		let db = DatabaseReader::new(dir)?;
		for record in db.get_filter(&filter)
		{
			formatted::print_record(
				&record, &mut stdout
			)?;
			writeln!(&mut stdout, "")?;
		}
	}
	else
	{
		eprintln!("A command must be specified (read, add, compact)");
		std::process::exit(1);
	}

	Ok(())
}

fn add(dir: &Path, fmt: &str, ts_format: Option<&str>, nocheck: bool)
{
	let db = DatabaseReader::new(dir).expect("opening db");
	let mut tx = CreateTx::new(dir).expect("creating tx");

	let stdin = std::io::stdin();
	let mut stdin = stdin.lock();

	formatted::add_from_stream(&mut tx, &db, fmt, &mut stdin, ts_format, nocheck)
		.expect("adding value");
	tx.commit().expect("failed to commit transaction");
}

fn compact(
	dir: &Path, major: bool,
	gegnum: Option<&std::ffi::OsStr>, ts_format: &str, nocheck: bool)
	-> Result<(), crate::WriteFailure>
{
	use fs2::FileExt;

	let lock = File::create(dir.join(".compact"))?;
	lock.lock_exclusive()?;

	let db;
	if major
		{ db = DatabaseReader::new(dir)?; }
	else
		{ db = DatabaseReader::without_main_db(dir)?; }
	let db = std::sync::Arc::new(db);


	let mut compacted = CreateTx::new(dir)?;

	if let Some(gegnum) = gegnum
	{
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
		let reader_thread = std::thread::spawn(
			move || -> std::io::Result<()>
			{
				{
					let reader = reader_db.get_range(..);
					for record in reader
					{
						formatted::print_record_with_fmt(
							&record, &ts_format_copy, &mut childinput
						)?;
						writeln!(&mut childinput, "")?;
					}
				}
				Ok(())
			}
		);

		let childoutput = child.stdout.take().expect("process had no stdout");
		let mut childoutput = std::io::BufReader::new(childoutput);
		formatted::add_from_stream_with_fmt(
			&mut compacted, &db, &mut childoutput,
			Some(ts_format),
			nocheck,
		)?;

		reader_thread.join()
			.expect("failed to join subprocess writing thread")
			.expect("child writer failed");
		let result = child.wait()?;
		if !result.success()
		{
			panic!("child process failed: cancelling compact");
		}
	}
	else
	{
		{
			let ps = db.transaction_paths();
			if ps.len() == 1 && ps[0].file_name().expect("filename") == "main"
			{
				eprintln!("nothing to do");
				return Ok(());
			}
		}
		// create the new transaction after opening the database reader
		let reader = db.get_range(..);
		let mut n=0u64;
		for record in reader
		{
			compacted.add_record(
				record.key(),
				record.format(),
				record.value(),
			)?;
			n+= 1;
		}
		eprintln!("compacted {} records", n);
	}

	if major
	{
		compacted.commit_to(&dir.join("main"))
			.expect("failed to replace main database");
	}
	else
	{
		compacted.commit()
			.expect("failed to commit compacted database");
	}

	for txfile in db.transaction_paths()
	{
		if txfile.file_name().expect("filename in txfile") == "main"
			{ continue; }
		if let Err(e) = std::fs::remove_file(&txfile)
		{
			eprintln!("warning: failed to remove {:?}: {}", txfile, e);
		}
	}

	Ok(())
}
