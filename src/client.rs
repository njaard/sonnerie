extern crate shlex;
extern crate sonnerie_api;
extern crate rustyline;
extern crate clap;
extern crate chrono;

use self::sonnerie_api::NaiveDateTime;

use std::net::TcpStream;

use std::process::{Child,Stdio,Command};

use std::io::Write;

pub fn run(args: &::clap::ArgMatches, address: &str)
{
	let stream = TcpStream::connect(address)
		.expect("failed to connect");

	let mut client = sonnerie_api::Client::new(stream)
		.expect("initiating connection");

	let mut in_tx=false;
	let mut writing=false;

	if args.is_present("read")
	{
		client.begin_read().unwrap();
		in_tx=true;
	}

	use clap::{Arg,App,SubCommand};
	let app = App::new("sonnerie")
		.setting(clap::AppSettings::NoBinaryName)
		.subcommand(
			SubCommand::with_name("begin")
				.about("begins a read or write transaction")
				.arg(Arg::with_name("read")
					.long("read")
					.short("r")
					.help("make it a read transaction")
					.required_unless("write")
					.conflicts_with("write")
				)
				.arg(Arg::with_name("write")
					.long("write")
					.short("w")
					.help("make it a write transaction")
					.required_unless("read")
					.conflicts_with("read")
				)
		)
		.subcommand(
			SubCommand::with_name("read")
				.about("read a range")
				.arg(Arg::with_name("from")
					.long("from")
					.short("f")
					.help("starting at this date")
					.takes_value(true)
				)
				.arg(Arg::with_name("to")
					.long("to")
					.short("t")
					.takes_value(true)
					.help("ending at (and including) this date")
				)
				.arg(Arg::with_name("series")
					.takes_value(true)
					.required(true)
					.help("name of series")
				)
		)
		.subcommand(
			SubCommand::with_name("exit")
				.about("disconnect, rolling back any transaction")
		)
		.subcommand(
			SubCommand::with_name("commit")
				.about("save the current transaction")
		)
		.subcommand(
			SubCommand::with_name("rollback")
				.about("discard the current transaction")
		)
		.subcommand(
			SubCommand::with_name("create")
				.about("create a new series")
				.arg(Arg::with_name("series")
					.index(1)
					.required(true)
				)
		)
		.subcommand(
			SubCommand::with_name("add")
				.about("add a value")
				.arg(Arg::with_name("series")
					.index(1)
					.required(true)
				)
				.arg(Arg::with_name("ts")
					.index(2)
					.required(true)
				)
				.arg(Arg::with_name("value")
					.index(3)
					.required(true)
				)
		)
		.subcommand(
			SubCommand::with_name("dump")
				.about("read values from many series")
				.arg(Arg::with_name("like")
					.help("for series names SQL-like this string (\"%\" is the wildcard)")
					.takes_value(true)
				)
				.arg(Arg::with_name("from")
					.long("from")
					.short("f")
					.help("starting at this date")
					.takes_value(true)
				)
				.arg(Arg::with_name("to")
					.long("to")
					.short("t")
					.takes_value(true)
					.help("ending at (and including) this date")
				)
		)
		.subcommand(
			SubCommand::with_name("help")
		)
		.subcommand(
			SubCommand::with_name("exit")
				.alias("quit")
				.alias("q")
		);


	if let Some(c) = args.value_of("command")
	{
		let a = command(&app, &mut client, &mut in_tx, &mut writing, c);
		if !a.unwrap_or(true)
		{
			::std::process::abort();
		}
	}
	else
	{
		let mut rl = rustyline::Editor::<()>::new();
		loop
		{
			let prompt;
			if writing && in_tx
				{ prompt = "sonnerie[w]> " }
			else if !writing && in_tx
				{ prompt = "sonnerie[r]> " }
			else
				{ prompt = "sonnerie> " };
			let r = rl.readline(prompt);
			let line;
			match r
			{
				Err(rustyline::error::ReadlineError::Eof) => return,
				Err(e) => panic!("error {}", e),
				Ok(l) => line = l,
			};

			if line.is_empty() { continue; }
			rl.add_history_entry(line.clone());
			let a = command(&app, &mut client, &mut in_tx, &mut writing, &line);
			if a.is_none() { break; }
		}
	}
}

/// run a single command returning Some(true) on success, Some(false) on error
/// and None when it's time to exit
fn command<'client>(
	app: &clap::App,
	client: &'client mut sonnerie_api::Client,
	in_tx: &mut bool,
	writing: &mut bool,
	line: &str,
) -> Option<bool>
{
	let cmd = shlex::split(&line);
	if cmd.is_none()
	{
		eprintln!("error parsing command");
		return Some(false);
	}

	let cmd = cmd.unwrap();

	let m = app.clone().get_matches_from_safe( cmd );
	if let Err(e) = m
		{ println!("{}", e); return Some(true); }
	let m = m.unwrap();
	match m.subcommand()
	{
		("help", _) =>
		{
			app.clone().print_help().unwrap();
			println!("");
		},
		("begin", Some(cmd)) =>
		{
			if *in_tx
			{
				eprintln!("already in a transaction (commit or rollback first)");
				return Some(false);
			}
			if cmd.is_present("read")
			{
				client.begin_read().unwrap();
				*in_tx = true;
				*writing = false;
			}
			else
			{
				client.begin_write().unwrap();
				*in_tx = true;
				*writing = true;
			}
		},
		("create", Some(cmd)) =>
		{
			if !*in_tx || !*writing
			{
				eprintln!("not in a writing transaction");
				return Some(false);
			}

			let name = cmd.value_of("series").unwrap();
			let r = client.create_series(name);
			if let Err(e) = r
			{
				eprintln!("error creating series: {:?}", e);
				return Some(false);
			}
		},
		("commit", Some(_)) =>
		{
			if !*in_tx
			{
				eprintln!("not in a transaction");
				return Some(false);
			}

			if let Err(e) = client.commit()
			{
				eprintln!("error committing transaction: {:?}", e);
				return Some(false);
			}
			*in_tx = false;
		},
		("rollback", Some(_)) =>
		{
			if !*in_tx
			{
				eprintln!("not in a transaction");
				return Some(false);
			}

			if let Err(e) = client.commit()
			{
				eprintln!("error rolling back transaction: {:?}", e);
				return Some(false);
			}
			*in_tx = false;
		},
		("read", Some(cmd)) =>
		{
			let from;
			if let Some(v) = cmd.value_of("from")
			{
				if let Some(v) = parse_human_times(v)
					{ from = v; }
				else
				{
					eprintln!("couldn't parse --from time");
					return Some(false);
				}
			}
			else
			{
				from = NaiveDateTime::new(
					chrono::NaiveDate::from_ymd(-262144,1,1),
					chrono::NaiveTime::from_hms(0,0,0)
				);
			}
			let to;
			if let Some(v) = cmd.value_of("to")
			{
				if let Some(v) = parse_human_times(v)
					{ to = v; }
				else
				{
					eprintln!("couldn't parse --to time");
					return Some(false);
				}
			}
			else
			{
				to = NaiveDateTime::new(
					chrono::NaiveDate::from_ymd(262143,12,31),
					chrono::NaiveTime::from_hms(0,0,0)
				);
			}

			let samples = client.read_series_range(
				cmd.value_of("series").unwrap(),
				&from,
				&to,
			);

			match samples
			{
				Ok(samples) =>
				{
					let mut child = run_pager();
					{
						let mut stdin = child.stdin.as_mut().expect("Failed to open stdin");
						for (ts,val) in samples
						{
							writeln!(stdin, "{}   {}", ts, val)
								.unwrap();
						}
					}
					child.stdin.take();
					child.wait().unwrap();
				},
				Err(e) =>
				{
					eprintln!("error reading values: {:?}", e);
					return Some(false);
				}
			}
		},
		("dump", Some(cmd)) =>
		{
			let like = cmd.value_of("like").unwrap_or("%");

			let from;
			if let Some(v) = cmd.value_of("from")
			{
				if let Some(v) = parse_human_times(v)
					{ from = v; }
				else
				{
					eprintln!("couldn't parse --from time");
					return Some(false);
				}
			}
			else
			{
				from = NaiveDateTime::new(
					chrono::NaiveDate::from_ymd(-262144,1,1),
					chrono::NaiveTime::from_hms(0,0,0)
				);
			}
			let to;
			if let Some(v) = cmd.value_of("to")
			{
				if let Some(v) = parse_human_times(v)
					{ to = v; }
				else
				{
					eprintln!("couldn't parse --to time");
					return Some(false);
				}
			}
			else
			{
				to = NaiveDateTime::new(
					chrono::NaiveDate::from_ymd(262143,12,31),
					chrono::NaiveTime::from_hms(0,0,0)
				);
			}

			let mut child = run_pager();

			{
				let mut stdin = ::std::io::BufWriter::new(child.stdin.take().expect("Failed to open stdin"));

				let res;
				{
					let display =
						|name: &str, ts, val| -> ::std::result::Result<(), String>
						{
							writeln!(stdin, "{:?}\t{}\t{}", ts, val, name)
								.unwrap();
							Ok(())
						};

					res = client.dump_range(like, &from, &to, display);
				}
				if let Err(_) = res
				{
					writeln!(stdin, "(unexpected failure)")
						.unwrap();
					return Some(false);
				}
			}

			child.wait().unwrap();
		},
		("add", Some(cmd)) =>
		{
			if !*in_tx || !*writing
			{
				eprintln!("not in a writing transaction");
				return Some(false);
			}

			let series = cmd.value_of("series").unwrap();

			let ts;
			if let Some(v) = parse_human_times(cmd.value_of("ts").unwrap())
				{ ts = v; }
			else
			{
				eprintln!("couldn't parse time");
				return Some(false);
			}

			let value = cmd.value_of("value").unwrap().parse();
			if value.is_err()
			{
				eprintln!("unable to parse value");
				return Some(false);
			}
			let value: f64 = value.unwrap();

			let r = client.add_value(series, &ts, value);
			if let Err(e) = r
			{
				eprintln!("error inserting value: {:?}", e);
				return Some(false);
			}
		},
		("exit", _) | ("q", _) | ("quit", _) => return None,
		_ =>
		{
			eprintln!("unknown command");
			return Some(false);
		}

	}
	Some(true)
}

fn run_pager() -> Child
{
	if let Ok(p) = ::std::env::var("PAGER")
	{
		let child = Command::new(p)
			.stdin(Stdio::piped())
			.spawn();
		if let Ok(c) = child
			{ return c; }
	}
	let child = Command::new("less")
		.arg("-FX")
		.stdin(Stdio::piped())
		.spawn();
	if let Ok(c) = child
		{ return c; }
	let child = Command::new("more")
		.stdin(Stdio::piped())
		.spawn();
	if let Ok(c) = child
		{ return c; }
	let child = Command::new("cat")
		.stdin(Stdio::piped())
		.spawn();
	if let Ok(c) = child
		{ return c; }

	panic!("failed to run any kind of pager, even cat");
}

fn parse_human_times(t: &str)
	-> Option<NaiveDateTime>
{
	if let Ok(a) = NaiveDateTime::parse_from_str(
		t, "%Y-%m-%d %H:%M:%S"
	)
		{ return Some(a); };
	if let Ok(a) = NaiveDateTime::parse_from_str(
		t, "%Y-%m-%dT%H:%M:%S"
	)
		{ return Some(a); };
	if let Ok(a) = NaiveDateTime::parse_from_str(
		&format!("{} 00:00:00", t), "%Y-%m-%d %H:%M:%S"
	)
		{ return Some(a); };
	None
}
