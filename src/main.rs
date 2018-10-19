#[macro_use]
extern crate intrusive_collections;

mod service;
mod metadata;
mod blocks;
mod wal;
mod db;
mod disk_wal;
mod block_file;
mod client;
mod row_format;

extern crate rusqlite;
extern crate clap;
extern crate nix;
extern crate daemonize;

use std::fs::create_dir;

use clap::{Arg, App, SubCommand};
use std::path::Path;

fn main()
{
	let args =
		App::new("Sonnerie")
			.version("1.0")
			.author("Charles Samuels <kalle@eventures.vc>, e.ventures Management LLC")
			.about("A database for storing time-series")
			.arg(Arg::with_name("listen")
				.long("listen")
				.short("l")
				.help("listen on this address and port or unix domain socket")
				.takes_value(true)
			)
			.subcommand(
				SubCommand::with_name("start")
					.about("Starts the database")
					.arg(Arg::with_name("no-fork")
						.long("no-fork")
						.short("F")
						.help("Do not fork a daemon process")
					)
					.arg(Arg::with_name("data")
						.long("data")
						.short("d")
						.value_name("DIR")
						.help("Specifies location to store data")
						.takes_value(true)
						.required(true)
					)
			)
			.subcommand(
				SubCommand::with_name("client")
					.about("Starts a REPL session")
					.arg(Arg::with_name("read")
						.long("read")
						.short("r")
						.help("Start the client in a read-only transaction")
					)
					.arg(Arg::with_name("command")
						.long("command")
						.short("c")
						.help("Run specified command, then exit")
						.takes_value(true)
						.requires("read")
					)
			)
			.get_matches();

	
	let address = args.value_of("listen").unwrap_or("[::1]:5599");

	if let Some(args) = args.subcommand_matches("start")
	{
		let path = args.value_of("data").expect("require a database dir (--data)");
		let _ = create_dir(path);
		let db = db::Db::open(Path::new(path).to_path_buf());

		if address.starts_with("/") || address.starts_with("unix:")
		{
			let address =
				if address.starts_with("unix:")
					{ &address[5..] }
				else
					{ address };

			use std::os::unix::net::UnixListener;
			let listener
				= UnixListener::bind(address)
				.expect(&format!("binding to socket {}", address));

			if ! args.is_present("no-fork")
			{
				daemonize::Daemonize::new()
					.working_directory(".")
					.start()
					.expect("failed to daemonize");
			}
			service::service_unix(listener, db);
		}
		else
		{
			use std::net::TcpListener;
			let listener
				= TcpListener::bind(address)
				.expect("binding to socket");

			if ! args.is_present("no-fork")
			{
				daemonize::Daemonize::new()
					.working_directory(".")
					.start()
					.expect("failed to daemonize");
			}
			service::service_tcp(listener, db);
		}
	}
	else if let Some(args) = args.subcommand_matches("client")
	{
		client::run(args, address);
	}
	else
	{
		eprintln!("no subcommand specified");
		std::process::exit(1);
	}
}
