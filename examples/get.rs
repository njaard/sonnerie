use sonnerie::Reader;
use std::io::Write;
use std::path::Path;

fn main() -> std::io::Result<()> {
	use clap::Arg;
	let matches = clap::App::new("get")
		.version("1.0")
		.author("Charles Samuels <kalle@eventures.vc>")
		.about("Dump a single transaction file data")
		.arg(
			Arg::with_name("db")
				.help("database file")
				.required(true)
				.takes_value(true),
		)
		.arg(Arg::with_name("filter").required(true).takes_value(true))
		.get_matches();

	let db = Path::new(matches.value_of_os("db").unwrap());
	let filter = matches.value_of("filter").unwrap();

	let stdout = std::io::stdout();
	let mut stdout = stdout.lock();

	let filter = sonnerie::Wildcard::new(filter);

	let w = std::fs::File::open(db)?;
	let r = Reader::new(w).unwrap();
	for record in r.get_filter(&filter).into_iter() {
		sonnerie::formatted::print_record(
			&record,
			&mut stdout,
			sonnerie::formatted::PrintTimestamp::Seconds,
			sonnerie::formatted::PrintRecordFormat::Yes,
		)?;
		writeln!(&mut stdout, "")?;
	}

	Ok(())
}
