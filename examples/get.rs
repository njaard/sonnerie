use sonnerie::Reader;
use std::io::Write;
use std::path::PathBuf;

fn main() -> std::io::Result<()> {
	use clap::Arg;
	let matches = clap::Command::new("get")
		.version("1.0")
		.author("Charles Samuels <kalle@eventures.vc>")
		.about("Dump a single transaction file data")
		.arg(
			Arg::new("db")
				.help("database file")
				.required(true)
				.action(clap::ArgAction::Set),
		)
		.arg(
			Arg::new("filter")
				.required(true)
				.action(clap::ArgAction::Set),
		)
		.get_matches();

	let db: &PathBuf = matches.get_one("db").unwrap();
	let filter: &String = matches.get_one("filter").unwrap();

	let stdout = std::io::stdout();
	let mut stdout = stdout.lock();

	let filter = sonnerie::Wildcard::new(filter);

	let w = std::fs::File::open(db)?;
	let r = Reader::new(w).unwrap();
	for record in r.left().unwrap().get_filter(&filter) {
		sonnerie::formatted::print_record(
			&record,
			&mut stdout,
			sonnerie::formatted::PrintTimestamp::Seconds,
			sonnerie::formatted::PrintRecordFormat::Yes,
			&choice_string::Selection::All,
		)?;
		writeln!(&mut stdout)?;
	}

	Ok(())
}
