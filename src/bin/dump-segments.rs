use std::path::Path;
use sonnerie::segment_reader::SegmentReader;

fn main()
  -> std::io::Result<()>
{
  use clap::Arg;
  let matches
    = clap::App::new("dump-segments")
      .version("1.0")
      .author("Charles Samuels <kalle@eventures.vc>")
      .about("Dump debugging information from a sonnerie db")
      .arg(Arg::with_name("db")
        .help("database file")
        .required(true)
        .takes_value(true)
      )
      .get_matches();

  let db = Path::new(matches.value_of_os("db").unwrap());

  let mut w = std::fs::File::open(db)?;
  let r = SegmentReader::open(&mut w).unwrap();

  r.print_info(&mut std::io::stdout())?;

  Ok(())
}
