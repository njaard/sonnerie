extern crate escape_string;
extern crate chrono;

use std::net::{TcpListener, TcpStream};
use std::thread;
use std::io::{Write,BufReader,BufRead,BufWriter};
use std::sync::Arc;

use db::Db;
use db::Timestamp;
use db::Transaction;

use self::escape_string::{split_one, split, escape};

struct Client<'db>
{
	db: &'db Db,
	input_lines: ::std::io::Lines<BufReader<TcpStream>>,
	writer: BufWriter<TcpStream>,
	transaction: Option<Transaction<'db>>,
	cache_last_series_id: Option<(String,u64)>,
}

impl<'db> Client<'db>
{
	fn new(stream: TcpStream, db: &'db Db)
		-> Client<'db>
	{
		println!(
			"Connection from {}",
			stream.peer_addr().unwrap()
		);

		let reader = BufReader::new(stream.try_clone().unwrap());
		let writer = BufWriter::new(stream.try_clone().unwrap());
		let input_lines = reader.lines();

		Client
		{
			db: db,
			input_lines: input_lines,
			writer: writer,
			transaction: None,
			cache_last_series_id: None,
		}
	}

	fn run(&mut self)
	{
		writeln!(&mut self.writer, "Greetings from Sonnerie").unwrap();

		loop
		{
			self.writer.flush().unwrap();
			let line = self.input_lines.next();
			if line.is_none()
				{ break; }
			let line = line.unwrap().unwrap();

			let cmd = split_one(&line);
			if cmd.is_none()
			{
				writeln!(&mut self.writer, "error: failed to parse command: {}", line).unwrap();
				continue;
			}
			let cmd = cmd.unwrap();
			if cmd.0.len()==0 { continue; }
			if cmd.0 == "exit" { break; }

			if let Err(e) = self.one_command(&cmd.0, cmd.1)
			{
				writeln!(&mut self.writer, "error: {}", e).unwrap();
			}

		}
	}

	fn one_command(&mut self, cmd: &str, remainder: &str) -> Result<(), String>
	{
		let ref mut writer = self.writer;
		let ref mut db = self.db;
		let ref mut cache_last_series_id = self.cache_last_series_id;

		let mut cache_last_series_id =
			|tx: &Transaction, name: &str| -> Option<u64>
			{
				if let Some((cn, cv)) = cache_last_series_id.as_ref()
				{
					if name == cn { return Some(*cv); }
				}

				let series_id = tx.series_id(name);
				if let Some(series_id) = series_id
				{
					*cache_last_series_id = Some((name.to_string(), series_id));
					Some(series_id)
				}
				else
				{
					None
				}
			};


		if cmd == "help"
		{
			writeln!(
				writer, "{}", include_str!("help.txt")
			).unwrap();
		}
		else if cmd == "create"
		{ // create a series by name
			let arg = split_one(remainder);
			if arg.is_none() { Err("command requires exactly \
				one parameter".to_string())?; }
			let name = arg.unwrap().0;
			if let Some(tx) = self.transaction.as_mut()
			{
				let _ = tx.create_series(&name);
				writeln!(writer, "creating a timeseries named \"{}\"", name).unwrap();
			}
			else
			{
				writeln!(writer, "error: not in a transaction").unwrap();
			}
		}
		else if cmd == "begin"
		{ // begin a transaction
			let args = split(remainder);
			if args.is_none() { Err("failed to parse arguments")?; }
			let args = args.unwrap();
			if self.transaction.is_some()
			{
				writeln!(writer, "error: already in transaction").unwrap();
			}
			else if args.len()==1 && args[0] == "read"
			{
				self.transaction = Some( db.read_transaction() );
				writeln!(writer, "started transaction").unwrap();
			}
			else if args.len()==1 && args[0] == "write"
			{
				self.transaction = Some( db.write_transaction() );
				writeln!(writer, "started transaction").unwrap();
			}
			else
			{
				writeln!(writer, "error: you must specify 'read' or 'write'").unwrap();
			}
		}
		else if cmd == "commit"
		{ // commit a transaction
			if let Some(a) = self.transaction.take()
			{
				a.commit();
				writeln!(writer, "transaction completed").unwrap();
			}
			else
			{
				writeln!(writer, "error: not in a transaction").unwrap();
			}
		}
		else if cmd == "rollback"
		{ // discard a transaction
			if let Some(_) = self.transaction.take()
			{
				writeln!(writer, "transaction ended").unwrap();
			}
			else
			{
				writeln!(writer, "error: not in a transaction").unwrap();
			}
		}
		else if cmd == "read"
		{
			let args = split(remainder);
			if args.is_none() { Err("failed to parse arguments")?; }
			let args = args.unwrap();
			if args.len() != 3 { return Err("command requires exactly \
				3 parameters".to_string()); }
			let name = &args[0];
			let ts1 = parse_time(&args[1])?;
			let ts2 = parse_time(&args[2])?;

			if let Some(tx) = self.transaction.as_ref()
			{
				let series_id = cache_last_series_id(tx, name)
					.ok_or_else(|| format!("no series \"{}\"", name))?;
				let items = tx.read_series(
					series_id,
					ts1,
					ts2,
				);

				for (ts, val) in items
				{
					let ts = ts.0;
					writeln!(writer, "{}\t{:.17}", ts, val).unwrap();
				}
				writeln!(writer, "").unwrap();
			}
			else
			{
				writeln!(writer, "error: not in a transaction").unwrap();
			}
		}
		else if cmd == "add"
		{
			let args = split(remainder);
			if args.is_none() { Err("failed to parse arguments")?; }
			let args = args.unwrap();
			if args.len() != 1 { return Err("command requires exactly \
				one parameter".to_string()); }
			// add <name>
			// <ts> <val>
			// ...
			// (one blank line)
			let name = &args[0];

			let mut samples = vec!();

			loop
			{
				let line = match self.input_lines.next()
				{
					Some(a) => a,
					None => break,
				};

				let line = line.unwrap();
				let split_one = split_one(&line);
				if split_one.is_none()
				{
					writeln!(writer, "error: failed to parse line: {}", line).unwrap();
					continue;
				};
				let split_one = split_one.unwrap();
				if split_one.0.is_empty() { break; }

				let ts = parse_time(&split_one.0)?;
				let val = parse_float(&split_one.1)?;

				samples.push((ts, val));
			}

			if let Some(tx) = self.transaction.as_mut()
			{
				let series_id = tx.series_id(name)
					.ok_or_else(|| format!("no series \"{}\"", name))?;
				if let Err(e) = tx.insert_into_series(
						series_id,
						&mut samples
					)
				{
					writeln!(writer, "error: {}", e).unwrap();
				}
				writeln!(writer, "inserted values").unwrap();
			}
			else
			{
				writeln!(writer, "error: not in a transaction").unwrap();
			}
		}
		else if cmd == "add1"
		{
			let args = split(remainder);
			if args.is_none() { Err("failed to parse arguments")?; }
			let args = args.unwrap();
			if args.len() != 3 { Err("command requires exactly \
				four parameters".to_string())?; }
			// add1 <name> <ts> <val>
			let name = &args[0];
			let ts = parse_time(&args[1])?;

			let val = parse_float(&args[2])?;

			if let Some(tx) = self.transaction.as_mut()
			{
				let series_id = cache_last_series_id(tx, name)
					.ok_or_else(|| format!("no series \"{}\"", name))?;
				if let Err(e) = tx.insert_one_into_series(
					series_id,
					ts,
					val
				)
				{
					writeln!(writer, "error: {}", e).unwrap();
				}
				writeln!(writer, "inserted value").unwrap();
			}
			else
			{
				writeln!(writer, "error: not in a transaction").unwrap();
			}
		}
		else if cmd == "dump"
		{
			let args = split(remainder);
			if args.is_none() { Err("failed to parse arguments")?; }
			let args = args.unwrap();
			if args.len() != 3 { Err("command requires exactly \
				four parameters".to_string())?; }
			// add1 <name> <ts> <val>
			let like = &args[0];
			let ts1 = parse_time(&args[1])?;
			let ts2 = parse_time(&args[2])?;

			if let Some(tx) = self.transaction.as_ref()
			{
				{
					let print_res =
						|name: &str, series_id: u64|
						{
							let items = tx.read_series(
								series_id,
								ts1,
								ts2,
							);

							for (ts, val) in items
							{
								let ts = ts.0;
								writeln!(
									writer,
									"{}\t{}\t{:.17}",
									escape(&name), ts, val
								).unwrap();
							}
						};

					tx.series_like(
						like,
						print_res,
					);
				}
				writeln!(writer, "").unwrap();
			}
			else
			{
				writeln!(writer, "error: not in a transaction").unwrap();
			}
		}
		else
		{
			writeln!(writer, "error: no such command \"{}\"", cmd).unwrap();
		}
		Ok(())
	}
}

fn parse_float(t: &str) -> Result<f64, String>
{
	if t == "nan" { return Ok(::std::f64::NAN); }
	t.parse()
		.map_err(|e| format!("while parsing {}: {}", t, e))
}

fn parse_time(t: &str) -> Result<Timestamp, String>
{
	let t: i64 = t.parse::<i64>()
		.map_err(|e| format!("failed to parse timestamp \"{}\": {}", t, e))?;
	Ok(Timestamp(t))
}

pub fn service(listener: TcpListener, db: Db)
{
	let db = Arc::new(db);
	
	for stream in listener.incoming()
	{
		match stream
		{
			Ok(stream) =>
			{
				let db = db.clone();
				thread::spawn(
					move ||
					{
						let s = stream;
						// connection succeeded
						let mut c = Client::new(s, &db);
						c.run();
					}
				);
			}
			Err(e) =>
			{
				eprintln!("Failed to establish connection: {}", e);
			}
		}
    }
}

