extern crate escape_string;
extern crate chrono;

use std::thread;
use std::io::{Read,Write,BufReader,BufRead,BufWriter};
use std::sync::Arc;

use db::Db;
use db::Timestamp;
use db::Transaction;

use self::escape_string::{split_one, split, escape};

struct Session<'db>
{
	db: &'db Db,
	input_lines: ::std::io::Lines<BufReader<Box<Read>>>,
	writer: BufWriter<Box<Write>>,
	transaction: Option<Transaction<'db>>,
	cache_last_series_id: Option<(String,u64)>,
}

impl<'db> Session<'db>
{
	fn new(r: Box<Read>, w: Box<Write>, db: &'db Db)
		-> Session<'db>
	{
		let reader = BufReader::new(r);
		let writer = BufWriter::new(w);
		let input_lines = reader.lines();

		Session
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
		{ // create a series by name and format
			let (name,remainder) = split_one(remainder)
				.ok_or_else( || "command requires series name".to_string() )?;
			let (format,_) = split_one(remainder)
				.ok_or_else( || "command requires format".to_string() )?;

			if let Some(tx) = self.transaction.as_mut()
			{
				if let Some(_) = tx.create_series(&name, &format)
				{
					writeln!(writer, "creating a timeseries named \"{}\"", name).unwrap();
				}
				else
				{
					writeln!(writer, "error: format \"{}\" does not match existing", format).unwrap();
				}
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
				tx.read_series(
					series_id,
					ts1,
					ts2,
					|ts, format, data|
					{
						write!(writer, "{}\t", ts.0).unwrap();
						format.to_protocol_format(data, writer).unwrap();
						writeln!(writer, "").unwrap();
					}
				);

				writeln!(writer, "").unwrap();
			}
			else
			{
				writeln!(writer, "error: not in a transaction").unwrap();
			}
		}
		else if cmd == "format"
		{
			let (name,_) = split_one(remainder)
				.ok_or_else( || "command requires series name".to_string() )?;

			if let Some(tx) = self.transaction.as_ref()
			{
				let f = tx.series_format_string(&name)
					.ok_or_else(|| format!("no series \"{}\"", name))?;
				writeln!(writer, "{}", f).unwrap();
			}
			else
			{
				writeln!(writer, "error: not in a transaction").unwrap();
			}
		}
		else if cmd == "create-add"
		{
			// create-add
			// <name> <format> <ts> <vals>
			// ...
			// (one blank line)

			let tx =
				if let Some(tx) = self.transaction.as_mut()
					{ tx }
				else
				{
					writeln!(writer, "error: not in a transaction").unwrap();
					return Ok(());
				};

			let line_reader = &mut self.input_lines;

			for line in line_reader
			{
				let line = line.map_err(|e| format!("failed to read input: {}", e))?;
				let (name,remainder) = split_one(&line)
					.ok_or_else( || "command requires series name".to_string() )?;
				if name.is_empty() { break; }
				let (format,remainder) = split_one(remainder)
					.ok_or_else( || "command requires format".to_string() )?;
				let (ts,values) = split_one(remainder)
					.ok_or_else( || "command requires timestamp".to_string() )?;
				let ts = parse_time(&ts)?;

				let id = tx.create_series(&name, &format)
					.ok_or_else( || format!("format for '{}' is different", format))?;

				let mut done = false;
				tx.insert_into_series(
					id,
					|fmt, bytes|
					{
						if done { return None; }
						done = true;
						fmt.to_stored_format(&ts, &values, bytes)
							.unwrap();
						Some(ts)
					}
				)?;
			}
			writeln!(writer, "inserted values").unwrap();
		}
		else if cmd == "add"
		{
			let args = split(remainder);
			if args.is_none() { Err("failed to parse arguments")?; }
			let args = args.unwrap();
			if args.len() != 1 { return Err("command requires exactly \
				one parameter".to_string()); }
			// add <name>
			// <ts> <vals>
			// ...
			// (one blank line)
			let name = &args[0];

			let line_reader = &mut self.input_lines;


			if let Some(tx) = self.transaction.as_mut()
			{
				let series_id = tx.series_id(name)
					.ok_or_else(|| format!("no series \"{}\"", name))?;
				if let Err(e) = tx.insert_into_series(
						series_id,
						|format, bytes|
						{
							let line = match line_reader.next()
							{
								Some(a) => a,
								None => panic!("error: failed to read input"),
							};
							let line = line.unwrap();
							let split_one = split_one(&line);
							if split_one.is_none()
							{
								panic!("error: failed to parse line: {}", line);
							}
							let split_one = split_one.unwrap();
							if split_one.0.is_empty() { return None; }
							let ts = parse_time(&split_one.0).unwrap();
							format.to_stored_format(&ts, &split_one.1, bytes).unwrap();
							Some(ts)
						}
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
			// add1 <name> <ts> <vals>
			let (name,remainder) = split_one(remainder)
				.ok_or_else( || "command requires series name".to_string() )?;
			let (ts,remainder) = split_one(remainder)
				.ok_or_else( || "command requires timestamp".to_string() )?;
			let ts = parse_time(&ts)?;

			if let Some(tx) = self.transaction.as_mut()
			{
				let series_id = cache_last_series_id(tx, &name)
					.ok_or_else(|| format!("no series \"{}\"", name))?;

				let mut did_one=false;

				tx.insert_into_series(
					series_id,
					|format, data|
					{
						if did_one { return None; }
						format.to_stored_format(&ts, remainder, data).unwrap();
						did_one=true;
						Some(ts)
					}
				)?;
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
							tx.read_series(
								series_id,
								ts1,
								ts2,
								|ts, format, data|
								{
									write!(writer, "{}\t{}\t", escape(&name), ts.0).unwrap();
									format.to_protocol_format(data, writer).unwrap();
									writeln!(writer, "").unwrap();
								}
							);
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

fn parse_time(t: &str) -> Result<Timestamp, String>
{
	let t: u64 = t.parse::<u64>()
		.map_err(|e| format!("failed to parse timestamp \"{}\": {}", t, e))?;
	Ok(Timestamp(t))
}

use std::net::TcpListener;

pub fn service_tcp(listener: TcpListener, db: Db)
{
	let db = Arc::new(db);
	
	for stream in listener.incoming()
	{
		match stream
		{
			Ok(stream) =>
			{
				println!(
					"Connection from {}",
					stream.peer_addr().unwrap()
				);

				let db = db.clone();
				thread::spawn(
					move ||
					{
						let r = stream.try_clone().unwrap();

						// connection succeeded
						let mut c = Session::new(
							Box::new(r), Box::new(stream), &db
						);
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

use std::os::unix::net::UnixListener;

pub fn service_unix(listener: UnixListener, db: Db)
{
	let db = Arc::new(db);

	for stream in listener.incoming()
	{
		match stream
		{
			Ok(stream) =>
			{
				println!("Connection");

				let db = db.clone();
				thread::spawn(
					move ||
					{
						let r = stream.try_clone().unwrap();

						// connection succeeded
						let mut c = Session::new(
							Box::new(r), Box::new(stream), &db
						);
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

