use std::path::PathBuf;

use hyper::Server;
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Instant,Duration};

use serde_derive::*;

use sonnerie::*;

pub use hyper::Body;
pub type Response = hyper::Response<Body>;
pub type Request = hyper::Request<Body>;

use futures::stream::StreamExt;
use futures::sink::SinkExt;
use escape_string::split_one;

fn main()
{
	use clap::Arg;
	let matches
		= clap::App::new("sonnerie-serve")
			.version("0.5.4")
			.author("Charles Samuels <kalle@eventures.vc>")
			.about("A network server for sonnerie")
			.arg(Arg::with_name("listen")
				.long("listen")
				.short("l")
				.help("listen on this address (unix:/path or addr:port)")
				.required(true)
				.takes_value(true)
			)
			.arg(Arg::with_name("dir")
				.long("dir")
				.short("d")
				.help("store data here")
				.required(true)
				.takes_value(true)
			)
			.get_matches();

	let addr = matches.value_of("listen").expect("--listen");
	let addr = addr.parse().unwrap();
	let dir = matches.value_of_os("dir").expect("--dir");
	let dir = std::path::Path::new(dir);

	let mut runtime = tokio::runtime::Builder::new()
		.threaded_scheduler()
		.core_threads(4)
		.thread_name("sonnerie")
		.thread_stack_size(1024 * 1024)
		.enable_all()
		.build()
		.expect("tokio runtime");

	let srv = Tsrv
	{
		dir: dir.to_owned(),
		shared_reader: RwLock::new(Arc::new(DatabaseReader::new(dir).unwrap())),
		shared_reader_age: RwLock::new(Some(Instant::now())),
	};

	let srv = Arc::new(srv);

	let make_service = hyper::service::make_service_fn(
		move |_conn|
		{
			let srv = srv.clone();
			async move
			{
				Ok::<_, std::convert::Infallible>(hyper::service::service_fn(
					move |req: Request|
					{
						let srv = srv.clone();
						async move
						{
							let srv = srv.clone();
							match srv.run(req).await
							{
								k @ Ok(_) => k,
								Err(e) =>
									Ok(hyper::Response::builder()
										.status(500)
										.body(e.into())
										.unwrap()),
							}
						}
					}
				))
			}
		}
	);

	runtime
		.block_on(
			async
			{
				let serve = Server::bind(&addr)
					.serve(make_service);
				eprintln!("now running");
				serve.await
			}
		)
		.expect("rt run");
}

struct Tsrv
{
	dir: PathBuf,
	shared_reader: RwLock<Arc<DatabaseReader>>,
	shared_reader_age: RwLock<Option<Instant>>,
}


impl Tsrv
{
	async fn run(self: Arc<Tsrv>, req: Request)
		-> Result<Response, String>
	{
		match req.method()
		{
			&hyper::Method::GET =>
			{
				self.get(req).await
			},
			&hyper::Method::PUT =>
			{
				self.put(req).await
			},
			_ =>
				Ok(hyper::Response::builder()
					.status(hyper::StatusCode::BAD_REQUEST)
					.body(Body::from("invalid request"))
					.unwrap()),
		}
	}

	async fn put(&self, req: Request)
		-> Result<Response, String>
	{
		// let db = DatabaseReader::new(&self.dir).unwrap();
		let mut tx = CreateTx::new(&self.dir)
			.map_err(|e| format!("create tx: {}", e))?;

		/*enum CheckingFormat
		{
			NotYet,
			KeyAndFmt(String, String),
			NoMore,
		}*/

		//let mut next_key_to_check_fmt = CheckingFormat::NotYet;


		let tmpfile = tempfile::NamedTempFile::new()
			.map_err(|e| format!("creating tempfile {}", e))?;

		let mut sorted_file = shardio::ShardWriter::<SortingRecord>::new(
			tmpfile.path(),
			1024*100, // kind of random numbers
			1024*100*4,
			1024*100*4*4,
		).map_err(|e| format!("opening sorted writer {}", e))?;

		{
			let mut writer = sorted_file.get_sender();

			let mut lines = lines_from_request::lines(req.into_body());

			while let Some(line) = lines.next().await
			{
				let line = line.map_err(|e| format!("reading one row from network: {}", e))?;
				let line = String::from_utf8(line)
					.map_err(|e| format!("data must be utf-8: {}", e))?;
				let tail = line.trim_end();
				if tail.is_empty() { continue; }
				let (key, tail) = split_one(&tail).ok_or_else(|| format!("reading key"))?;
				let (timestamp, tail) = split_one(&tail).ok_or_else(|| format!("reading timestamp"))?;
				let ts: Timestamp = timestamp.parse().map_err(|e| format!("parsing timestamp {}", e))?;
				let (format, tail) = split_one(&tail).ok_or_else(|| format!("reading timestamp"))?;

				let rec = SortingRecord
				{
					key: key.to_string(),
					ts,
					format: format.to_string(),
					tail: tail.to_string(),
				};

				tokio::task::block_in_place(
					|| -> Result<(), String>
					{
						writer.send(rec)
							.map_err(|e| format!("writing to sorted tempfile: {}", e))
					}
				)?;
			}
		}

		tokio::task::block_in_place(
			|| -> Result<(), String>
			{
				sorted_file.finish()
					.map_err(|e| format!("doing the external sorting {}", e))?;

				let reader = shardio::ShardReader::<SortingRecord>::open(tmpfile.path())
					.map_err(|e| format!("opening sorted: {}", e))?;

				let mut row_data = vec!();

				for record in reader.iter()
					.map_err(|e| format!("reading from sorted: {}", e))?
				{
					let SortingRecord{ key, ts, format, tail } = record
						.map_err(|e| format!("parsing temporary data: {}", e))?;
					let row_format = parse_row_format(&format);
					row_format.to_stored_format(ts, &tail, &mut row_data)
						.map_err(|e| format!("parsing data according to format: {}", e))?;
					tx.add_record(&key, &format, &row_data)
						.map_err(|e| format!("processing record {}[{}]: {:?}", key, ts, e))?;
					row_data.clear();
				}

				tx.commit()
					.map_err(|e| format!("committing tx: {}", e))?;

				// after a commit happens, invalidate the shared reader
				{
					let mut age = self.shared_reader_age.write();
					*age = None;
				}

				Ok(())
			}
		)?;

		hyper::Response::builder()
			.status(201)
			.header(hyper::header::CONTENT_TYPE, "text/plain")
			.body("ok".into())
			.map_err(|e| format!("{}", e))
	}

	async fn get(self: Arc<Self>, req: Request)
		-> Result<Response, String>
	{
		let p = req.uri().path();
		if !p.starts_with("/")
		{
			return Ok(hyper::Response::builder()
				.status(hyper::StatusCode::BAD_REQUEST)
				.body(Body::from("invalid path"))
				.expect("error request"))
		}
		let key = &p[1..];

		let query_string : Vec<_> =
			match req.uri().query()
			{
				Some(q) =>
					url::form_urlencoded::parse(
						q.as_bytes()
					)
					.into_owned()
					.collect(),
				None => vec!()
			};

		let human_dates = query_string.iter().find(|k|k.0=="human").is_some();

		let timestamp_fmt;
		if human_dates
			{ timestamp_fmt = Default::default(); }
		else
			{ timestamp_fmt = sonnerie::formatted::PrintTimestamp::Nanos; }

		let filter = sonnerie::Wildcard::new(key);
		let (mut send, recv) = futures::channel::mpsc::channel(16);

		let srv = self.clone();
		std::thread::spawn(
			move ||
			futures::executor::block_on(
				async
				{
					let db;
					{
						// reuse the same reader object so that
						// we don't have to do a "dirent" on the db directory
						// and then open all the files all the time
						let mut make_new_reader = false;
						{
							let age = srv.shared_reader_age.read();
							if age.is_none() || age.unwrap().elapsed() > Duration::from_secs(10)
							{
								drop(age);
								// make sure another reader thread didn't get here first
								let mut age = srv.shared_reader_age.write();
								if age.is_none() || age.unwrap().elapsed() > Duration::from_secs(10)
								{
									*age = Some(Instant::now());
									make_new_reader = true;
								}
							}
						}

						if make_new_reader
						{
							let newdb = Arc::new(DatabaseReader::new(&srv.dir).unwrap());
							db = newdb.clone();
							let mut rdr = srv.shared_reader.write();
							*rdr = newdb;
						}
						else
						{
							let rdr = srv.shared_reader.read();
							db = rdr.clone();
						}
					}

					// trick sonnerie to not do an fadvise when you search for a single key
					let searcher: Box<dyn Iterator<Item=sonnerie::record::OwnedRecord>>;
					if filter.is_exact()
						{ searcher = Box::new(db.get(filter.prefix())); }
					else
						{ searcher = Box::new(db.get_filter(&filter)); }

					for record in searcher
					{
						let mut row: Vec<u8> = vec!();
						sonnerie::formatted::print_record2(
							&record,
							&mut row,
							timestamp_fmt,
							sonnerie::formatted::PrintRecordFormat::No,
						).unwrap();
						row.push(b'\n');
						let e = send.send(row).await;
						if let Err(e) = e
						{
							eprintln!("channel error: {}", e);
							break;
						}
					}
				}
			)
		);

		Ok(hyper::Response::builder()
			.header(hyper::header::CONTENT_TYPE, "text/plain")
			.body(Body::wrap_stream(recv.map(|a| -> Result<_, std::io::Error> { Ok(a) } )))
			.expect("creating response"))
	}
}


#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, PartialOrd, Ord)]
struct SortingRecord
{
	key: String,
	ts: Timestamp,
	format: String,
	tail: String,
}
