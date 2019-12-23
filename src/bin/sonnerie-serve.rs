use std::path::PathBuf;

use hyper::Server;
use futures::Stream;
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Instant,Duration};

use serde_derive::*;

use sonnerie::*;

pub use hyper::Body;
pub type Response = hyper::Response<Body>;
pub type Request = hyper::Request<Body>;

use futures::future::{Future, lazy};
use futures::sink::Sink;
use escape_string::split_one;

type GenericError = tokio_threadpool::BlockingError;
type ResponseFuture = Box<dyn Future<Item=Response, Error=GenericError> + Send>;

fn main()
{
	use clap::Arg;
	let matches
		= clap::App::new("sonnerie-serve")
			.version("1.0")
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

	let threadpool = tokio_threadpool::Builder::new()
		.pool_size(400)
		.stack_size(1024*1024)
		.keep_alive(Some(std::time::Duration::from_secs(20)))
		.build();

	let srv = Tsrv
	{
		dir: dir.to_owned(),
		threadpool,
		shared_reader: RwLock::new(Arc::new(DatabaseReader::new(dir).unwrap())),
		shared_reader_age: RwLock::new(Some(Instant::now())),
	};

	let srv = Arc::new(srv);

	let new_service =
		move ||
		{
			let srv = srv.clone();
			hyper::service::service_fn(
				move |req: Request|
				{
					let srv = srv.clone();
					srv.run(req)
				}
			)
		};

	let exec = tokio::runtime::current_thread::TaskExecutor::current();

	let sev = Server::bind(&addr)
		.executor(exec)
		.serve(new_service)
		.map_err(|e| eprintln!("server error: {}", e));

	eprintln!("now running");
	tokio::runtime::current_thread::Runtime::new()
		.expect("rt new")
		.spawn(sev)
		.run()
		.expect("rt run");
}

struct Tsrv
{
	dir: PathBuf,
	threadpool: tokio_threadpool::ThreadPool,
	shared_reader: RwLock<Arc<DatabaseReader>>,
	shared_reader_age: RwLock<Option<Instant>>,
}


impl Tsrv
{
	fn run(self: Arc<Tsrv>, req: Request)
		-> ResponseFuture
	{
		match req.method()
		{
			&hyper::Method::GET =>
			{
				self.get_outer(req)
			},
			&hyper::Method::PUT =>
			{
				self.put_outer(req)
			},
			_ =>
				Box::new(futures::future::ok(
					hyper::Response::builder()
						.status(404)
						.body(Body::from("key not found"))
						.unwrap()
				)),
		}
	}

	fn put_outer(self: Arc<Tsrv>, req: Request)
		-> ResponseFuture
	{
		let srv = self.clone();
		Box::new(
		{
			let e =
			self.threadpool.spawn_handle(
				lazy(
					move ||
					{
						let e =std::panic::catch_unwind(
							std::panic::AssertUnwindSafe(move ||
								match srv.put(req)
								{
									Ok(v) => Ok(v),
									Err(s) =>
									{
										eprintln!("put error: {}", s);
										Ok(hyper::Response::builder()
											.status(500)
											.body(hyper::Body::from(format!("Failure: {}", s)))
											.unwrap())
									}
								}
							));
						match e
						{
							Ok(k) => k,
							Err(s) =>
							{
								Ok(hyper::Response::builder()
									.status(500)
									.body(hyper::Body::from(format!("Failure: (panic) {:?}", s)))
									.unwrap())
							}
						}
					}
				)
			);
			e
		})
	}

	fn put(&self, req: Request)
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

			let lines = lines_from_request::lines(req.into_body());

			for line in lines
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

				writer.send(rec)
					.map_err(|e| format!("writing to sorted tempfile: {}", e))?;
			}
		}

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

		hyper::Response::builder()
			.status(201)
			.header(hyper::header::CONTENT_TYPE, "text/plain")
			.body("ok".into())
			.map_err(|e| format!("{}", e))
	}

	fn get_outer(self: Arc<Tsrv>, req: Request)
		-> ResponseFuture
	{
		let srv = self.clone();
		Box::new(
		{
			let e =
			self.threadpool.spawn_handle(
				lazy(
					move ||
					{
						let e =std::panic::catch_unwind(
							std::panic::AssertUnwindSafe(move ||
								match srv.get(req)
								{
									Ok(v) => Ok(v),
									Err(s) =>
									{
										eprintln!("get error: {}", s);
										Ok(hyper::Response::builder()
											.status(500)
											.body(hyper::Body::from(format!("Failure: {}", s)))
											.unwrap())
									}
								}
							));
						match e
						{
							Ok(k) => k,
							Err(s) =>
							{
								Ok(hyper::Response::builder()
									.status(500)
									.body(hyper::Body::from(format!("Failure: (panic) {:?}", s)))
									.unwrap())
							}
						}
					}
				)
			);
			e
		})
	}

	fn get(self: Arc<Self>, req: Request)
		-> Result<Response, String>
	{
		let p = req.uri().path();
		if !p.starts_with("/")
		{
			return hyper::Response::builder()
				.status(hyper::StatusCode::BAD_REQUEST)
				.body(Body::from("invalid path"))
				.map_err(|e| format!("{}", e));
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

		let filter = sonnerie::Wildcard::new(key);
		let (send, recv) = futures::sync::mpsc::channel(16);

		let srv = self.clone();
		std::thread::spawn(
			move ||
			{
				let mut send = send.wait();

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
					if human_dates
					{
						sonnerie::formatted::print_record(
							&record, &mut row,
						).unwrap();
					}
					else
					{
						sonnerie::formatted::print_record_nanos(
							&record, &mut row,
						).unwrap();
					}
					row.push(b'\n');
					let e = send.send(row);
					if let Err(e) = e
					{
						eprintln!("channel error: {}", e);
						break;
					}
				}
			}
		);

		hyper::Response::builder()
			.header(hyper::header::CONTENT_TYPE, "text/plain")
			.body(Body::wrap_stream(recv.map_err(
				|_| Box::new(std::io::Error::new(std::io::ErrorKind::Other, "oh no")))))
			.map_err(|e| format!("{}", e))
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
