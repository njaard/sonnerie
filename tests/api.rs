extern crate tempfile;
extern crate chrono;
extern crate sonnerie_api;

use chrono::NaiveDateTime;
use std::process::{Stdio,Command, Child};
use std::os::unix::net::UnixStream;

use sonnerie_api::Client;

struct Instance
{
	_dir: tempfile::TempDir,
	service: Child,
	client: Client,
}

impl Drop for Instance
{
	fn drop(&mut self)
	{
		self.service.kill().unwrap();
	}
}

impl Instance
{
	fn new() -> Instance
	{
		let prog = std::env::current_exe().unwrap()
			.parent().unwrap()
			.parent().unwrap()
			.join("sonnerie");

		eprintln!("running {:?}", prog);

		let t = tempfile::TempDir::new().unwrap();
		let sock = t.path().join("sock");
		let service = Command::new(prog)
			.arg("-l")
			.arg(&format!("unix:{}", sock.to_str().unwrap()))
			.arg("start")
			.arg("-F")
			.arg("--data")
			.arg(t.path().join("data"))
			.stdin(Stdio::null())
			.stdout(Stdio::null())
			.spawn()
			.expect("spawning sonnerie");

		while !sock.exists()
		{
			std::thread::sleep(std::time::Duration::from_millis(100));
		}

		let conn = UnixStream::connect(sock).unwrap();
		let conn2 = conn.try_clone().unwrap();

		let client = Client::from_streams(conn, conn2).expect("from_streams");

		Instance
		{
			_dir: t,
			service,
			client,
		}
	}
}

#[test]
fn api_rollback()
{
	let mut n = Instance::new();
	let ref mut client = n.client;

	client.begin_write().unwrap();
	{
		let mut adder = client.create_and_add().unwrap();
		adder.row(
			"horse",
			"u",
			&NaiveDateTime::from_timestamp(100,0),
			&[&100],
		);
	}
	client.commit().unwrap();

	client.begin_write().unwrap();
	{
		let mut adder = client.create_and_add().unwrap();
		adder.row(
			"horse",
			"u",
			&NaiveDateTime::from_timestamp(101,0),
			&[&101],
		);
	}
	client.rollback().unwrap();

	client.begin_write().unwrap();
	{
		let mut adder = client.create_and_add().unwrap();
		adder.row(
			"horse",
			"u",
			&NaiveDateTime::from_timestamp(101,0),
			&[&101],
		);
	}
	client.commit().unwrap();

	let r = client.read_series_range(
		"horse",
		&NaiveDateTime::from_timestamp(100,0),
		&NaiveDateTime::from_timestamp(200,0),
	).unwrap();
	assert_eq!(
		&format!("{:?}", r),
		"[(1970-01-01T00:01:40, [OwnedColumn { serialized: \"100\" }]), (1970-01-01T00:01:41, [OwnedColumn { serialized: \"101\" }])]"
	);
}
