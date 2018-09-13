extern crate tempfile;

use std::process::{Stdio,Command, Child};
use std::os::unix::net::UnixStream;

use std::io::{Read,Write,BufReader,BufRead,BufWriter};

struct Instance
{
	_dir: tempfile::TempDir,
	service: Child,
	read: BufReader<Box<Read>>,
	write: BufWriter<Box<Write>>,
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

		Instance
		{
			_dir: t,
			service: service,
			read: BufReader::new(Box::new(conn) as Box<Read>),
			write: BufWriter::new(Box::new(conn2)),
		}
	}

	fn check_transcript(&mut self, script: &str)
	{
		let mut line = String::new();
		self.read.read_line(&mut line).unwrap();
		assert_eq!("Greetings from Sonnerie\n", line);

		for l in script.lines()
		{
			let l = l.trim();
			line.clear();
			if l == "*"
			{
				self.read.read_line(&mut line).unwrap();
			}
			else if l.starts_with(">")
			{
				self.read.read_line(&mut line).unwrap();
				let line = line.trim();
				assert_eq!(line, l[1..].trim());
			}
			else
			{
				writeln!(&mut self.write, "{}", l).unwrap();
				self.write.flush().unwrap();
			}
		}

		writeln!(&mut self.write, "exit").unwrap();
		self.write.flush().unwrap();
		line.clear();
		self.read.read_line(&mut line).unwrap();
		let line = line.trim();
		assert_eq!(line, "");
	}
}

#[test]
fn protocol_just_connection()
{
	Instance::new().check_transcript("\n");
}

#[test]
fn protocol_basic1()
{
	Instance::new().check_transcript("
		begin write
		*
		create horse F
		*
		add1 horse 1000 55
		*
		read horse 0 1000
		> 1000\t55.00000000000000000
	");
}

#[test]
fn protocol_basic2()
{
	Instance::new().check_transcript("
		begin write
		*
		create horse FF
		*
		add1 horse 1000 55 44
		*
		read horse 0 1000
		> 1000\t55.00000000000000000 44.00000000000000000
	");
}

#[test]
fn protocol_basic3()
{
	Instance::new().check_transcript("
		begin write
		*
		create horse ii
		*
		add1 horse 1000 55 -44
		*
		read horse 0 1000
		> 1000\t55 -44
	");
}

#[test]
fn protocol_basic4()
{
	Instance::new().check_transcript("
		begin write
		*
		create horse iu
		*
		add1 horse 1000 55 4000000000
		*
		read horse 0 1000
		> 1000\t55 4000000000
	");
}

#[test]
fn protocol_basic5()
{
	Instance::new().check_transcript("
		begin write
		*
		create horse iu
		*
		add horse
		999 50 3000000000
		1000 55 4000000000

		*
		read horse 0 1000
		> 999\t50 3000000000
		> 1000\t55 4000000000
		>
	");
}

#[test]
fn protocol_basic6()
{
	Instance::new().check_transcript("
		begin write
		*
		create horse fu
		*
		add horse
		999 50 3000000000
		1000 55 4000000000

		*
		read horse 0 1000
		> 999\t50.00000000000000000 3000000000
		> 1000\t55.00000000000000000 4000000000
		>
		read horse 0 999
		> 999\t50.00000000000000000 3000000000
		>
	");
}
