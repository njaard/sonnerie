//! Read lines from a socket.
//!
//! This is necessary because a normal socket may block even
//! if there's a line available. This one will set the
//! socket to nonblocking and then block until a single
//! full line is available, but no more.

extern crate libc;

use std::os::unix::io::{RawFd,AsRawFd};
use std::io::{Read,BufRead,BufReader};
use std::io::ErrorKind::WouldBlock;

use std::net::TcpStream;
use std::os::unix::net::UnixStream;

use std::io::Result;

pub struct LineStream
{
	stream: BufReader<Box<Read>>,
	fd: RawFd,
}

impl LineStream
{
	pub fn new<S: 'static + NBSocket>(stream: S) -> Result<LineStream>
	{
		stream.set_nonblocking(true)?;
		let fd = stream.as_raw_fd();
		Ok(Self
		{
			stream: BufReader::new(Box::new(stream)),
			fd,
		})
	}

	fn wait(&self) -> Result<()>
	{
		unsafe
		{
			let mut fdset = std::mem::uninitialized();
			libc::FD_ZERO(&mut fdset);
			libc::FD_SET(self.fd, &mut fdset);
			libc::select(
				self.fd+1,
				&mut fdset as *mut libc::fd_set,
				std::ptr::null_mut(),
				std::ptr::null_mut(),
				std::ptr::null_mut(),
			);
		}
		Ok(())
	}
}

impl Read for LineStream
{
	fn read(&mut self, buf: &mut [u8]) -> Result<usize>
	{
		self.stream.read(buf)
	}
}


impl BufRead for LineStream
{
	fn read_line(&mut self, string: &mut String) -> Result<usize>
	{
		let mut sum = 0;
		loop
		{
			let e = self.stream.read_line(string);
			match e
			{
				Err(e) =>
				{
					if e.kind() == WouldBlock
					{
						self.wait()?;
					}
					else
					{
						return Err(e);
					}
				},
				Ok(c) =>
				{
					sum += c;
					if c == 0 || string.ends_with("\n")
						{ break; }
				},
			}
		}
		Ok(sum)
	}

	fn read_until(&mut self, byte: u8, buf: &mut Vec<u8>) -> Result<usize>
	{
		let mut sum = 0;
		loop
		{
			let e = self.stream.read_until(byte, buf);
			match e
			{
				Err(e) =>
				{
					if e.kind() == WouldBlock
					{
						self.wait()?;
					}
					else
					{
						return Err(e);
					}
				},
				Ok(c) =>
				{
					sum += c;
					if c == 0 || buf[buf.len()-1] == byte
						{ break; }
				},
			}
		}
		Ok(sum)
	}

    fn fill_buf(&mut self) -> Result<&[u8]>
    {
		self.stream.fill_buf()
    }
    fn consume(&mut self, amt: usize)
    {
		self.stream.consume(amt);
    }
}

pub trait NBSocket : Read+AsRawFd
{
	fn set_nonblocking(&self, nonblocking: bool) -> Result<()>;
}

impl NBSocket for TcpStream
{
	fn set_nonblocking(&self, nonblocking: bool) -> Result<()>
	{
		self.set_nonblocking(nonblocking)
	}
}

impl NBSocket for UnixStream
{
	fn set_nonblocking(&self, nonblocking: bool) -> Result<()>
	{
		self.set_nonblocking(nonblocking)
	}
}
