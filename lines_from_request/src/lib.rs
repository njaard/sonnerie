//! Create an iterator of lines from a hyper::Body

use hyper::{Body, error::Error};
use std::collections::VecDeque;

use futures::stream::Stream;

pub fn lines(body: Body) -> Lines
{
	Lines
	{
		body: body.wait(),
		buffer: VecDeque::new(),
		done: false,
	}
}

pub struct Lines
{
	body: futures::stream::Wait<Body>,
	buffer: VecDeque<u8>,
	done: bool,
}

impl Iterator for Lines
{
	type Item = Result<Vec<u8>, Error>;

	fn next(&mut self) -> Option<Self::Item>
	{
		while !self.done || self.buffer.len()>0
		{
			// check if we already have a nl
			let nlpos = self.buffer.iter().enumerate().find(|&(_,&a)| a==b'\n')
				.map(|(i, _)| i);
			if let Some(nlpos) = nlpos
			{
				let next_line = self.buffer.drain(0 ..= nlpos).take(nlpos).collect();
				return Some(Ok(next_line));
			}
			else if self.done
			{
				// no new line, but we're at the end of the object
				let next_line = self.buffer.drain(..).collect();
				return Some(Ok(next_line));
			}

			// get more data
			if let Some(chunk) = self.body.next()
			{
				if let Err(e) = chunk
				{
					return Some(Err(e));
				}
				let chunk = chunk.unwrap();
				self.buffer.extend( chunk.iter() );
			}
			else
			{
				self.done = true;
			}
		}

		None
	}
}
