//! Efficiently split lines by whitespace, while handling the backslash
//! escape sequences in Rust-like string format.
//!
//! For example, if you had a string like:
//! ```text
//!   One\ two three\ four
//! ```
//! Naïve splitting on whitespace would produce four outputs:
//! * `One\`
//! * `two`
//! * `three\`
//! * `four`
//!
//! This crate will instead produce two strings:
//! * `One two`
//! * `three four`
//!
//! This crate also handles special escape sequences like "\n", which represents a newline.
//! Specifically, the escape sequences are:
//!
//! * `\a`
//! * `\b`
//! * `\t`
//! * `\n`
//! * `\v`
//! * `\f`
//! * `\r`
//! * `\\`
//!
//! If the backslash character is found, but the successive character is not special,
//! then the backslash is disregarded and the successive character is included verbatim.

use std::borrow::Cow;

/// Splits once
///
/// Returns a tuple of the first "word" up until the first unescaped whitespace character,
/// and then every after the whitespace characters.
pub fn split_one_bytes<'a>(bytes: &'a [u8]) -> Option<(Cow<'a, [u8]>, &'a [u8])> {
	let mut start = 0usize;
	while let Some(b) = bytes.get(start) {
		if b.is_ascii_whitespace() {
			start += 1;
		} else {
			break;
		}
	}

	let mut owned: Option<Vec<u8>> = None;

	let mut position = start;

	while position < bytes.len() {
		if bytes[position] == b'\\' {
			if !owned.is_some() {
				owned = Some(bytes[start..position].to_owned());
			}
			let b = owned.as_mut().unwrap();
			position += 1;
			match bytes.get(position) {
				None => return None,
				Some(b'a') => b.push(b'\x07'),
				Some(b'b') => b.push(b'\x08'),
				Some(b't') => b.push(b'\t'),
				Some(b'n') => b.push(b'\n'),
				Some(b'v') => b.push(b'\x0b'),
				Some(b'f') => b.push(b'\x0c'),
				Some(b'r') => b.push(b'\r'),
				Some(b' ') => b.push(b' '),
				Some(b'\\') => b.push(b'\\'),
				Some(a) => b.push(*a),
			}
			position += 1;
		} else if bytes[position].is_ascii_whitespace() {
			break;
		} else {
			if let Some(o) = owned.as_mut() {
				o.push(bytes[position]);
			}
			position += 1;
		}
	}

	let mut after = position;
	while let Some(b) = bytes.get(after) {
		if b.is_ascii_whitespace() {
			after += 1;
		} else {
			break;
		}
	}

	let after = &bytes[after..];

	if let Some(owned) = owned {
		Some((Cow::Owned(owned), after))
	} else {
		Some((Cow::Borrowed(&bytes[start..position]), after))
	}
}

/// Split some text by unescaped whitespace.
///
/// find the first unescaped whitespace in `text`, return
/// a tuple of the text before the whitespace and the text after
/// the whitespace.
///
/// Ignores prefixed whitespace and discards whitespace between
/// the first portion and the text after the whitespace.
///
/// Returns None if there was an escape character and then nothing
///
/// Does not look at the following text at all.
pub fn split_one<'a>(text: &'a str) -> Option<(Cow<'a, str>, &'a str)> {
	if let Some((one, remainder)) = split_one_bytes(text.as_bytes()) {
		let one_text;
		match one {
			Cow::Borrowed(b) => {
				one_text = unsafe { Cow::Borrowed(std::str::from_utf8_unchecked(b)) }
			}
			Cow::Owned(b) => one_text = unsafe { Cow::Owned(String::from_utf8_unchecked(b)) },
		}
		let remainder = unsafe { std::str::from_utf8_unchecked(remainder) };
		Some((one_text, remainder))
	} else {
		None
	}
}

/// Produce all the words as a vector
pub fn split<'a>(mut text: &'a str) -> Option<Vec<Cow<'a, str>>> {
	let mut res = vec![];

	while !text.is_empty() {
		let s = split_one(text);
		if s.is_none() {
			return None;
		}
		let s = s.unwrap();
		res.push(s.0);
		text = s.1;
	}
	Some(res)
}

/// Converts text with all the special characters escape with a backslash
pub fn escape<'a>(text: &'a str) -> Cow<'a, str> {
	let bytes = text.as_bytes();

	let mut owned = None;

	for pos in 0..bytes.len() {
		let special: Box<[u8]> = std::ascii::escape_default(bytes[pos]).collect();
		if special.len() > 1 {
			if owned.is_none() {
				owned = Some(bytes[0..pos].to_owned());
			}
			owned.as_mut().unwrap().push(special);
		} else if let Some(owned) = owned.as_mut() {
			owned.push(bytes[pos]);
		}
	}

	if let Some(owned) = owned {
		unsafe { Cow::Owned(String::from_utf8_unchecked(owned)) }
	} else {
		unsafe { Cow::Borrowed(std::str::from_utf8_unchecked(bytes)) }
	}
}

#[cfg(test)]
mod tests {
	use escape;
	use split;
	use split_one;

	fn check(text: &str, one: &str, two: &str) {
		let a = split_one(text);
		let a = a.unwrap();
		assert_eq!(a.0, one);
		assert_eq!(a.1, two);
	}

	#[test]
	fn failure() {
		assert_eq!(split_one("abc\\"), None);
	}

	#[test]
	fn fine() {
		check("abc\\\\", "abc\\", "");
		check("1525824000000 520893", "1525824000000", "520893");
		check("abc\\\\ def", "abc\\", "def");
		check("abc\\\\\\\\ def", "abc\\\\", "def");
		check("abc\\\\\\\\    def", "abc\\\\", "def");
		check("abc\\ def   ghi", "abc def", "ghi");
		check("abc\\ def   ghi", "abc def", "ghi");
		check("", "", "");
		check(" ", "", "");
	}
	#[test]
	fn splitting() {
		assert_eq!(format!("{:?}", split("abc\\\\")), "Some([\"abc\\\\\"])");
		assert_eq!(
			format!("{:?}", split("abc def")),
			"Some([\"abc\", \"def\"])"
		);
		assert_eq!(format!("{:?}", split("abc\\ def")), "Some([\"abc def\"])");
	}

	#[test]
	fn escaping() {
		assert_eq!(escape("abc\ndef"), "abc\\ndef");
		assert_eq!(escape("abc\n def"), "abc\\n\\ def");
	}

	#[test]
	fn round_trip() {
		check(&escape("ads\nasd"), "ads\nasd", "");
	}
}
