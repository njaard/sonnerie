//! Parse SQL "`LIKE`"-like filters.

/// matches % as a wildcard operator
pub struct Wildcard {
	w: String,
}

impl Wildcard {
	/// Parse a wildcard filter. All strings are valid, so never fails.
	pub fn new(w: &str) -> Wildcard {
		Wildcard { w: w.to_owned() }
	}

	/// Returns the shortest possible fixed prefix.
	///
	/// If no "%" is in the filter, then the entire string is returned,
	/// Otherwise everything up to the "%" is returned, which may
	/// be an empty string.
	pub fn prefix(&self) -> &str {
		if let Some(o) = self.w.find('%') {
			&self.w[0..o]
		} else {
			&self.w[..]
		}
	}

	/// returns true if this search can only match a single key.
	///
	/// This only happens when there is no "%" in the filter.
	pub fn is_exact(&self) -> bool {
		self.w.find('%').is_none()
	}

	/// Returns the regex that matches my wildcard.
	///
	/// Returns None if the prefix is all that's needed, even
	/// if it's still a wildcard.
	///
	/// "prefix%suffix" returns `Some` but `prefix%` returns
	/// `None`.
	pub fn as_regex(&self) -> Option<regex::Regex> {
		let mut re = String::with_capacity(self.w.len() + 4);
		re += "^";

		let mut haspct = false;
		let mut needre = false;

		for c in self.w.chars() {
			if haspct {
				needre = true;
			}

			match c {
				'%' => {
					re += ".*";
					haspct = true;
				}
				a @ '.'
				| a @ '('
				| a @ ')'
				| a @ '{'
				| a @ '}'
				| a @ '\\'
				| a @ '|'
				| a @ '^'
				| a @ '$'
				| a @ '['
				| a @ ']' => {
					re.push('\\');
					re.push(a);
				}
				a => re.push(a),
			}
		}

		re += "$";

		if needre || !haspct {
			Some(regex::Regex::new(&re).unwrap())
		} else {
			None
		}
	}
}
