

/// matches % as a wildcard operator
pub struct Wildcard
{
  w: String,
}

impl Wildcard
{
  pub fn new(w: &str) -> Wildcard
  {
    Wildcard
    {
      w: w.to_owned()
    }
  }

  pub fn prefix(&self) -> &str
  {
    if let Some(o) = self.w.find('%')
    {
      &self.w[0 .. o]
    }
    else
    {
      &self.w[..]
    }
  }

  /// returns true if this search can only match
  /// a single key
  pub fn is_exact(&self) -> bool
  {
    self.w.find('%').is_none()
  }

  /// Returns the regex that matches my wildcard,
  /// or None if the prefix is all that's needed
  pub fn as_regex(&self) -> Option<regex::Regex>
  {
    let mut re = String::with_capacity(self.w.len()+4);
    re += "^";

    let mut haspct = false;
    let mut needre = false;

    for c in self.w.chars()
    {
      if haspct { needre = true; }

      match c
      {
        '%' =>
        {
          re += ".*";
          haspct=true;
        }
        a @ '.' | a@'(' | a@')'
          | a@'{' | a@'}' | a@'\\' | a@'|'
          | a@'^' | a@'$'
          | a@'[' | a@']'
          =>
        {
          re.push('\\'); re.push(a);
        },
        a => re.push(a),
      }
    }

    re += "$";

    if needre || !haspct
      { Some(regex::Regex::new(&re).unwrap()) }
    else
      { None }
  }
}
