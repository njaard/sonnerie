Sonnerie's file format consists of "segments" of keys. Each segment has
uncompressed indexing data and then compressed the actual values ("Payload").

Fixed-length numbers are stored big-endian.

Varints use `unsigned-varint`-encoding.

All numbers are stored as big endian.

All strings (keys) must be valid UTF-8.

# Segment header

* Each segment starts with `@TSDB_SEGMENT_`
* Then two bytes indicating the segment version. The current version is 0x0100. You'll
have to look at older versions of `file_format.md` to see documentation for previous versions.
* Then five varints
  * the length in bytes of the first key in this segment
  * the length in bytes of the last key in this segment
  * the stored length of the payload
  * the stored length of the previous segment (meaning compressed, including all headers)
  * the number of bytes of all previous segments that contain data for first_key, or 0
  if this is the first one.
* The first key in this segment (with a length of the first number above)
* the last key in this segment (with a length of the second number above)
* The LZ4-compressed payload. The compressed size is recorded in the header.
If `@TSDB_SEGMENT_` is in the _compressed_ data, then it is replaced with "`@TSDB_SEGMENT_\xff\xff`".

The first key is always lexigraphically less than or equal to the last one.

# Payload
The payload stores all its keys as such:

* Four 32-bit numbers:
  * the length of the key
  * the length of the format string
  * the length of the "actual data" in bytes for this key
* the key (a string of the above length)
* the format string (a string of the above length)
* the "actual data", repeated instances of for each timestamp:
  * If the format string is not of a fixed size (it contains strings),
  store a varint of the entire record length, not including timestamp.
  * The timestamp stored as an 8-byte integer.
  * The value for each column as specified in the format. If a column is
  a string, store the string's length as a varint and then the string.


# A segments-file
A file of segments contains a bunch of segments, each with their
complete header. The file of segments' segments are sorted lexicgraphically
by key. A key may span multiple segments. Sonnerie chooses a reasonable approximate
maximum segment size.

Each segment's last key always comes lexigraphically before or equal to
the following segment's first key.

# How to search for a key in a segments-file

Do a binary search on the file itself, starting by taking the size of the file,
choosing a point near the middle and then scanning it until you find
the `@TSDB_SEGMENT_`. If you need to go backwards just a single
segment, then you can use that segment's header "the compressed length of the payload"
value to know how far to go back.

Once you find the segment that contains the key you're searching for
(because the key lives lexigraphically between the 'first key' and 'last key'
in the segment's header), you can decompress the LZ4 data and actually
get the values.

# The database format
The database is a directory with a bunch of these segments-files where
one of them is "main" and the rest start with "tx." but do not end in ".tmp".

To get all data you must do a sorted merge on all of those files which
requires you to "search for a key in a segment" on every one of those files,
and then give precedence to the data in the file whose name
is lexically last (that allows us to replace old values easily).

That means that if you have a key "abcd" and it appears in multiple files,
you have to do a sorted-merge on all "abcd" values. If a single timestamp
on that key appears in multiple segments-file, you choose the values
associated with that timestamp from the filename that is lexigraphically last.

# File format design notes
Every time you do a binary search on a file, you do it the same way,
so the OS's disk cache will keep the first few steps in memory. This means
that the first few steps of a search for any arbitrary key will not require
any disk accesses!

# Compacting
Do a sorted-merge on a group of files, and then create a new file, after which
time you can delete all of the members of that group of files.

It's logically acceptable, but not optimal for performance, if the new file
and the old file exist simultaneously, as they will, briefly.

# Format String
Is a string where each character is one of 'f', 'F', 'u', 'U', 'i', 'I'
corresponding to 32 or 64-bit float, unsigned integer, signed integer, respectively.

A character may also be 's', which means that the column stores a string
of a non-fixed length. Storage of the actual data uses the "non-fixed length" storage
which includes some varints for length.
