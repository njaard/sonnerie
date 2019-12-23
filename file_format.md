TSDB's file format consists of "segments" of keys. Each segment has
uncompressed indexing data and then compressed the actual values ("Payload").

All numbers are stored as big endian.

All strings (keys) must be valid UTF-8.

# Segment header

* Each segment starts with `@TSDB_SEGMENT_\0\0` (yes, those are null bytes).
* Then four 32-bit numbers
  * the length in bytes of the first key in this segment
  * the length of the last key in this segment
  * the compressed length of the payload
  * the compressed length of the previous segment (include all headers)
* The first key in this segment (with a length of the first number above)
* the last key in this segment (with a length of the second number above)
* The LZ4-compressed payload. The compressed size is recorded in the header

The first key is always lexigraphically less than or equal to the last one.

# Payload
The payload stores all its keys as such:

* Four 32-bit numbers:
  * the length of the key
  * the length of the format string
  * the length of each value (which should correspond with the format string)
  * the length of data in bytes
* the key (a string of the above length)
* the format string (a string of the above length)
* the actual data, repeated instances of the timestamp stored as a 64-bit number
and the value for that timestamp.

So a record might look like this:
* `0x00000005` -- key length five
* `0x00000001` -- format length 1
* `0x00000004` -- record length 4
* `0x00000008` -- data length 12 ((timestamp=8 + record=4))*number of samples
* `abcde` - the key
* `u` - the format
* `0x1122334400000000` the timestamp
* `0x00001000` a 32-bit unsigned integer as specified by the format

# a segments-file
A file of segments contains a bunch of segments, each with their
complete header. The file of segments segments are sorted lexicgraphically
by key. A key must not span multiple segments, even if it results
in a really big segment.

Each segment's last key always comes lexigraphically before the following
segment's first key.

# How to search for a key in a segments-file

Do a binary search on the file itself, starting by taking the size of the file,
choosing a point near the middle and then scanning it until you find
the `@TSDB_SEGMENT_\0\0`. If you need to go backwards just a single
segment, then you can use that segment's header "the compressed length of the payload"
value to know how far to go back.

Once you find the segment that contains the key you're searching for
(because the key lives lexigraphically between the 'first key' and 'last key'
in the segment's header), you can decompress the LZ4 data and actually
get the values.

There's a bug in which if the compressed data actually magically contains
the `@TSDB_SEGMENT_\0\0`, then you might get messed up. That sucks man,
I feel bad for you, as it's really unlikely to ever happen.

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

It's logically acceptable, but not optimial for performance, if the new file
and the old file exist simultaneously, as they will, briefly.

# Format String
Is a string where each character is one of 'f', 'F', 'u', 'U', 'i', 'I'
corresponding to 32 or 64-bit float, unsigned integer, signed integer, respectively.
