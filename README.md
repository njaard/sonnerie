[![GitHub license](https://img.shields.io/badge/license-BSD-blue.svg)](https://raw.githubusercontent.com/njaard/sonnerie/master/LICENSE)
[![Crates.io](https://img.shields.io/crates/v/sonnerie.svg)](https://crates.io/crates/sonnerie)
[![docs](https://img.shields.io/badge/docs-api-green)](https://docs.rs/sonnerie)

Refer to the [Changelog](Changelog.md) for information on releases.

# Introduction

Sonnerie is a time-series database. Map a string to a list of timestamps and value.
Store multiple of these series in a single database. Insert tens of millions
of samples in minutes, on rotational media or solid-state.

Sonnerie is optimized for storing data that comes in as
many values over many series (insertion of millions of items takes minutes and
doesn't block other readers or writers), and for reading one series at a time in
10s of milliseconds. It is also very good at dumping lexicographically sequential
series (which means: everything).

Sonnerie can very efficiently do random insertions and updates, and works
well for huge databases. Due to the compact disk format, sparse data such
as keys with only a few timestamps can be very efficiently stored.

Sonnerie is mostly intended for on-disk archival, realtime updates, and realtime accesses
of individual series. For analytical purposes, one would load the necessary data
into memory and process it through other means.

# Features
* A straight-forward protocol for reading and writing
* Easy setup: insert data on the command line.
* No query language
* Transactional: a transaction is completely committed or not at all.
* Isolated: A transaction doesn't see updates from other transactions or expose its changes until it has been committed.
Note the semantics of "last record wins" - if two transactions write two values with same key and timestamp, then the record from
the last completed transaction will be the retained one.
* Durable: committed data is resistant to loss from unexpected shutdown.
* Nanosecond-resolution timestamps (64 bit), 1970-2554
* No weird dependencies, no virtual machines, one single native binary for the command line tool
* Floating point, integer, and string values, multiple columns per sample
* Concurrent reading of ranges with Rayon - do a "map-reduce" style query from Rust
in 30 seconds per billion records per core on modern hardware.

Sonnerie runs on Unix-like systems and is developed on Linux.

# Quick Start

## Install

Sonnerie is implemented in Rust, a systems programming language that runs
blazingly fast. Installation from source therefor requires you to
[install the rust compiler](https://www.rust-lang.org/en-US/install.html),
which is as simple as: `curl https://sh.rustup.rs -sSf | sh`.

Sonnerie can then be installed from Cargo: `cargo install sonnerie`.

Sonnerie consists of one executable, `sonnerie` (`~/.cargo/bin/sonnerie`)

## Create a database

Create a database by creating a directory and an empty file named "`main`":

	mkdir database
	touch database/main

## Insert data
	echo -e "\
	fibonacci 2020-01-01T00:00:00 1
	fibonacci 2020-01-02T00:00:00 1
	fibonacci 2020-01-03T00:00:00 2
	fibonacci 2020-01-04T00:00:00 3
	fibonacci 2020-01-05T00:00:00 5
	fibonacci 2020-01-06T00:00:00 8" \
	| sonnerie -d database/ add --format u --timestamp-format=%FT%T

If the "add" command succeeds, then the transaction is committed to disk.

Items added with `sonnerie add` must be sorted lexicographically by their
key and then chronologically. This requirement does not exist in
`sonnerie-serve`.

## Read the data back

	sonnerie -d database/ read %

The `%` is a wildcard as is used in "`LIKE`" in SQL and filters
on the key. Searching based on a prefix is very efficient:

	sonnerie -d database/ read fib%

Sonnerie outputs the matched values:

	fibonacci 2020-01-01 00:00:00     1
	fibonacci 2020-01-02 00:00:00     1
	fibonacci 2020-01-03 00:00:00     2
	fibonacci 2020-01-04 00:00:00     3
	fibonacci 2020-01-05 00:00:00     5
	fibonacci 2020-01-06 00:00:00     8

## Delete records

	sonnerie -d database/ delete --after-time=2020-01-04

Instantaneously removes all values at the specified time and later, also available
is `--before-time` and similar functions for filtering by key range.

The data is immediately removed from the database. A later compaction will
purge it and recover disk space.

Deletions merely suppress output; reading a sequence of records that
contain the deleted items will still have to iterate through the deleted records.
You should thus compact (with --major) to not pay that penalty.

# Usage

## Row format
Each series has a **`format`**. The format is specified as a
bunch of single character codes, one for each value.

The character codes are:
* `f` - a 32 bit float (f32)
* `F` - a 64 bit float (f64)
* `u` - a 32 bit unsigned integer (u32)
* `U` - a 64 bit unsigned integer (u64)
* `i` - a 32 bit signed integer (i32)
* `I` - a 64 bit signed integer (i64)
* `s` - a UTF-8 encoded string type. When strings are outputted, they are
encoded in "backslash escaped" form, so all whitespace and backslashes are
preceded by a backslash.

In the above "fibonacci" example, we're using the "u" format.

Multi-column rows are permitted; for two floating point values representing
latitude and longitude:

	oceanic-airlines 2018-01-01T00:00:00 ff 37.686751 -122.602227
	oceanic-airlines 2018-01-01T00:00:01 ff 37.686810 -122.603713
	oceanic-airlines 2018-01-01T00:00:02 ff 37.686873 -122.605997
	oceanic-airlines 2018-01-01T00:00:03 ff 37.687022 -122.609997
	oceanic-airlines 2018-01-01T00:00:04 ff 37.687364 -122.610945
	oceanic-airlines 2018-01-01T00:00:05 ff 37.687503 -122.615211

## Sonnerie allows heterogeneous formats.
A single key may change its format, for example:

	keyname 2020-01-01T00:00:00 u 42
	keyname 2020-01-02T00:00:00 f 3.1415
	keyname 2020-01-03T00:00:00 s Now\ a\ string

While a key may change its format, it has more storage overhead,
so it's best to not allow keys to oscillate between types.

This is permitted new in version 0.6, older versions had an "unsafe" mode
that allowed the test to be bypassed for performance.

## No server is necessary

All actions can be done by running `sonnerie -d /path/to/data/`. Furthermore,
a file, (after it gets its ".tmp" suffix removed) will never change, though
the files may sometimes get replaced. This means you can
replicate a database by hardlinking all the files (`ln`).

## The database must be compacted

On a regular (possibly daily) basis, you must compact the database. This
rolls a bunch of transaction files into a single large transaction file.
This is important for performance. By the time about 100 transaction files
are present, performance suffers greatly. Therefor, compact the database
at approximately the rate necessary to prevent that.

There are two types of compactions, a major and a minor one. A major
one replaces the entire database, which requires reading
and rewriting the entire database. A minor one replaces all of the transaction
files with a single new transaction file. This is a lot faster because it
requires only reading and rewriting the contents of the transaction files
and not the `main` file.

A major compaction is accomplished with:

    sonnerie -d /path/to/data/ compact --major

And a minor compaction:

    sonnerie -d /path/to/data/ compact

Compacting doesn't block readers or writers, but only one can
happen at any given moment, so a lock is placed to prevent multiple
concurrent compactions.

Compactions are atomic, so you can cancel it (with `^C`) at any time.

## You can compact and filter

In case some data in the database needs to be modified, you can use
`compact` with the `--gegnum` option. Gegnum means "through" in Icelandic.

This command removes records that start with `bad-objects`:

    compact --major --gegnum 'grep -v ^bad-objects'

Do a normal compaction, but also count records:

    compact --major --gegnum 'pv -l'

The `--gegnum` runs its command inside a /bin/sh, so pipelines work. Filter
out bad objects AND modify the names of other objects:

    compact --major --gegnum 'grep -v ^bad-objects | sed "s/^old-name/new-name/"'

The outut of the `--gegnum` command must be in sorted order.

You can also see a preview of its output by piping your command into `| tee /dev/stderr`.

Note that the rows come as "key\ttimestamp\tformat\tvalue"

You can also "read | filter | add" into a different database, but `gegnum` allows
you to modify an existing database which is useful for online maintenance on a database
that gets concurrent updates.

# sonnerie-serve
A server is provided so that you can conveniently read and write to the database
via HTTP.

Run `sonnerie-serve -d /path/to/database/ -l 0.0.0.0:5555` and then you may
make `PUT` and `GET` requests:

* Read the named series:

	`curl http://localhost:5555/fibonacci`

(The response is the entire series in a format similar to `sonnerie read`)

* Read series by wildcard:

	`curl http://localhost:5555/fib%`

(The response is each series, in alphabetical order, in a format similar to
`sonnerie read`)

* Output human-readable timestamps:

	`curl http://localhost:5555/fib%?human`

(The timestamps are in ISO-8601 instead of nanoseconds)

* Add more data:

	`curl -X PUT http://localhost:5555/ --data-binary 'fibonacci 1578384000000000000 u 13'`

(`200 OK` means that the transaction was committed)

Unlike `sonnerie add`, `sonnerie-serve` allows unsorted input.

Note that because sonnerie `mmap`s its files, sonnerie-serve will show
huge values for its virtual memory usage (`VIRT` in top), but actual
memory utilization will be reasonable.

You may continue to read and modify your sonnerie database by the command
line or even via another concurrently-running `sonnerie-serve`s.

An alternate approach is to use "sshfs" to mount the database remotely. This
approach is very performant because only compressed data goes through the network
and the server doesn't need to do any of the decompressing. Avoid nfs
because compactions will cause files to get deleted, and then the client will get an
IO error, as NFS cannot track files that are closed on the server.

# Contributing
Bug reports and pull requests are always welcome no matter how big or small.
Development of Sonnerie is people-first and we comply with Rust's 
[Code of Conduct](https://www.rust-lang.org/policies/code-of-conduct).

If you use Sonnerie, please provide feedback!

# Sonnerie is used in production
Sonnerie is used by Headline with a >100GiB database and 10s
of billions of rows.

# Performance
An approximate average lookup latency for a random key in a large database is
around 15ms on an SSD and much slower on a busy rotational media device. Sequential
access (i.e., reading the whole database in lexicographical order) is somewhere around
2k keys/sec and 3M records/sec, very much depending on the data itself.

# Copyright

Sonnerie was implemented by Charles Samuels at
[Headline](https://headline.com).
