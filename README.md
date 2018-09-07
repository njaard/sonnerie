# Introduction

Sonnerie is a time-series database. Map a timestamp to a floating-point
value. Store multiple of these series in a single database. Insert
tens of millions of samples in minutes, on rotational media.

## Features

* A straight-forward protocol for reading and writing
* Easy setup: insert data with "netcat" or "telnet" in 5 minutes
* No query language
* Transactional: a transaction is completely committed or not at all.
* Isolated: A transaction doesn't see updates from other transactions or
expose its changes until it has been committed.
* Durable: committed data is resistant to loss from unexpected shutdown.
* Millisecond-resolution timestamps (64 bit)
* No weird dependencies, no virtual machines, one single native binary

Sonnerie runs on Unix-like systems and is developed on Linux.

## Performance

Sonnerie is designed to accept millions of samples across disparate
series quickly, and then later fetch ranges from individual series.
Memory is used for write-combining, write-ahead-logs are used to
keep commits fast while still durable.

Fundamentally, the database is append-only. Edits and insertions
are costly (and not yet implemented!).

## Why

You intake a lot of samples related to different entities all
at once, and then want to read a lot of data for a entity, the
disk usage patterns become very different

Timestamp           | Entity 1 | Entity 2 | Entity 3
------------------- | -------- | -------- | --------
2000-01-01 00:00:00 |  50.0    |   23.0   |  95.3
2000-01-02 00:00:00 |          |   24.0   |
2000-01-03 00:00:00 |  51.5    |   25.0   |
2000-01-04 00:00:00 |  53.0    |   26.0   |  94.8

At each timestamp (row), you insert some samples (it can be millions).

Some time later on, you want to run an analysis on a single Entity, Sonnerie
allows one to quickly access all its values (an entire column).

# Quick Start

## Install

Sonnerie is implemented in Rust, a systems programming language that runs
blazingly fast. Installation from source therefor requires you to
[install the rust compiler](https://www.rust-lang.org/en-US/install.html),
which is as simple as: `curl https://sh.rustup.rs -sSf | sh`.

Sonnerie can then be installed from Cargo: `cargo install sonnerie`.

Sonnerie consists of one executable, `sonnerie` (`~/.cargo/bin/sonnerie`)

## Run

Just run Sonnerie, `sonnerie start -d <database directory to use>`.

Sonnerie is running in the background, listening on `[::1]:5599` for connections.

## Insert data

Start the Sonnerie client:

    sonnerie client

Start a transaction:

    begin --write

Create a series:

    create fibonacci

Add a few values to the series

	add fibonacci 2018-01-01T00:00:00 1
	add fibonacci 2018-01-02T00:00:00 1
	add fibonacci 2018-01-03T00:00:00 2
	add fibonacci 2018-01-04T00:00:00 3
	add fibonacci 2018-01-05T00:00:00 5
	add fibonacci 2018-01-06T00:00:00 8

Read some of those values back:

	read fibonacci -f 2018-01-03 -t 2018-01-06

Sonnerie replies with:

    2018-01-03 00:00:00     2
    2018-01-04 00:00:00     3
    2018-01-05 00:00:00     5
    2018-01-06 00:00:00     8

Commit the transaction:

	commit

After `commit` completes, the data is definitely on disk.

Try `help` and `read --help` (or `--help` with any command)
for more information.

# Errata

## The protocol

Telnet into Sonnerie (`telnet ::1 5599`) and type "help" to see what you can
do. The protocol is text-based and very similar to the client frontend.

Commands use shell-like escaping, so spaces can be escaped with
a backslash. Timestamps are milliseconds since the Unix Epoch.

The protocol formats floats with enough precision such that
they can represent themselves exactly.

## Fast imports

In order to ensure durability, many `fsync`s need to be called (a few per
transaction). This can slow down imports. You should consider running `sonnerie`
prefixed with [`eatmydata`](https://packages.debian.org/stretch/eatmydata),
which is a Debian package. It will temporarily suppress fsync. After
your import is done, start Sonnerie again normally.

When doing your inputs, tweak the size of the transaction until you find
the optimal size. This might be a megabyte or so of data.

## Backup

Online *incremental* backups are possible (the file format is
designed accordingly) but not yet implemented.

You can do a *full* online backup as such, maintaining the following order:

    mkdir dst
    sqlite3 src/meta .dump | sqlite3 dst/meta
    cp src/blocks dst/blocks

(This method will no longer apply once compacting is implemented).

## Editing

Modifying existing data will be implemented shortly. It will result
in wasted disk space and increased fragmentation.

## Disk usage

Each sample requires 16 bytes on average plus small amounts of metadata.

# Roadmap

* Online incremental backups
* Compacting
* Compression
* Old blocks can be compressed and unused space (including that
caused by edits and insertions) compacted away.
* An HTTP-based protocol
* Most recent values
* Store other fixed-size data (multiple floats per timestamp)
* Store variable-sized data (a string or blob per timestamp)

# Copyright

Sonnerie was implemented by Charles Samuels at
[e.ventures Management LLC](http://eventures.vc).

