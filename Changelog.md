# 0.6.5: Unreleased
* Fix a critical bug in string fields, when multiple appear consecutively.
* The transaction filenames now permit 1M transactions per second

# 0.6.4: 2022-02-17
* Fix bug in which sometimes the latest version of a record did not take
precedence.
* Increase version of the depencency `parking_lot`
* Add exponential backoff when trying to create more than 1 transaction
per second. This is a workaround for how transaction filenames are based on
seconds and not a smaller unit.

# 0.6.3: 2021-11-29
* Remove `dbg!()` statement that worked its way into the code

# 0.6.2: 2021-11-25
* Fix writing certain malformed files in very rare cases, when a large
key is written near the size limit of a segment. Files written as such
cannot be read, but there's no data loss. Please file a ticket if you
need help correcting your files. It's unlikely that this bug is affecting
you, if it is, it shows as a panic such as `thread '<unnamed>' panicked at 'range
end index 1048643 out of range for slice of length 1048631', src/record.rs:119:10`.

# 0.6.1: 2021-09-09
* The command line tool's `--parallel` option now runs its command
inside of the shell specified by the environment variable `$SHELL`, instead of `sh`.
`sh` is still used as a fallback if `$SHELL` is not present.

# 0.6.0: 2021-08-18
* Columns don't need to keep the same type between samples anymore,
replacing "unsafe-unchecked" mode.
* There's a new file format
* Sonnerie 0.6 can read databases created with version 0.5, but only writes
0.6-format databases, therefor you will not be able to switch back to 0.5
after modifying a database. Doing a major compaction (`compact -M`) will
create a new 0.6-format database.
* A number of subtle bugs in String types are now fixed.
* Databases were slightly malformed in prior versions which may cause short
range reads to return less data than expected; a major compaction will losslessly
correct the data.
* `OwnedRecord` is renamed `Record` and has high-level functions for reading
the timestamp and values.
* `DatabaseKeyReader` now implements Rayon's [ParallelIterator](https://docs.rs/rayon/latest/rayon/iter/trait.ParallelIterator.html).
* The command line tool has these improvements to the `read` command:
	* The `--parallel` option was added; it can be used for partitioning the database.
	* The `--before` and `--after` options are renamed `--before-key` and
	`--after-key`.
	* There are now a `--before-time` and `--after-time` options
	which can filter on timestamps.
* Reading is now about 30% faster, independent of the new parallelism.
* There are fewer submodules that are directly `pub`-exported. The remaining
modules, `formatted` and `row_format` may be removed in a later version.

# 0.5.9: 2020-08-27
* Set the correct permissions on new transactions

# 0.5.8: 2020-07-25
* Fix when --before is used without --after
* Never create .tmp files (use anonymous files) on Linux

# 0.5.7: 2020-07-23
* the "read" cli command has --before and --after, which allows
filtering on ranges and not just prefix
* dependencies were updated

# 0.5.6: 2020-03-05
* When adding to an empty database, replace `main`.

# 0.5.5: 2020-03-05
* (yanked)

# 0.5.4: 2020-01-23
* Fix multicolumn rows
* Add support for string types

# 0.5.3: 2020-01-17
* Add formatting options to the `sonnerie read` command.
* Unify formatted output functions and deprecate parts of the API

# 0.5.2: 2020-01-10
* `PUT` works again in sonnerie-serve

# 0.5.1: 2020-01-08
* Public API is now roughly sensible
* Add reasonably complete API documentation
* Switch to tokio 0.2
* Now you can disable optional features to deactivate binaries and have a lot fewer dependencies.

# 0.5.0: 2019-12-23
* Sonnerie was rewritten from the ground up and is totally incompatible with previous versions.

# Previous versions
See github history.
