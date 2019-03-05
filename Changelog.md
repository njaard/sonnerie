# 0.4.2: 2019-03-05
* don't forget blocks sometimes when "dump"ing.
* increase write-lock timeout substantially
* more logging when committing transactions

# 0.4.1: 2018-12-14
* Don't sometimes overwrite previously written data (!!!)
* Many performance improvements
* Save transactions correctly
* fsync periodically so that the OS's disk buffers don't get extremely large
* Do sqlite wal checkpointing from the checkpointing thread
* the "rollback" command should actually rollback (in the CLI client)
* Add many tests
* No incompatible changes, but an extremely important upgrade

# 0.4.0: 2018-11-25
* Optimize dump
* Increase write lock timeout
* Client no longer exits if you exit the pager
* Set sockets to nonblocking mode so server doesn't get stuck sometimes
* Improve write concurrency
* Name threads
* Protocol incompatibility: entire command now complete before errors are reported
* Add option to change metadata directory

# 0.3.4: 2018-11-12
* Don't store the wrong end-of-data offset

# 0.3.3: 2018-11-12
* Don't forget the end of my file, overwriting old
blocks in the process
* Optimize read transactions

# 0.3.2: 2018-11-10
* Change default block sizes
* Correct a very ugly corruption bug
* Improve timestamp parsing (thank you [https://github.com/barskern](barskern))

# 0.3: 2018-10-18
* Allow insertion not after the last sample in a series,
including between two samples and before the first sample, chronologically.
* Allow erasing a range of samples (`erase_range` and `erase_range_like`).
* Read by upper or lower bound: over many series, efficiently
read the first value that is before or after a specific timestamp.

# 0.2: 2018-09-17
* Change file format
* Support multiple columns per record
* Timestamps are unsigned nanoseconds since the Epoch.

# 0.1: 2018-09-07
Initial release
