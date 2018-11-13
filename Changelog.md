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
