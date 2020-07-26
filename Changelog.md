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
