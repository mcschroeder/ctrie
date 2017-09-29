0.2 (September 2017)
--------------------
* Change return types of `insertIfAbsent`, `insert` and `delete` to indicate whether or not a value has been inserted/deleted. This is a breaking change, but it should be trivial to adapt your code. There are no changes in performance. Thanks to Tom Shackell for proposing this change.

0.1.1.0 (April 2016)
--------------------
* Add `insertIfAbsent` (https://github.com/mcschroeder/ctrie/pull/2)

0.1.0.3 (December 2015)
-----------------------
* Eliminate a redundant import warning on GHC 7.10
* Loosen dependency bounds

0.1.0.2  (October 2014)
-----------------------
* Use newer versions of `base` and `atomic-primops`
* Update benchmarks for `criterion 1.0`
