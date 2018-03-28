# Demo REPL

A demo REPL that shows how to use sparser.

The typical way to use the system is:

1. Use `decompose` to get a set of RFs from a list of string tokens.

2. Use `sparser_calibrate` to get a `sparser_query_t`. This represents a
   schedule that the system uses to search the input.

3. Use `sparser_search` to perform the actual search.

Users can pass a callback (a function pointer) to both functions. The callback
returns a non-zero value if  a filter passes in `sparser_calibrate`. In
`sparser_search`, the callback can perform any user-defined action (e.g.,
processing the record, storing a pointer to the record for later processing,
etc.). For example, the first example query here just counts the number of
passing records after applying a full JSON parser.
