# Current Workflow

Input Data in on-Disk Format (with Predicates Pushed Down) -> Parsed Data -> Filter Remaining -> Process

# Filters

The filters attempt to filter data based on a predicate quickly. This is
probably most interesting for nested data instead of individual values.

Most filters will apply a fast filter to a predicate set and then fall back to
a standard slow filter if the fast filter produces returns positive (i.e., the filter
passes). The fast filter turns off if the selectivity is low.

High level strategy should be to spend < 1 cycle/byte, amortized over the entire scan. 
Speedup increases as dataset size increases.

E.g., first 10 MB can be 2x slower, but next 4 GB are 10x faster.

Example Query:

```
tweet.lang == 'en' &&
tweet.created_at contains 2017 &&
tweet.user.followers_count > 50 &&
tweet.user.lang == 'en' &&
tweet.text contains 'united' &&
tweet.hashtags contains 'B747'
```

## Filtering Types

* `==` for strings
* `>`, `<` for numeric values
* substrings
* ranges (dates, numbers, prefixes)
* `&&` and `||` to combine predicates (across columns)

## Format-Independent Techniques

* Filter order (want to apply less expensive, more selective filters first)
* For many predicates, any way to combine the comparison or check to make it faster?
* For nested predicates, any way to do the same (e.g., retrieving some nested thing faster)
* Can information about the position of each field be stored while traversing the data? What's efficient?
* Structures for holding indexing information (relevant for column-oriented stuff?)
* Parallel approximate substring search (see `search.md`)

## Format-Specific Techniques

Techniques specific to each format.

### CSV (Structured Text Based)

Main bottlenecks:
* Finding delimiters
* Seeking to relevant column
* Deserializing integers and dates

Techniques:

* Stop parsing early (Always apply filters left -> right)
* AVX Instructions to find delimiters (HyPer Line Rate Bulk Loading)
* Positional indices (NoDB)
* Faster text deserialization

### JSON (Semi-structured Text Based)

Main Bottlenecks:
* Finding Field (Esp. with nesting)
* Deserializing Text

* `==` for integers: Abort early if parse fails (e.g., for parsing "1234" and predicate "1456", can stop parsing at second byte)

### Parquet (Columnar)

* Need to consider different byte array encodings (e.g., dictionary encoding).
* Check lengths before checking full string if available (e.g., in Parquet)

### Avro (Row-based Binary)

* Skipping to relevant fields directly
* Early abort (pushing predicates into parser)
* Something with nesting?

# Story

* fast algorithms for processing data (machine learning, joins, etc.)
* but lots of time goes into preparing data for processing
* loading and filtering
* Network and Disk speeds: approaching 40Gbps line rates
* Fastest parsers reach maybe 1GBps on modern hardware
* many of these parsers look at data unnecessarily: expensive filters, etc.
* sparser: system to load data at near line-rate
