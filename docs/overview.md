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

## Supported Filtering Types

* `==` for 1, 2, 4, and 8-wide integers.
* `==` for strings.
* substrings in strings
* `&&` and `||` to combine filters (across columns)

#### Still Need to Figure Out, But want to Support

* `>` and `<` for numeric values
* Encoded integers (e.g., zigzag encoded ints)

## Filtering Techniques

* 1, 2, 4, and 8 byte match with vectors
* key/value neighborhood search: `if (found field name) -> check next value in neighborhood, skipping whitespace`

## Search Algorithm

```python
for each batch:
  if batch_number % SAMPLE_RATE == 0:
    # Use the batch to update model
    for each predicate:
      best_substrings.append(best_substring(predicate))

    # Sorts by signal strength, i.e., Match : Record Passed ratio.
    sort(best_substrings)

    # Check combinations here - want to find correlations between signals
    best_combo = check_combos(best_substrings)
    return best_combo

def best_substring(predicate):
  search using substrings of predicate and return best one

```

To check for combinations, we do the following:

1. When checking each substring, create a bit vector for each of the `p` predicate. The number of bits needed is the number of bits which *fail* the verifcation
in a given sample.

2. If a search string passes a failed record `i`, set bit `i` to 1.

3. We now have `p` bit vectors of length `l` (number of failed records in the sample). Pairwise for each combination, do:

   ```
   rate = popcnt(pA & pB)
   cost = len(predicate A) + len(predicate B)
   ```

`rate` is the approximate false positive rate with `pA` and `pB`; the cost is the cost of running the two predicates.

4. Use some scoring function `score(rate, cost)` (it can just be the  product of the above two, for example) to give the combination a score.

5. Pick `min(score)` as the filter - this is the filter which (according to the scoring function) filters out the most data for least cost.

The method operates over multiple batches at once using bitwise operations (e.g., a 64-bit integer can account for 64 samples at once - we can also use
vectors).

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
