## Contributions

1. Cascading search technique to reduce complex, expensive-to-evaluate predicates into simpler ones with false positives. Search technique applies a number of "fuzzy" filters (e.g., comparing substrings, encoded ints, etc.).

2. Query scheduler which adaptively tunes the search and schedules filters and projections dynamically, while picking filters to apply for deserialization, etc. Schedule is driven by a probability model which estimates the costs of running filters.

3. Evaluation of the system on different data formats (JSON - an unstructured text-based format, CSV, a structured text-based format, and Parquet, a binary, structured columnar format), showing speedups of up to **Nx** for JSON and **Nx** on Parquet for loading data, and speedups of up to **Nx** when integrated with a distributed system (Spark)


## Evaluation Outline

### Questions the evaluation answers:

1. How does the system perform on parsing/data loading workloads? (microbenchmarks)

* Loading throughput, Zakir Queries, Twitter Queries (for Parquet and JSON)
* CSV for TPC-H?
* Vary selectivity for Twitter Data (1-100%) -> Retrive 1 field to full object
* Vary selectivity for Github Data (1-100%) -> Retrieve 1 field to full object (Github fields are ordered)

2. How much does the system speed up distributed frameworks such as Spark? (integration into distributed framework)

* End to end throughput, Zakir queries 1-4, TPC-H (all?) 

3. How much does each filter and technique help (lesion study)

---

### Graphs

#### Page One Motivation Plots

1. Clustered bar chart showing breakdown of runtime for queries in Spark (convinces reader that parsing takes a long time)
2. "Donald Trump" vs. "ld T" (convinces reader that data parallelism gives you speedup and substrings are good indicators of queries)

#### Sparser Performance

1. Zakir Query 1-4 - RapidJSON vs. Sparser

 * Clustered bar chart, x-axis is query, y-axis is GB/s
 * Shows performance compared to a state-of-the-art JSON
 * Can also compare GB/s to Mison (no source code available)

2. Twitter JSON Data - Sparser performance

 * Line Graph, each line is Selectivity, x-axis is Parsed Fields, y-axis is GBps (Figure 10 from Mison)
 * Shows how performance is affected by selectivity, parsed fields, etc.

3. Twitter Parquet Data - Sparser performance

 * Line Graph, each line is Selectivity, x-axis is Retrieved Fields, y-axis is GBps (Figure 10 from Mison)
 * Shows that filtering still has an effect even on columnar data formats

4. TPC-H CSV Data - Sparser Peformance

 * Line Graph, each line is Selectivity, x-axis is Retrieved Fields, y-axis is GBps (Figure 10 from Mison)
 * Shows that the technique is applicable to CSV and the deserialization filters help accelerate these workloads.s

#### Scheduler Performance

4. Zakir Query 1 - 4 - Runtime for choosing different filters

 * Table organized as follows:

 | Query | Ideal Runtime | Worst Runtime | Sparser Runtime |
 | --- | --- | --- | --- |

 *  Shows effects of choosing filters matters, and that picking the correct filter matters

 5. Zakir Query 1-4: Filter Lesion Study

 * Take ideal search string for each query. Show false positive rate for 1B, 2B, 4B, and 8B for each
 * shows ideal Query length (processing time per query) vs. accuracy
 * TODO this might not be the exact correct thing to show

 6. Zakir Query 1-4: Scheduling projections and filters

 * Bar graph. Show (1) all filters first, (2) all projections first, (3) sparser - dynamically interleaving them
 * Shows that scheduling these gives better performance in most cases
 * Show for both Parquet and JSON - shouldn't matter as much for parquet.

#### Spark Integration

7. Zakir Query 1 - 4: Spark vs. Sparser+Spark

* Bar graph with end to end job completion time
* Breakdown of parse vs. processing
* Show for Parquet + JSON

8. TPC-H: Spark vs. Sparser + Spark

* Same as above, but show CSV + Parquet
* Purpose of this is just to have one "known" workload

### Queries

Datasets: Github, Twitter, Yelp, Zakir (JSON and Parquet)

Queries:

```
Zakir Data

Query 1.
SELECT Metadata.*, IpAddress 
WHERE AS.asn == 24560 &&
P80.HTTP.GET.Header.Server == "apache"

Query 2.
SELECT P22.SSH.Banner
WHERE P23.Telnet.Banner.Banner != NULL &&
Location.Country = "United Kingdom"

Query 3.
SELECT AS.*
WHERE P443.HTTPS.TLS.ciphersuite.name == "TLS_RSA_WITH_RC4_128_SHA"

Query 4.
SELECT Location.*
WHERE P443.HTTPS.TLS.Chain[].parsed.issuer.common_name[] == "Let's Encrypt"
```

Parsed fields and selectivity experiments can go on this dataset.
* Vary selectivity from 1 - 100%
* Vary number of fields retrieved from 1 to all
```
JSON Data

Query 1.

Query 2.

Query 3.

Query 4.
```

* Vary selectivity from 1 - 100%
* Vary number of fields retrieved from 1 to all
```
Github Data

Query 1.

Query 2.

Query 3.
```

Compare against:

* Mison technique
* MongoDB without indexing?

Lesion study

* Probability model turned off (just pick rarest letters or something, or the first field)
* Each deserialization technique turned off
* Only fuzzy search without projection
* Only projection without fuzzy search
