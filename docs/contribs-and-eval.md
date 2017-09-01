## Contributions

1. Cascading search technique to reduce complex, expensive-to-evaluate predicates into simpler ones with false positives. Search technique applies a number of "fuzzy" filters (e.g., comparing substrings, encoded ints, etc.).

2. Query scheduler which adaptively tunes the search and schedules filters and projections dynamically, while picking filters to apply for deserialization, etc. Schedule is driven by a probability model which estimates the costs of running filters.

3. Evaluation of the system on different data formats (JSON - an unstructured text-based format, CSV, a structured text-based format, and Parquet, a binary, structured columnar format), showing speedups of up to **Nx** for JSON and **Nx** on Parquet for loading data, and speedups of up to **Nx** when integrated with a distributed system (Spark)


## Evaluation Outline

### Appetizer/Motivation Plots

1. Clustered bar chart showing breakdown of runtime for queries in Spark on JSON (convinces reader that parsing takes a long time)
2. Clustered bar chart showing breakdown of runtime for regex rule filtering in Snort (convinces reader that parsing takes a long time in this application as well). This and the previous one should be side-by-side, doesn't need to take up too much space.
3. Show sample queries common in JSON datasets, and show average selectivity of these predicates. (This could be moved to beginning of eval section.) An assumption in Sparser is that 1) data analysts want to filter their data with predicates when starting a particular analysis, and 2) these predicates are often fairly selective. If those are both true, then Sparser will dominate compared to Mison, since Mison only cares about projecting things out. If not, that's still okay, because Sparser can still schedule that projections come first—can't lose!
3. "Donald Trump" vs. "ld T" (convinces reader that data parallelism gives you speedup and substrings are good indicators of queries)
4. Equivalent substring example for Snort, again show side-by-side with previous one.

### Sparser Performance

1. Zakir Query 1-4 - Sparser vs. RapidJSON, Mison, Regex Search + RapidJSON
 * x-axis is query, y-axis is GB/s
 * Clustered bar chart for Sparser, to show the breakdown in runtime: scheduler + approx filtering + actual parsing.
 * Shows performance compared to a state-of-the-art JSON
 * Can also compare GB/s to Mison (no source code available)

2. Twitter JSON Data - Sparser performance
 * Firas: I would do the same as above. Figure 10 from Mison isn't very fair, IMO. Of course, if you have a single filter, then the more selective it is, the better it's going to do. But the Mison folks never evaluated their system on a query with more than 1 filter! This is where Sparser will really shine.
 * Line Graph, each line is Selectivity, x-axis is Parsed Fields, y-axis is GB/s (Figure 10 from Mison)
 * Shows how performance is affected by selectivity, parsed fields, etc.

3. TPC-H CSV Data - Sparser Peformance

 * Line Graph, each line is Selectivity, x-axis is Retrieved Fields, y-axis is GBps (Figure 10 from Mison)
 * Shows that the technique is applicable to CSV and the deserialization filters help accelerate these workloads.s

### Scheduler Performance

#### Substring selection

* Zakir queries 1 - 4, Twitter queries 1 - 8, TPC-H CSV: Runtime for choosing different filters

 * Table organized as follows:

    | Query Filters || Ideal Substring/FP rate/Runtime | Worst Substring/FP rate/Runtime | Sparser Substring/FP rate/Runtime |

 *  Shows effects of choosing filters matters, and that picking the correct filter matters

#### Scheduling between filters and projections

* Zakir queries 1-4, Twitter queries 1 - 8, TPC-H CSV: Scheduling projections and filters
  * Table organized as follows:
  | Query Filters | Query Projections | All Filters First Runtime | All Predicates First Runtime | Sparser Scheduler Order + Runtime |
  * Shows that scheduling these gives better performance in most cases

#### Handling Multiple Filters and Multiple Selectivities

  1. Bar chart: x-axis: # of filters, y-axis: GB/sec
    * Twitter query 1 (or whatever query has the most filters)
    * Sample a few filters, each with a different selectivity, but make the aggregate selectivity constant (e.g., 10%)
    * \# of projections should be held constant throughout
    * Mison has poor support for multiple filters, because it always focuses on projections first. Here, we'll show that, not only do we beat Mison on a single filter, we dominate on multiple filters.

  2. Line chart: x-axis: # of projected fields, y-axis: GB/sec
    * Each line has different selectivity but same \# of filters
    * Here, we vary selectivity but keep \# of fields constant.
    (If we set \# of fields to be 1, then we are recreating Fig. 10 from the Mison paper.)
    * This shows that we do better along both the selectivity and \#-of-filters axes

### Spark Integration

7. Zakir queries 1 - 4, Twitter queries 1 - 8, TPC-H: Spark vs. Sparser+Spark

  * Clustered bar chart with end-to-end job completion time
  * Breakdown of parse vs. processing
  * Show for JSON and CSV


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

Lesion study

* Probability model turned off (just pick rarest letters or something, or the first field)
* Each deserialization technique turned off
* Only fuzzy search without projection
* Only projection without fuzzy search
