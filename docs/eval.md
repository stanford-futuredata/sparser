# Evaluation

The evaluation should answer the following questions:

1. What kinds of environments does Sparser work well in?
2. How does Sparser compare with existing string matching algorithms (e.g., `strstr` or `grep` implemented with Boyer-Moore or Aho-Corasick) and optimized parsers such as Mison and RapidJSON?
3. Does Sparser's predicate and projection scheduling affect performance?
4. Does Sparser improve performance in real systems?

### Motivation Plots

These plots answer question (1) - what kinds of environments does Sparser's core data parallel algorithm work well in?

1. Clustered bar chart showing breakdown of runtime for queries in Spark on JSON (convinces reader that parsing+filtering takes a long time)
2. Clustered bar chart showing breakdown of runtime for rule filtering in Snort (convinces reader that filtering takes a long time in this application as well). This and the previous one should be side-by-side, doesn't need to take up too much space.
3. Show sample queries common in JSON datasets, and show average selectivity of these predicates. (This could be moved to beginning of eval section.) An assumption in Sparser is that 1) data analysts want to filter their data with predicates when starting a particular analysis, and 2) these predicates are often fairly selective. If those are both true, then Sparser will dominate compared to Mison, since Mison only cares about projecting things out. If not, that's still okay, because Sparser can still schedule that projections come first—can't lose!
3. "Donald Trump" vs. "ld T" (convinces reader that data parallelism gives you speedup and substrings are good indicators of queries)

## Sparser Performance

This section answers question (2) - how does Sparser compare to fast parsers and string matching algorithms.

1. Zakir Query 1-4 - Sparser vs. RapidJSON, Mison, `ag` (Boyer-Moore) Search + RapidJSON
 * x-axis is query, y-axis is GB/s
 * Stacked bar chart for Sparser, to show the breakdown in runtime: scheduler + approx filtering + actual parsing.
 * Bar for RapidJSON ("best" JSON parser) on each record, followed by filtering
 * Bar for Mison leveled colon map creation (lower bound for Mison)
 * Stacked bar chart for `ag` + RapidJSON: Show advantages of data parallel algorithms with fuzzy search compared to state of the art string search algorithms. Argument here is that we want to leverage the hardware instead of doing byte-wise operations
 * Shows performance compared to a state-of-the-art JSON
 * Can also compare GB/s to Mison leveled colon map creation (no full parsing)

2. Twitter JSON Data - Sparser performance
 * Firas: I would do the same as above. Figure 10 from Mison isn't very fair, IMO. Of course, if you have a single filter, then the more selective it is, the better it's going to do. But the Mison folks never evaluated their system on a query with more than 1 filter! This is where Sparser will really shine.
 * Line Graph, each line is Selectivity, x-axis is Parsed Fields, y-axis is GB/s (Figure 10 from Mison)
 * Shows how performance is affected by selectivity, parsed fields, etc.

3. TPC-H CSV Data - Sparser Peformance

 * Line Graph, each line is Selectivity, x-axis is Retrieved Fields, y-axis is GBps (Figure 10 from Mison)
 * Shows that the technique is applicable to CSV and the deserialization filters help accelerate these workloads.s

### Scheduler Performance

This answers question 3.

#### Substring selection

5. Zakir queries 1 - 4, Twitter queries 1 - 8, TPC-H CSV: Runtime for choosing different filters

 * Table organized as follows:

   Query Filters | Ideal Substring/FP rate/Runtime | Worst Substring/FP rate/Runtime | Sparser Substring/FP rate/Runtime
   --- | --- | --- | ---

 *  Shows effects of choosing filters matters, and that picking the correct filter matters

#### Scheduling between filters and projections

6. Zakir queries 1-4, Twitter queries 1 - 8, TPC-H CSV: Scheduling projections and filters
  * Table organized as follows:
  
  Query Filters | Query Projections | All Filters First Runtime | All Predicates First Runtime | Sparser Scheduler Order + Runtime
  --- | --- | --- | --- | ---

  * Shows that scheduling these gives better performance in most cases

#### Handling Multiple Filters and Multiple Selectivities

7. Bar chart: x-axis: # of filters, y-axis: GB/sec
    * Twitter query 1 (or whatever query has the most filters)
    * Sample a few filters, each with a different selectivity, but make the aggregate selectivity constant (e.g., 10%)
    * \# of projections should be held constant throughout
    * Mison has poor support for multiple filters, because it always focuses on projections first. Here, we'll show that, not only do we beat Mison on a single filter, we dominate on multiple filters.

8. Line chart: x-axis: # of projected fields, y-axis: GB/sec
    * Each line has different selectivity but same \# of filters
    * Here, we vary selectivity but keep \# of fields constant.
    (If we set \# of fields to be 1, then we are recreating Fig. 10 from the Mison paper.)
    * This shows that we do better along both the selectivity and \#-of-filters axes

### Spark Integration

9. Zakir queries 1 - 4, Twitter queries 1 - 8, TPC-H: Spark vs. Sparser+Spark

  * Clustered bar chart with end-to-end job completion time
  * Breakdown of parse vs. processing
  * Show for JSON and CSV
  
10. Snort `blacklist.rules` ruleset - Perform string matching for some of these rules
  * Throughput in Gbps with and without Sparser - Sparser tosses out data quickly.

## Queries

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
