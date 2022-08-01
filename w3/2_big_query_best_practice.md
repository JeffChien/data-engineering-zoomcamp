## Cost reduction
**Avoid `Select *`**

**Price your queries before running them**

like we do `explain` in traditional database, to see the complexity of the query.

we can do it with 
- `query validator` in the GCP console.
- `--dry_run` flag in bq cli

**Use clustered or partitioned tables**

**Use streaming inserts with caution**

because there's a minimum size(1KB) cost for a row, so you will easily cost fortune for the wasted size.

	Streaming inserts (`tabledata.insertAll`): $0.010 per 200 MB (You are charged for rows that are successfully inserted. Individual rows are calculated using a 1 KB minimum size.)

so, use batch insert.

**Materialize query results in stages**

a view called *materialized view* is a precomputed result,
- BigQuery acn reads only delta changes form the base tables to comput up-to-date results.
	- zero maintenance, when source table change, BigQuery has background process to update the view
- periodically cached
	- fresh data, if source has changed, you will get the value from the source, if not if will get the cached one from the view
- smart tuning, if part of the query can be resolved by query the view, BigQuery will reroutes the query to it.

> postgresql has weaker version of this function

unlike normal view, normal view can be think as a query shortcut, the `M-View` is a snapshot of the query, once it's created, it's like a real table but it can't *insert* and *update*. we can periodically use one command
to tell it to update changes from the source table.


## Query performance
**Filter on partitioned columns**

**Denormalizing data**

normalized db means we have many tables, the relations are well defined and managed.

denormalized db means we have a gian table contains every thing, the benefit is query speed.

**Use nested or repeated columns**

see reference link for detail.

like storing a json object in db.
- a column act like a array (repeated column), we can store many a json like object inside, then each *key* is a nested field.

**Use external data sources approprately**

- because you may also be charged for cloud storage cost.

**Reduce data before using a JOIN**
- avoiding full table scan

**Don't treat WITH clauses as prepared statements**

is it related to the material view?

the document says each time a CTE is reference, they are evaluated each time, so if possible
- store the result in a scalar variable or a temporary table.
  we are not changed for storage of temporary tables.

**Avoid oversharding tables**

unlike traditional, sharding in most case improve performance, but in BigQuery please use partitioning instead for most cases
more sharding means BigQuery need to maitain more schema and meta data, so the overhead may eat the benefit.

**Avoid JavaScript user-defined functions**

**Use Approximate aggregation functions (HyperLogLog++)**

**Order Last, for query operations to maximize performance**

**Optimize your join patterns**

in this order
1. the one with the largest number of rows
2. the one with the fewest rows
3. the rest place by decreasing size.

the reason is, the biggest one will be distributed evenly to all nodes, then the second one will be broadcasted to these nodes.


---
# References
- [Specify nested and repeated columns in table schemas  |  BigQuery  |  Google Cloud](https://cloud.google.com/bigquery/docs/nested-repeated#sql)
- [Control costs in BigQuery  |  Google Cloud](https://cloud.google.com/bigquery/docs/best-practices-costs)
- [Reduce data processed in queries  |  BigQuery  |  Google Cloud](https://cloud.google.com/bigquery/docs/best-practices-performance-communication#avoid_oversharding_tables)