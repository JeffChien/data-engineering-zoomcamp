some advantages over traditional database
- serverless.
- easy to scale out to large data.
- more features like builtin ML.


## playing with BigQuery
- the query is cached by default.
    can be switch off in `query setting / cache preference.`

- provide many public dataset.

- external table
   we can tell BigQuery to use data stored in the cloud storage to create table automatically
   ```sql
   create or relpace external table `<prj_name>.<dataset_name>.<table_name>`
   options (
	   format = 'parquet',
	   uris = ['gs://<bucket>/*.parquet*']
   )
   ```
   but because it's *extenal*, BigQuery will not know the table size and log-term storage size, we can see it in the *Details* tab. 

- cost
    2 types
    - on demain
    - 1TB of data processed is $5
    - flat rate
        - based on number of pre requested `slots`?
        - 100 slots -> $2,000/month = 400TB

    the concept of `slot` is like a connection pool, if your k quries has occupied whole 100 slots, then the k+1 query has to wait, unlike on demain model that BigQuery will keep give you more slot.

    there're 2 processed informations in the sql execution page, the top right is the *expected cost*, the one under teh query result is the *actual* cost.


## partitionng and clustering
- partitioning
    - limit up to 4,0000 per table
    - similar to split gian table into multiple smaller ones base on some conditions.
    - bigquery will route query to specific partitioned table.
- cluster
    - limit up to 4 clusters per table.
    - put similar values in a continuous block.