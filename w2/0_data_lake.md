# What is a Data Lake?

A repository that hold many big data from different sources, geneally data can be
- *structure*
- *semi-structure* 
- *unstructure*

the goal is to ingest data as quick as possible and make it available to other members. 

geneally if you would store data in data lake, you would associate some sort of meta data for quick access.

A data lake
- has to be secured
- easy to scale
- as cheaper as possible

## Data lake vs Data warehouse

|           | Data lake                | Data warehouse    |
| --------- | ------------------------ | ----------------- |
| data type | unstructured             | structured        |
| data size | raw and large            | refined and small |
| user      | Data scientists/analysts | Business analysts |
| use case  | strea / ML / realtime    | batch / BI        |

## Why did it start?

- companies realized the value of data
- store and access data quickly
- cannot always define structure of data
- usefulness of data being realized later in the project lifecycle.
- need cheeaper storage for big data

## ETL vs ELT

|              | Export Transform and Load(ETL) | Export Load and Transform(ELT) |
| ------------ | ------------------------------ | ------------------------------ |
| size of data | small                          | large                          |
| solution for | data warehouse                 | data lake                      |
| schema       | schema on write                | schema on read                 |

schema on write means define data schema first, then write to accordingly to database.
schema on read means data come with no schema, the user has to define the schema to read the data.

## Gocha of Data Lake

Data swamp, data lake becomes hard to use, because of
- no versioning
- incompatible schema for same data without versioning.
- joins not possible
- no metadata associated
