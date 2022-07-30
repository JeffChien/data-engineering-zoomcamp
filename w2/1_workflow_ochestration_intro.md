The problem in the w1's docker pipeline there are too many steps in a single module
- download dataset
- parse and transform dataset
- store to database

expecially the download step, if we has network issuse, our pipeline will be blocked.

so a better way to do is to split into 2 different modules
- download and parse
- store to database

one module's output is another one's input, each has parameters to control how they should work.

a module is also called a *job*, jobs should be arrange in a *directed acyclic graph(DAG)*

we also need a good software to manage or orchestrate our DAG jobs, popular softwares are
- Luigi
- Airflow
- prefect