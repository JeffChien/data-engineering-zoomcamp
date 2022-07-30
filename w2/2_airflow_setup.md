1. download the official docker-compose.yaml file: [🔗](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml)

1. build the customized airflow image: [🔗](./airflow/Dockerfile)
    - to have python packages needed
    - to have gcloud command

2. prepare `.env` file: [🔗](./airflow/.env)
    - most airflow setting can be change though environment variable.

3. modify official docker-compose.yaml
    - add gcp credential volume mapping


## Light weight airflow
The official ones runs too many different services together, give my computer a very heavy load, so I modify the original docker-composed.yml file
1. remove redis, celery, flower, trigger, worker, postgres

2. we already have a postgres, why don't we reuse it?

    we have to create **airflow** database and user first

    ```sql
    create database airflow;
    create user airflow with password 'airflow';
    grant all privileges on database airflow to airflow;
    ```

    `docker-compose` allows us to use multiple yaml config together, it will merge them as a giant yaml config, we can check the final result with the commad

    ```
    > docker compose convert
    ```

    I've created a [short wrapper script](./airflow/compose.sh) for it.

I also keep the [official config](./airflow/refs/docker-compose.yaml) in a reference directory.