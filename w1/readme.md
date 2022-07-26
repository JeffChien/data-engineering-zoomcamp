# [Basic and Setup](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup)


Mapping
| video                                                                                | state | notes                   |
| ------------------------------------------------------------------------------------ | ----- | ----------------------- |
| [1.1.1 - Introduction to Google Cloud Platform][v1.1.1]                              | skip  |                         |
| [1.2.1 - Introduction to Docker][v1.2.1]                                             | done  | [0_container_setup][f1] |
| [1.2.2 - Ingesting NY Taxi Data to Postgres][v1.2.2]                                 | done  | [1_data_ingestion][f2]  |
| [1.2.3 - Connecting pgAdmin and Postgres][v1.2.3]                                    | done  | [0_container_setup][f1] |
| [1.2.4 - Dockerizing the Ingestion Script][v1.2.4]                                   | done  | [2_dockerize][f3]       |
| [1.2.5 - Running Postgres and pgAdmin with Docker-Compose][v1.2.5]                   | done  | [0_container_setup][f1] |
| [1.2.6 - SQL Refreshser][v1.2.6]                                                     | done  | [3_sql_refresher][f4]   |
| [1.3.1 - Introduction to Terraform Concepts & GCP Pre-Requisites][v1.3.1]            | done  | [4_gcp][f5]             |
| [1.3.2 - Creating GCP Infrastructure with Terraform][v1.3.2]                         | done  | [5_terraform][f6]       |
| [1.4.1 - Setting up the Environment on Google Cloud (Cloud VM + SSH access)][v1.4.1] | skip  |                         |
| [1.4.2 - Port Mapping and Networks in Docker (Bonus)][v1.4.2]                        | skip  |                         |

# Prerequisite

In order to install `pgcli` package in mac, we need to have postgres in our system.

macos + poetry

```shell

> brew install postgres

> poetry add pgcli
```

<!-- file links -->
[f1]: ./0_container_setup.ipynb
[f2]: ./1_data_ingestion.ipynb
[f3]: ./2_dockerize.ipynb
[f4]: ./3_sql_refresher.ipynb
[f5]: ./4_gcp.ipynb
[f6]: ./5_terraform.ipynb

<!-- video links -->
[v1.1.1]: https://www.youtube.com/watch?v=18jIzE41fJ4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=3
[v1.2.1]: https://www.youtube.com/watch?v=EYNwNlOrpr0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=4
[v1.2.2]: https://www.youtube.com/watch?v=2JM-ziJt0WI&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=5
[v1.2.3]: https://www.youtube.com/watch?v=hCAIVe9N0ow&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=7
[v1.2.4]: https://www.youtube.com/watch?v=B1WwATwf-vY&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=8
[v1.2.5]: https://www.youtube.com/watch?v=hKI6PkPhpa0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=9
[v1.2.6]: https://www.youtube.com/watch?v=QEcps_iskgg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=10
[v1.3.1]: https://www.youtube.com/watch?v=Hajwnmj0xfQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=11
[v1.3.2]: https://www.youtube.com/watch?v=dNkEgO-CExg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=12
[v1.4.1]: https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=13
[v1.4.2]: https://www.youtube.com/watch?v=tOr4hTsHOzU&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=14