import logging
import os
from dataclasses import dataclass
from distutils.log import error
from urllib import request

import sqlalchemy

logger = logging.getLogger("data_ingestion")
logger.setLevel(logging.INFO)
import csv
from io import StringIO

import numpy as np
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq


@dataclass
class DBConf:
    host: str
    user: str
    password: str
    port: int
    database: str

    def postgres_url(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


def ensure_nemeric(df, dtype, cols_names):
    for cname in cols_names:
        if cname not in df.columns:
            continue
        if dtype == df[cname].dtype:
            continue
        df[cname] = pd.to_numeric(df[cname], errors="coerce").fillna(0).astype(dtype)
    return df


def fix_bad_dt(df, cols_names):
    def do_fix(dt):
        if dt.year >= 3000:
            dt = dt.replace(year=dt.year - 1000)
        return dt

    for cname in cols_names:
        if cname not in df.columns:
            continue
        if np.issubdtype(df[cname].dtype, np.datetime64):
            continue
        dt_series = pd.to_datetime(df[cname], errors="coerce")
        mask = dt_series.isnull()
        dt_series[mask] = pd.to_datetime(df.loc[mask, cname].apply(do_fix))
        df[cname] = dt_series.dt.tz_localize("EST")
    return df


def fix_df(df):
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df = fix_bad_dt(
        df,
        [
            "pickup_datetime",
            "dropOff_datetime",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
        ],
    )
    df = ensure_nemeric(
        df, "int64", ["vendorid", "pulocationid", "dolocationid", "locationid"]
    )
    df = ensure_nemeric(
        df,
        "float64",
        [
            "passenger_count",
            "trip_distance",
            "ratecodeid",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
            "ehail_fee",
            "payment_type",
            "trip_type",
        ],
    )
    return df


def download(url, dest):
    try:
        logger.info(f"Downloading from {url}")
        res = request.urlopen(url)
        with open(dest, "wb+") as f:
            f.write(res.read())
            logger.info(f"save to {dest}")
    except Exception as err:
        logger.error(err)


def csv_to_parquet(csv_file, output_file):
    pq.write_table(pv.read_csv(csv_file), output_file)


def psq_insert_copy(table, conn, keys, data_iter):
    output = StringIO()
    writer = csv.writer(output)
    writer.writerows(data_iter)
    output.seek(0)
    columns = ",".join(f'"{k}"' for k in keys)
    if table.schema:
        table_name = f"{table.schema}.{table.name}"
    else:
        table_name = table.name

    sql = f'COPY "{table_name}" ({columns}) FROM STDIN WITH CSV'

    with conn.connection.cursor() as cur:
        cur.copy_expert(sql=sql, file=output)


def ingest_data(file, db_conf: DBConf, table, batch_size=100000):
    logger.info(f"Importing {file}")
    pq_file = pq.ParquetFile(file)
    engine = sqlalchemy.create_engine(db_conf.postgres_url())
    runs = 0
    count = 0
    for batch in pq_file.iter_batches(batch_size=batch_size):
        df = batch.to_pandas(timestamp_as_object=True)
        df = fix_df(df)
        if runs == 0:
            df.head(0).to_sql(name=table, con=engine, if_exists="replace", index=False)

        df.to_sql(
            name=table,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=batch_size,
            method=psq_insert_copy,
        )

        runs += 1
        count += df.shape[0]
        logger.info(f"runs: {runs}, records: {count:,}")
    logger.info(f"ingested total {count:,} lines")
