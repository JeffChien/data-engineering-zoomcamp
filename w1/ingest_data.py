#%%
import pandas as pd
from pip import main
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from io import BytesIO
import urllib.request as request
import json
import click

#%%
def ingest_data(fd, table, db_engine, batch_size=100000):
    pq_file = pq.ParquetFile(fd)
    for batch in pq_file.iter_batches(batch_size=batch_size):
        df = batch.to_pandas()
        df.to_sql(name=table, con=db_engine, if_exists='append', chunksize=batch_size)
        


@click.command()
@click.option('--user')
@click.option('--password')
@click.option('--host')
@click.option('--port')
@click.option('--db')
@click.option('--table')
@click.option('--batch-size', default=100000)
@click.argument('url')
def cli(user, password, host, port, db, table, batch_size, url):
    db_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(db_url)
    engine.connect()
    dataset_fd = BytesIO(request.urlopen(url).read())
    ingest_data(dataset_fd, table, engine, batch_size)

#%%
if __name__ == '__main__':
    cli()