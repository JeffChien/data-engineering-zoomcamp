# Problems I've encountered

## Need faster data ingestion

The default method of `dataframe.to_sql` was too slow, even worse with `method=multi`, the only solution I got was to use copy command with csv format, it's 1x times quicker.

## Timestamp conversion out of boundary

Especially in FHV dataset, there are rows with timestamp in year `30XX`.

1. Tell pyarrow to leave timestamp as object, we'll deal it with pandas.
    ```python
    pyarrow_table.to_pandas(timestamp_as_object=True)
    ```

2. Cast good ones, and leave the bad one as null.
    ```python
    dt_series = pd.to_datetime(df[cname], errors="coerce")
    mask = dt_series.isnull()
    ```

3. Fix the bad ones
    ```python
    def do_fix(dt):
        if dt.year >= 3000:
            dt = dt.replace(year=dt.year - 1000)
        return dt

    dt_series[mask] = df.loc[mask, cname].apply(do_fix)
    df[cname] = dt_series
    ```
