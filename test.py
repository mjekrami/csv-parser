import polars as pl
from polars import read_csv_batched


seen_group = set()
reader = read_csv_batched("./src/csv/SMS_EDR_20230830.csv", separator=";")
batches = reader.next_batches(100)

while batches:
    df_current_batches = pl.concat(batches)

    partition_df = df_current_batches.partition_by("CDR_STATUS", as_dict=True)

    for group, df in partition_df.items():
        if group in seen_group:
            with open(f"./data/{group}.csv", "a") as fh:
                fh.write(df.write_csv(file=None, has_header=False))
        else:
            df.write_csv(file=f"./data/{group}.csv", has_header=True)
        seen_group.add(group)
    batches = reader.next_batches(100)
