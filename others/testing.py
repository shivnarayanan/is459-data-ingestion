import pandas as pd
import json

filename = "my-firehose-2-2022-10-29-09-05-50-ea9309ac-f850-4c31-b888-ad4b1d06361d.parquet"

df = pd.read_parquet(filename)
df.to_csv("testing.csv", index=False)