import pandas as pd
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

df = pd.read_csv("myFile.csv", index_col=False)

df.to_csv("s3://is459-smux-project-storage/customers/file.csv",
    index=False,
    storage_options={
        "key": AWS_ACCESS_KEY_ID,
        "secret": AWS_SECRET_ACCESS_KEY,
    })



# print(df.head())

