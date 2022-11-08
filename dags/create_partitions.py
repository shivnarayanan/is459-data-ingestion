from pyspark.sql import SparkSession
import pandas as pd
from random import randrange

datasets = ["customers", "articles", "transactions"]

for dataset in datasets:
    partition = pd.read_csv("/usr/local/airflow/data/{}.csv".format(dataset), nrows=500)
    
    filepath = "/usr/local/airflow/data/{}/{}_partition_{}.json".format(dataset, dataset, kwargs['run_id'])
    partition.to_json(filepath, orient='records', lines=True)