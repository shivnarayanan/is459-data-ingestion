def convertPartitions(**kwargs):
    import pandas as pd
    from random import randrange

    datasets = ["transactions"]

    for dataset in datasets:
        partition = pd.read_csv("/usr/local/airflow/data/{}_partition.csv".format(dataset))
        
        filepath = "/usr/local/airflow/data/{}/{}_partition_{}.json".format(dataset, dataset, kwargs['run_id'])
        partition.to_json(filepath, orient='records', lines=True)

def sendPartitions(**kwargs):
    import pandas as pd
    import boto3 
    import time
    import random
    from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    
    kinesis = boto3.client('kinesis', region_name='us-east-1', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    stream_name = 'my-transactions-stream'
    partion_key = str(random.randrange(100))
    filename = "/usr/local/airflow/data/transactions/transactions_partition_{}.json".format(kwargs['run_id'])
    df = pd.read_json(filename, orient='records', lines=True)
    response = kinesis.put_record(StreamName=stream_name, Data=df.to_json(orient='records', lines=True), PartitionKey=partion_key)
    response
    
    time.sleep(1)

