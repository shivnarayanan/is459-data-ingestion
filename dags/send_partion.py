import pandas as pd
import boto3 
from datetime import datetime
import time
import calendar
import json
import random
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

kinesis = boto3.client('kinesis', region_name='us-east-1', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
stream_name = 'my-stream'
partion_key = str(random.randrange(100))

filename = "/usr/local/airflow/data/customer_partition_{}.json".format(kwargs['run_id'])
df = pd.read_json(filename, orient='records', lines=True)

response = kinesis.put_record(StreamName=stream_name, Data=df.to_json(orient='records', lines=True), PartitionKey=partion_key)

response

print(response)

time.sleep(1)
