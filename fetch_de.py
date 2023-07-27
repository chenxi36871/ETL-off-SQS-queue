import boto3
import json
import pandas as pd
from datetime import datetime

import hashlib
import psycopg2
import psycopg2.extras

from user_definition import *

def receive_messages(sqs, Queue_url, MaxNumberOfMessages):
    message_bodies = []
    while True:
        response = sqs.receive_message(QueueUrl=Queue_url, MaxNumberOfMessages=MaxNumberOfMessages)
        if 'Messages' not in response:
            break
        message = response['Messages'][0]
        body = json.loads(message['Body'])
        message_bodies.append(body)
        sqs.delete_message(QueueUrl=Queue_url, ReceiptHandle=message['ReceiptHandle'])
    return message_bodies

print('Start fetching data...')
sqs = boto3.client('sqs', endpoint_url=endpoint_url, region_name=region_name)
message_bodies = receive_messages(sqs, Queue_url, 1)

## hide PII
def mask_pii(device_id, ip):
    # Concatenate the device_id and ip to create a unique identifier
    identifier = f"{device_id}_{ip}"
    # Create a hash of the identifier using SHA-256
    hash_value = hashlib.sha256(identifier.encode()).hexdigest()
    # Use the first 8 characters of the hash as the pseudonym for device_id
    masked_device_id = hash_value[:8]
    # Use the last 8 characters of the hash as the pseudonym for ip
    masked_ip = hash_value[-8:]
    return masked_device_id, masked_ip


df = pd.DataFrame(columns=['user_id', 'device_type', 'masked_ip', 'masked_device_id', 'locale', 'app_version', 'create_date'])
idx = 0
for x in message_bodies:
    try:
        x['create_date'] = datetime.today().date()
        masked_x = mask_pii(x['device_id'], x['ip'])
        x['masked_device_id'] = masked_x[0]
        x['masked_ip'] = masked_x[1]
        df.loc[idx] = x
        idx += 1
    except:
        continue

if len(df)==0:
    print('*'*30)
    print('There is no data to be inserted.')
    print('*'*30)
else:
    print('Data is successfully fetched, start inserting...')
    ## write the data into postgres

    conn_string = f"postgresql://{username}:{password}@{hostname}:{port}/{database}"
    pg_conn = psycopg2.connect(conn_string)
    pg_conn.autocommit = True
    cur = pg_conn.cursor()

    # recreate the table to make data format appropriate
    sql = f"""
    DROP TABLE {table_name};
    """
    cur.execute(sql)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        user_id VARCHAR(128),
        device_type VARCHAR(32),
        masked_ip VARCHAR(256),
        masked_device_id VARCHAR(256),
        locale VARCHAR(32),
        app_version VARCHAR(32),
        create_date DATE
    );
    """

    cur.execute(create_table_query)

    ## insert the data into database
    tups = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))

    insert_query = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols)
    psycopg2.extras.execute_values(cur, insert_query, tups)

    cur.close()
    print('Data successfully inserted!')