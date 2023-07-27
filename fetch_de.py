import boto3
import json
import pandas as pd
from datetime import datetime

import hashlib
from cryptography.fernet import Fernet
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

# Generate a secret key for encryption (keep this key secure and don't share it)
secret_key = Fernet.generate_key()

def mask_pii(var):
    # Concatenate the device_id and ip to create a unique identifier
    identifier = var
    # Create an instance of the Fernet cipher with the secret key
    cipher = Fernet(secret_key)
    # Encrypt the identifier
    encrypted_identifier = cipher.encrypt(identifier.encode())
    return encrypted_identifier

def recover_pii(encrypted_identifier):
    # Create an instance of the Fernet cipher with the secret key
    cipher = Fernet(secret_key)

    # Decrypt the encrypted identifier to recover the original PII
    decrypted_identifier = cipher.decrypt(encrypted_identifier).decode()

    return decrypted_identifier


df = pd.DataFrame(columns=['user_id', 'device_type', 'masked_ip', 'masked_device_id', 'locale', 'app_version', 'create_date'])
idx = 0
for x in message_bodies:
    try:
        x['create_date'] = datetime.today().date()
        x['masked_device_id'] = mask_pii(x['device_id'])
        x['masked_ip'] = mask_pii(x['ip'])
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