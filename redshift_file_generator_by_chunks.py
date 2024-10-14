#!/usr/bin/env python
# coding: utf-8

import csv
from zipfile import ZipFile, ZipInfo
from io import BytesIO, StringIO
from typing import IO, List
from datetime import datetime
from sys import exit, stdout
from redshift_connector import connect, Connection
import os
import argparse


# In[8]:


def redshift_open_connection_by_dict(dict_secret: dict, database: str = None) -> Connection:
    print(f'Opening connection by secret dict')

    if database is None:
        database = dict_secret['dbname']

    return connect(
        host=dict_secret['hostname'],
        database=database,
        user=dict_secret['user'],
        password=dict_secret['password'],
        port=dict_secret['port']
    )


# In[9]:


# Function to save data in chunks to CSV
def redshift_get_rows_and_save_csv_by_chunks(dict_secret: dict, str_query: str, save_path: str, delimiter: str = '|', quoting: int = csv.QUOTE_NONNUMERIC, lineterminator: str = '\r\n', upper_header: bool = True, batch_size: int = 1000):
    conn = redshift_open_connection_by_dict(dict_secret)
    conn.autocommit = False
    cur = conn.cursor()

    print('Executing query...')
    cur.execute(str_query)
    cols = [a[0] for a in cur.description]

    # Ensure the directory exists
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    with open(save_path, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file, delimiter=delimiter, quotechar='"', quoting=quoting, lineterminator=lineterminator)

        if upper_header:
            cols = [k.upper() for k in cols]
            
        # Write header
        writer.writerow(cols)
        idx = 1
        while True:
            print(f'Running idx batch_size: {idx}')
            idx += 1
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            writer.writerows(rows)

    print(f"Data saved to CSV at: {save_path}")

    cur.close()
    conn.close()


# ## Set your credencials and root path to store files

# In[10]:


dict_secret = {
    "dbname": "datalake_dw",
    "port": 5439,
    "hostname": "asgard-redshift-production.cmqegk5gj3mi.sa-east-1.redshift.amazonaws.com",
    "user": "herculano_cunha",
    "password": "your secret password"
}
str_path_save = "./"


# In[16]:


parser = argparse.ArgumentParser(description="Process a value.")
args = None
try:
    parser.add_argument("--query", type=str, help="Query")
    parser.add_argument("--filename", type=str, help="Filename")
    parser.add_argument("--filepath", type=str, help="Filepath")
    parser.add_argument("--username", type=str, help="Username")
    parser.add_argument("--password", type=str, help="Password")
    args = parser.parse_args()
    print(f'Args: {args}')
except:
    print('Arguments not passed')


# In[19]:


list_of_querys = [
    {'query': "select * from credit_portfolio.metrica limit 100000", 'filename': 'metrica.csv'},
    {'query': "select * from credit_portfolio.contrato limit 100000", 'filename': 'contrato.csv'}
]


# In[17]:
if (
    args is not None and
    args.query is not None and
    args.filename is not None and 
    args.filepath is not None 
    ):
    str_path_save = args.filepath
    list_of_querys = [
    {'query': args.query, 'filename': args.filename}
    ]

if (
    args is not None and
    args.username is not None and 
    args.password is not None
    ):
    dict_secret['user']=args.username
    dict_secret['password']=args.password
    


# In[18]:


for dict_query in list_of_querys:
    print(f"Selecting rows on Redshift for query: {dict_query.get('query')}")
    save_path = f"{str_path_save}\{dict_query.get('filename')}"
    redshift_get_rows_and_save_csv_by_chunks(dict_secret=dict_secret, str_query=dict_query.get('query'), save_path=save_path, delimiter= '|', quoting=csv.QUOTE_NONE)
    print(f"{save_path} - saved")
    print()
    print('------------------------')
    print()

