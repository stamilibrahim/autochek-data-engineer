import sqlite3
import os
import pandas as pd

QUERY_FILE = 'transform.sql'
# TODO first run create-db.py
DATABASE_NAME = 'autochek.db'

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
database_file = os.path.join(parent_dir, DATABASE_NAME)

conn = sqlite3.connect(database_file)
cursor = conn.cursor()

with open(os.path.join(current_dir, QUERY_FILE), 'r') as file:
    q = file.read()
    # cursor.execute(q)
    df = pd.read_sql_query(q, conn)

print(df)

conn.commit()
conn.close()