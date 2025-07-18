import pandas as pd
import sqlite3

def save_to_csv(data, file_path):
    data.to_csv(file_path, index=False)

def load_from_csv(file_path):
    return pd.read_csv(file_path)

def save_to_db(data, db_path, table_name):
    conn = sqlite3.connect(db_path)
    data.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()