from datetime import datetime

def current_date():
    return datetime.now().strftime("%Y-%m-%d")

def current_date_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def add_source_db_prefix(col_name, source_db):
    return source_db + col_name

import os
import pandas as pd

def import_csv_gracefully(directory, file_name, chunksize=None, low_memory=True):
    file_path = os.path.join(directory, file_name)
    try:
        file_data = pd.read_csv(file_path, chunksize=chunksize, low_memory=low_memory) 
        return file_data
    except Exception as e:
        print(f"There was an issue importing the file located at: {file_path}")
