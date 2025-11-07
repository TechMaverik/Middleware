from Database.Connections import conn
from datetime import datetime
from pathlib import Path
from sqlalchemy import INTEGER, DateTime, String, Numeric
import pandas as pd
import os
import threading
import logging

logging.basicConfig(
    filename="logfile.log",
    filemode="w", 
    level=logging.INFO,
)

now = datetime.now()
print("Start time:", now.strftime("%Y-%m-%d %H:%M:%S"))

folder_path = 'sourcefolder'
from pathlib import Path

all_files = [f.name for f in Path(folder_path).glob("*.xls*")]


if not all_files:
    print("No Excel files found in sourcefolder.")
    logging.warning("No Excel files found in sourcefolder.")
    exit()
engine = conn()
dtype_mapping = {
    'object': String(255),
    'string': String(255),
    'int64': INTEGER,
    'datetime64[ns]': DateTime,
    'float64': Numeric
}

def process_file(file_name):
    try:
        file_path = os.path.join(folder_path, file_name)
        df = pd.read_excel(file_path,)
        # df.drop_duplicates()
        df.fillna('')
        df.columns = df.columns.str.strip()
        table_name = Path(file_name).stem
        df.to_sql(
            name=table_name, con=engine, if_exists='append', index=False, dtype=dtype_mapping, chunksize=1000, method='multi')

        msg = f"{file_name} inserted into table {table_name}"
        logging.info(msg)

    except Exception as e:
        err = f"Error processing {file_name}: {e}"
        logging.error(err)


threads = []
for file_name in all_files:
    t = threading.Thread(target=process_file, args=(file_name,))
    t.start()
    threads.append(t)
for t in threads:
    t.join()
endtime = datetime.now()
print("End time:", endtime.strftime("%Y-%m-%d %H:%M:%S"))
print("Total time:", endtime - now)

logging.info(f"Total time: {endtime - now}")
