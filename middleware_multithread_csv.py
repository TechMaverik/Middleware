from Database.Connections import conn
from datetime import datetime
from pathlib import Path
from sqlalchemy import INTEGER,DateTime,String,Numeric
import pandas as pd
import os
import threading
import shutil
import logging

logging.basicConfig(
    filename="logfile.log",
    level=logging.INFO,
)
now = datetime.now()
print("start time : ", now.strftime("%Y-%m-%d %H:%M:%S"))

folder_path = 'sourcefolder'
operation_folder = 'operationfolder'
os.makedirs(operation_folder, exist_ok=True)

all_files = os.listdir(folder_path)

for file_name in all_files:
    src = os.path.join(folder_path, file_name)
    dst = os.path.join(operation_folder, file_name)
    shutil.copy(src, dst)
    logging.info(f"Copied {file_name} to operationfolder")

engine = conn()
dtype_mapping = {
            'object': String(100),
            'string': String(100),
            'int64': INTEGER,
            'datetime64[ns]': DateTime,
            'decimal': Numeric
         }
def process_file(file_name):
    try:
        file_path = os.path.join(operation_folder,file_name)
        df = pd.read_csv(file_path)
        df = df.drop_duplicates().fillna('')
        df.columns = df.columns.str.strip()
        
        table_name = Path(file_name).stem
        df.to_sql(name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            dtype=dtype_mapping)
        
        msg = f" {file_name} inserted into table {table_name}"
        logging.info(msg)
        
    except :
        err = f" Error processing {file_name}"
        logging.error(err)

threads = []
for file_name in os.listdir(operation_folder):
    t = threading.Thread(target=process_file, args=(file_name, ))
    t.start()
    threads.append(t)
    
for t in threads:
    t.join()
     
logging.info("tables created and data inserted")
endtime = datetime.now()
print("End time : ", endtime.strftime("%Y-%m-%d %H:%M:%S"))

print("total time :", endtime-now)

logging.info(f"Total time: {endtime - now}")


























