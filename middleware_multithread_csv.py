from Database.Connections import conn
from datetime import datetime
from pathlib import Path
from sqlalchemy import INTEGER,DateTime,String,Numeric
import pandas as pd
import os
import threading

now = datetime.now()
print("start time : ",now.strftime("%Y-%m-%d %H:%M:%S"))

folder_path = 'sourcefolder'

all_files = os.listdir(folder_path)
# print(all_files)

engine = conn()
dtype_mapping = {
            'object': String(100),
            'string': String(100),
            'int64': INTEGER,
            'datetime64[ns]': DateTime,
            'decimal': Numeric
         }

threads = []
for file_name in all_files:
    t = threading.Thread(target=lambda fn = file_name :
        (pd.read_csv( os.path.join(folder_path, fn))
         .drop_duplicates()
         .fillna('')
         .to_sql(Path(fn).stem, con=engine, if_exists='replace',index=False, dtype=dtype_mapping, chunksize=1000)))
    t.start()
    threads.append(t)
    
for t in threads:
    t.join()
     
print("tables created and data inserted")
endtime = datetime.now()
print ("End time : ", endtime.strftime("%Y-%m-%d %H:%M:%S"))

print("total time :", endtime-now)




























