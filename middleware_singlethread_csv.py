from Database.Connections import conn
from datetime import datetime
from pathlib import Path
from sqlalchemy import INTEGER,DateTime,String,Numeric
import pandas as pd
import os

now = datetime.now()
print("start time : ",now.strftime("%Y-%m-%d %H:%M:%S"))

folder_path = 'sourcefolder'

all_files = os.listdir(folder_path)
print(all_files)

engine = conn()

for file_name in all_files:
    file_path = os.path.join(folder_path, file_name)
    df = pd.read_csv(file_path)
    table_name = Path(file_name).stem
    # df_list.append(df)
    
#df = pd.concat(df_list, ignore_index=True)
#print(df.columns.to_list())
    df = df.drop_duplicates()
    df=df.fillna('')
    df.columns = df.columns.str.strip()
    dtype_mapping = {
            'object': String(100),
            'string': String(100),
            'int64': INTEGER,
            'datetime64[ns]': DateTime,
            'decimal': Numeric
         }

    df.to_sql(name=table_name, con=engine, if_exists='replace',index=False, dtype=dtype_mapping)
    
     
print("tables created and data inserted")
endtime = datetime.now()
print ("End time : ", endtime.strftime("%Y-%m-%d %H:%M:%S"))

print("total time :", endtime-now)


























