
from sqlalchemy import create_engine

def conn():
    engine = create_engine("mysql+pymysql://root:root@localhost:3306/covid19_db")
    return engine


