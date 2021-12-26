import pandas as pd

from sqlalchemy import create_engine
import pymysql



db_connection_str = 'mysql+pymysql://root:123@localhost/stock'
db_connection = create_engine(db_connection_str)
df = pd.read_sql('SELECT * FROM daily limit 1000', con=db_connection)

df.to_csv(r'C:\Users\98912\Desktop\Stock\t.csv',index= False)


