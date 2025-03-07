import pandas as pd 
from sqlalchemy import create_engine, text 

select_query = text('SELECT * FROM games;')
engine = create_engine('sqlite:///../data/test')
data = pd.read_sql(select_query, 'sqlite:///../data/test', index_col='GameID')

print(data.head())