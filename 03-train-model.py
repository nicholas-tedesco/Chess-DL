# README ------------------------------------------------------------------------------------------
# 
#   file: 03-train-model.py
#   goal: fit deep learning model to predict next game board state given current board state
# 
# -------------------------------------------------------------------------------------------------

# packages and data -------------------------------------------------------------------------------

import pandas as pd 
from sqlalchemy import create_engine, text 

db_path = '../data/lichess.db'
table_name = 'games'

select_query = text(f'SELECT * FROM {table_name};')
engine = create_engine(f'sqlite:///{db_path}')
data = pd.read_sql(select_query, f'sqlite:///{db_path}', index_col='GameID')

print('')
print('\tSuccessfully loaded chess game data.')
print(f'\t- Rows: {data.shape[0]:,}')
print(f'\t- Cols: {data.shape[1]}')


# 