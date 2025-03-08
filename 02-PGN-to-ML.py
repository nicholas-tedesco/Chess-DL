# README ------------------------------------------------------------------------------------------
# 
#   file: 02-PGN-to-ML.py
#   goal: prepare data for ML modeling by translating PGN to series of individual moves, 
#         where each move is represented as current board state : next board state pair 
#         and moves are from perspective of game winner
# 
# -------------------------------------------------------------------------------------------------

# packages and data -------------------------------------------------------------------------------

import pandas as pd 
from sqlalchemy import create_engine, text 
import re
import chess, chess.pgn
import io 

db_path = '../data/lichess.db'
table_name = 'games'

engine = create_engine('sqlite:///' + db_path) 
select_query = f'SELECT * FROM {table_name};'

data = pd.read_sql(select_query, engine, index_col='GameID')


# functions ---------------------------------------------------------------------------------------

def find_winner_pgn(pgn:str) -> str: 

    """ given PGN string, returns winner of game -> [White, Black, Draw]. 
    """

    return 'White' if pgn[-1] == '0' else 'Black' if pgn[-1] == '1' else 'Draw'

def pgn_to_board(pgn:str) -> chess.pgn.Game: 

    """ converts PGN from lichess game data to chess.Board object. 
    """

    pgn = re.sub('\\d+\\.\\.\\. ', '', pgn)
    pgn = re.sub('{.*?} ', '', pgn)
    pgn = re.sub('[!|?]', '', pgn)

    pgn = io.StringIO(pgn)
    return chess.pgn.read_game(pgn)




data['Winner'] = [find_winner_pgn(pgn) for pgn in data['PGN']]

test = data.loc[1, 'PGN']
test_fixed = pgn_to_board(test)

test_fixed.Bit