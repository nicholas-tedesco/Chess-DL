# README ------------------------------------------------------------------------------------------
# 
#   file: 01-pull-data.py
#   goal: gather chess games in PGN format from lichess; only interested in high-ELO games
# 
# -------------------------------------------------------------------------------------------------

# packages and data -------------------------------------------------------------------------------

import pandas as pd 
import requests
from bs4 import BeautifulSoup
import zstandard as zstd 
import io 
import re 
from sqlalchemy import create_engine, text


# functions ---------------------------------------------------------------------------------------

def get_lichess_links(url:str, include_years:list[str]): 

    """ retrieves download links for chess game files from lichess database. 
    """

    r = requests.get(url)
    soup = BeautifulSoup(r.text, features='html.parser')

    link_elements = soup.find_all('a')
    links = [element.get('href') for element in link_elements]

    standard_links = [link for link in links if 'standard/' in link and 'pgn.zst' in link and 'torrent' not in link]
    filtered_links = [link for link in standard_links if any(year in link for year in include_years)]

    final_links = [db_url + link for link in filtered_links]

    return final_links


def parse_lichess_stream(link:str, elo_min1:int, elo_min2:int):

    """ given a download link to lichess game data and minimum elo requirements, 
        creates an HTTP stream and filters data to meet requirements, 
        returns a dataframe of filtered game data. 
    """

    elo_req = [elo_min1, elo_min2]

    ## create response object, which is HTTP stream of game data 
    with requests.get(link, stream=True) as response:

        response.raise_for_status()
        dctx = zstd.ZstdDecompressor()

        ## link downloads compressed data; have to decompress as we go
        with dctx.stream_reader(response.raw) as reader:
            
            buffer = io.TextIOWrapper(reader, encoding='utf-8')

            final_game_data = pd.DataFrame()
            game_details = {} 

            meets_elo = False
            lost_on_time = False 
            skip_line = True 

            i = 0 

            ## file is being processed line by line; one "record" will have many lines 
            for line in buffer:

                line = line.strip() 

                ## lines containing game metadata (descriptors) characterized by brackets 
                if line.startswith('['): 

                    if line.startswith('[Event'):
                        game_details['TimeControl'] = ' '.join(line.split('"')[1].split(' ')[0:2]).strip()
                        game_details['EventType'] = line.split('"')[1].split(' ')[2].capitalize()

                    elif line.startswith('[WhiteElo'): 
                        try: 
                            elo = int(line.split('"')[1])
                            game_details['WhiteElo'] = elo 
                        ## if we're picking up a username, simply continue 
                        except: 
                            pass 

                    elif line.startswith('[BlackElo'): 
                        try: 
                            elo = int(line.split('"')[1])
                            game_details['BlackElo'] = elo 
                        except: 
                            pass

                    elif line.startswith('[Opening'): 
                        game_details['Opening'] = line.split('"')[1]
                    elif line.startswith('[Termination'):
                        game_details['Termination'] = line.split('"')[1]

                ## blank line used to separate metadata:PGN and game1:game2
                elif not line: 
                    continue
                
                ## if non-blank and no brackets, line is PGN. last line of a given game is PGN 
                else: 
                    
                    ## check if game meets requirements 
                    if game_details['WhiteElo'] >= max(elo_req) and game_details['BlackElo'] >= min(elo_req): 
                        meets_elo = True 

                    if game_details['WhiteElo'] >= min(elo_req) and game_details['BlackElo'] >= max(elo_req): 
                        meets_elo = True 

                    if 'Time forfeit' in game_details['Termination']: 
                        lost_on_time = True 

                    ## if so, save 
                    if meets_elo and not lost_on_time: 
                        game_details['PGN'] = line 
                        temp_game_data = pd.DataFrame([game_details]) 
                        final_game_data = pd.concat([final_game_data, temp_game_data])

                    ## reset game-specific trackers 
                    meets_elo = False
                    lost_on_time = False 
                    skip_line = True
                    game_details = {} 

                    ## CLI message 
                    print(f'\r\t- Current Line: {i:,}', end = '')
                    i += 1

    final_game_data.reset_index(drop = True, inplace = True)
    final_game_data.drop(columns = ['Termination'], inplace = True)

    return final_game_data


def init_db(path:str, table:str) -> None: 

    """ initializes sqlite3 database in specified directory. 
    """

    db_path = 'sqlite:///' + path
    engine = create_engine(db_path)
    conn = engine.connect() 

    # if table already exists, drop

    drop_table_query = text(f"""
        DROP TABLE IF EXISTS {table}; 
    """)

    conn.execute(drop_table_query)
    conn.commit() 

    # create new table with proper column specs 

    create_table_query = text(f"""
        CREATE TABLE {table}(
            GameID INTEGER PRIMARY KEY AUTOINCREMENT, 
            TimeControl VARCHAR(30), 
            EventType VARCHAR(30), 
            WhiteElo INT, 
            BlackElo INT, 
            Opening VARCHAR(50), 
            PGN VARCHAR(500)
        );
    """)

    conn.execute(create_table_query) 
    conn.commit() 

    conn.close() 


def insert_into_db(path:str, table:str, data:pd.DataFrame) -> None: 

    """ given chess data, inserts into local database. 
    """

    engine = create_engine('sqlite:///' + path)
    data.to_sql(table, engine, if_exists='append', index=False)
    

# script logic ------------------------------------------------------------------------------------

if __name__ == '__main__': 

    ## get download links for games by month 
    db_url = 'https://database.lichess.org/'
    include_years = ['2024', '2025']

    final_links = get_lichess_links(db_url, include_years)
    
    ## create db to store results
    db_path = '../data/test'
    table_name = 'games'
    init_db(db_path, table_name) 

    ## for each link, parse data and save locally 
    min_elo1 = 2500 
    min_elo2 = 2300 
    for link in final_links: 
        print(f'\n\tStarting iteration for {link}.') 
        game_data = parse_lichess_stream(link, min_elo1, min_elo2)
        insert_into_db(db_path, table_name, game_data) 
    print('')


