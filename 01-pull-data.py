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


# retrieve HTML for lichess database webpage ------------------------------------------------------

db_url = 'https://database.lichess.org/'

r = requests.get(db_url) 
page_html = r.text 


# extract data download links, then filter --------------------------------------------------------

soup = BeautifulSoup(page_html, features='html.parser')
link_elements = soup.find_all('a')
links = [element.get('href') for element in link_elements]

standard_links = [link for link in links if 'standard/' in link and 'pgn.zst' in link and 'torrent' not in link]

include_years = ('2024', '2025')
filtered_links = [link for link in standard_links if any(year in link for year in include_years)]

final_links = [db_url + link for link in filtered_links]


# for each dataset, only keep games above ELO threshold -------------------------------------------

def parse_lichess_stream(url, elo_req):

    """ given a download link to lichess game data and minimum elo requirements, 
        creates an HTTP stream and filters data to meet requirements, 
        returns a dataframe of filtered game data. 
    """

    ## create response object, which is HTTP stream of game data 
    with requests.get(url, stream=True) as response:

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
                
                if i % 1_000_000 == 0: 
                    print(f'\rReading Line: {i+1:,}', end = '')

                line = line.strip() 

                ## lines containing game metadata (descriptors) characterized by brackets 
                if line.startswith('['): 

                    if line.startswith('[Event'):
                        game_details['Time Control'] = ' '.join(line.split('"')[1].split(' ')[0:2]).strip()
                        game_details['Event Type'] = line.split('"')[1].split(' ')[2].capitalize()

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
                    i += 1

    return final_game_data.reset_index(drop = True)


for link in final_links: 
    game_df = parse_lichess_stream(link, (2200, 2400)) 
    print(game_df.head())
    break 

game_df.to_csv('test.csv')