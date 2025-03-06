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

def parse_lichess_stream(url, min_elo):

    ## create response object, streaming data from download link 
    with requests.get(url, stream=True) as response:

        response.raise_for_status()
        dctx = zstd.ZstdDecompressor()

        ## decompress file as it is read 
        with dctx.stream_reader(response.raw) as reader:
            
            buffer = io.TextIOWrapper(reader, encoding='utf-8')

            keep_white_elo = [] 
            keep_black_elo = [] 
            keep_pgn = [] 

            game_details = {} 

            keep = False
            skip = True

            i = 0 

            ## file is being processed line by line; one "record" will have many lines 
            for line in buffer:
                
                if i % 1_000_000 == 0: 
                    print(f'\rReading Line: {i+1:,}', end = '')

                line = line.strip() 

                ## game metadata (descriptors) characterized by brackets 
                if line.startswith('['): 

                    if 'WhiteElo' in line: 
                        try: 
                            elo = int(line.split('"')[1])
                            game_details['WhiteElo'] = elo 
                            if elo >= min_elo: 
                                keep = True 
                        ## if we're picking up a username, simply continue 
                        except: 
                            pass 

                    elif 'BlackElo' in line: 
                        try: 
                            elo = int(line.split('"')[1])
                            game_details['BlackElo'] = elo 
                            if elo >= min_elo: 
                                keep = True 
                        except: 
                            pass

                ## first non-blank line after metadata is PGN... however, one blank line separating
                elif line: 
                    if keep: 
                        game_details['PGN'] = line 

                else: 

                    ## skip over blank line separating metadata / PGN 
                    if skip:
                        skip = False
                        continue 

                    ## if blank line is after PGN, signals boundary between prev and next games
                    if keep: 
                        keep_white_elo.append(game_details['WhiteElo'])
                        keep_black_elo.append(game_details['BlackElo'])
                        keep_pgn.append(game_details['PGN'])

                    keep = False 
                    skip = True
                    game_details = {} 
                    i += 1
                
    ## format results as dataframe 
    output = pd.DataFrame({'WhiteElo': keep_white_elo, 'BlackElo': keep_black_elo, 'PGN': keep_pgn})
    return output 


for link in final_links: 
    game_df = parse_lichess_stream(link, 2000) 
    print(game_df.head())
    break 

game_df.to_csv('test.csv')