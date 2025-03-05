# README ------------------------------------------------------------------------------------------
# 
#   file: 01-pull-data.py
#   goal: gather chess games in PGN format from lichess; only interested in high-ELO games
# 
# -------------------------------------------------------------------------------------------------

# packages and data -------------------------------------------------------------------------------

import pandas as pd 
import requests
import beautifulsoup4 as bs4


# lichess API -------------------------------------------------------------------------------------

url = 'https://www.lichess.org/api/games'

params = {
    'rated': 'true', 
    'eloMin': 2000, 
    'max': 10
}

r = requests.get(url, params=params) 
print(r.text)