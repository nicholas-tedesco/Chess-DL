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
import re


# retrieve HTML for lichess database webpage ------------------------------------------------------

url = 'https://database.lichess.org/'

r = requests.get(url) 
page_html = r.text 


# extract data download links, then filter --------------------------------------------------------

soup = BeautifulSoup(page_html, features='html.parser')
link_elements = soup.find_all('a')
links = [element.get('href') for element in link_elements]

standard_links = [link for link in links if 'standard/' in link and 'pgn.zst' in link and 'torrent' not in link]

include_years = ('2024', '2025')
final_links = [link for link in standard_links if any(year in link for year in include_years)]


# for each dataset, only keep games above ELO threshold -------------------------------------------

test_link = final_links[0]
min_elo = 2200



