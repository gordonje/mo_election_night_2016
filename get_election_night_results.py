import requests
from datetime import datetime
from bs4 import BeautifulSoup
import xmljson
import json

response = requests.get(
    'https://raw.githubusercontent.com/gordonje/MO_votes_2014/master/data/feed_data.xml'
)

soup = BeautifulSoup(response.content, 'xml')

print soup.prettify()