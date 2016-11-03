"""
Download the latest results from the APfeed.
"""
import os
import requests
from datetime import datetime

url = 'http://enrarchives.sos.mo.gov/APFeed/Apfeed.asmx/GetElectionResults?'

# use the test key, unless it's election day
if datetime.now().date().isoformat() == '2016-11-08':
    payload = {'AccessKey': os.environ['APFEED_LIVE_KEY']}
else:
    payload = {'AccessKey': os.environ['APFEED_TEST_KEY']}

# make a get request for results from the APfeed
response = requests.get(url, params=payload)

# save the response contents locally (overwrite current results)
with open('apfeed_results.xml', 'wb') as f:
    f.write(response.content)
