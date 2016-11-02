"""
Process for fetching, parsing and saving results data from the SoS APfeed.
"""
import os
import csv
import requests
import boto3
from bs4 import BeautifulSoup
from datetime import datetime
from dateparser import parse as parsedate
from time import mktime

url = 'http://enrarchives.sos.mo.gov/APFeed/Apfeed.asmx/GetElectionResults?'

# create and load a fips_lookup dict
fips_lookup = {}
with open('counties_FIPS.csv', 'r') as f:
    reader = csv.DictReader(f)

    for row in reader:
        fips_lookup[row['County_Name']] = row['FIPS']

# use the test key, unless it's election day
if datetime.now().date().isoformat() == '2016-11-08':
    payload = {'AccessKey': os.environ['APFEED_LIVE_KEY']}
else:
    payload = {'AccessKey': os.environ['APFEED_TEST_KEY']}

# make a get request for results from the APfeed
response = requests.get(
    url,
    params=payload
)

# save the response contents locally
results_file_name = 'apfeed_results.xml'

with open(results_file_name, 'wb') as f:
    f.write(response.content)

# create a boto3 session (should load your stored credentials from env)
session = boto3.Session()

# create a soup object for easy parsing of the xml
soup = BeautifulSoup(response.content, 'xml')

# get the last update value and parse it into a datetime object
last_updated = parsedate(
    soup.find('ElectionResults')['LastUpdated'],
    settings={
        'TIMEZONE': 'GMT',
        'TO_TIMEZONE': 'CST',
        'RETURN_AS_TIMEZONE_AWARE': True,
    }
)

# create a client for interacting with dynamodb
dynamodb = session.resource('dynamodb')
# get the election_results dynamodb table
table = dynamodb.Table(os.environ['DYNAMO_DB_RESULTS_TABLE'])

items_to_save = []

# loop over the <TypeRace> tags
for type_race in soup.findAll('TypeRace'):

    # declare a variable to hold the data for a new table item
    item_data = {
        'race_type': type_race.find('Type').text.strip().replace(' ', '_').lower(),
        # convert late_updated to posix time for fast sorting
        'last_updated': int(mktime(last_updated.timetuple())),
        'races': [],
    }

    # loop over <Race> tags inside each <TypeRace>
    for race in type_race.find_all('Race'):
        race_output = {
            'title': race.find('RaceTitle').text.strip(),
            'counties': [],
        }
        
        # loop over the <Counties> tags
        for county in race.find_all('Counties'):
            # find the <CountyResults>
            results = county.find('CountyResults')

            county_output = {
                'name': county.find('CountyName').text.strip(),
                'reporting_precincts': results.find('ReportingPrecincts').text.strip(),
                'total_precincts': results.find('TotalPrecincts').text.strip(),
            }

            # look up the fips by county name and add the k/v to output
            county_output['fips'] = fips_lookup[
                county_output['name']
            ]

            # if it's a ballot issue, add key/values for yes and no votes
            if item_data['race_type'] == "ballot_issues":
                county_output['yes_votes'] = int(
                    results.find('Party').find('Candidate').find('YesVotes').text
                )
                county_output['no_votes'] = int(
                    results.find('Party').find('Candidate').find('NoVotes').text
                )
            # otherwise keep a list of all the candidates
            else:
                county_output['candidates'] = []

                # loop over the <Party> tags inside the <CountyResults> tag
                for party in results.find_all('Party'):
                    # find the <Candidate> tag
                    candidate = party.find('Candidate')

                    # append candidate dict to candidates list of county_output
                    county_output['candidates'].append(
                        {
                            'party': party.find('PartyName').text.strip(),
                            'id': party.find('CandidateID').text.strip(),
                            'name': candidate.find('LastName').text.strip(),
                            'votes': int(candidate.find('YesVotes').text),
                        }
                    )
            # append county_output to counties list of race_output
            race_output['counties'].append(county_output)
        # append race_output to races list of item_data
        item_data['races'].append(race_output)
    items_to_save.append(item_data)

for item in items_to_save:
    for race in item['races']:
        if item['race_type'] == 'ballot_issues':
            race['yes_votes'] = 0
            race['no_votes'] = 0

            for county in race['counties']:
                race['yes_votes'] += county['yes_votes']
                race['no_votes'] += county['no_votes']
        elif race['title'] == 'State Auditor':
            cand_dict = {}
            for county in race['counties']:
                for candidate in county['candidates']:
                    try:
                        cand_dict[candidate['id']]
                    except KeyError:
                        cand_dict[candidate['id']] = candidate.copy()
                    else:
                        cand_dict[candidate['id']]['votes'] += candidate['votes']
            race['candidates'] = [v for v in cand_dict.itervalues()]

    table.put_item(Item=item)

# create a client for interacting with s3
s3 = session.client('s3')

# upload the xml results to s3
s3.upload_file(
    results_file_name, 
    os.environ['S3_BUCKET_NAME'],
    "2014_data.xml"
)