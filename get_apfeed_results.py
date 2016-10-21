import csv
import requests
import boto3
from bs4 import BeautifulSoup
from dateparser import parse as parsedate
from time import mktime

# switch these two when we get an access key
# url = 'http://enrarchives.sos.mo.gov/APFeed/Apfeed.asmx/GetElectionResults?'
url = 'https://raw.githubusercontent.com/gordonje/MO_votes_2014/master/data/feed_data.xml'

# create and load a fips_lookup dict
fips_lookup = {}
with open('counties_FIPS.csv', 'r') as f:
    reader = csv.DictReader(f)

    for row in reader:
        fips_lookup[row['County_Name']] = row['FIPS']

results_file_name = 'apfeed_results.xml'

# make a get request for results from the APfeed
response = requests.get(url) # also pass in the access key here (once we get it)

# save the response contents locally
with open(results_file_name, 'wb') as f:
    f.write(response.content)

# create a boto3 session (should load your stored credentials automatically)
session = boto3.Session()

# create a soup object for easy parsing of the xml
soup = BeautifulSoup(response.content, 'xml')

# get the last update value and parse it into a datetime object
last_updated = parsedate(
    soup.find('ElectionResults')['LastUpdated'],
    settings={
        'TIMEZONE': 'CST',
        'RETURN_AS_TIMEZONE_AWARE': True,
    }
)

# create a client for interacting with dynamodb
dynamodb = session.resource('dynamodb')
# get the election_results dynamodb table
table = dynamodb.Table('election_results')

# loop over the <TypeRace> tags
for type_race in soup.findAll('TypeRace'):

    # declare a variable to hold the data for a new table item
    item_data = {
        'race_type': type_race.find('Type').text.strip(),
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
                'candidates': [],
            }

            # look up the fips by county name and add the k/v to output
            county_output['fips'] = fips_lookup[
                county_output['name']
            ]

            # loop over the <Party> tags inside the <CountyResults> tag
            for party in results.find_all('Party'):
                # find the <Candidate> tag
                candidate = party.find('Candidate')

                # append candidate dict to candidates list of county_output
                county_output['candidates'].append(
                    {
                        'party': party.find('PartyName').text.strip(),
                        'id': party.find('Candidate').text.strip(),
                        'name': candidate.find('LastName').text.strip(),
                        'votes': candidate.find('YesVotes').text.strip(),
                    }
                )
            # append county_output to counties list of race_output
            race_output['counties'].append(county_output)
        # append race_output to races list of item_data
        item_data['races'].append(race_output)
    table.put_item(Item=item_data)

# create a client for interacting with s3
s3 = session.client('s3')

# upload the results to s3
s3.upload_file(
    results_file_name, 
    "2016-election-results-archive",
    "2014_data.xml"
)