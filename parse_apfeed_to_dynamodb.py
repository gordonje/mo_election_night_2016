"""
Parse local copy of APfeed .xml and save to dynamo_db table.
"""
import os
import csv
import boto3
from bs4 import BeautifulSoup
from dateparser import parse as parsedate
from time import mktime

# create and load a fips_lookup dict
fips_lookup = {}
with open('counties_FIPS.csv', 'r') as f:
    reader = csv.DictReader(f)

    for row in reader:
        fips_lookup[row['County_Name']] = row['FIPS']

# read the saved results into BeautifulSoup
with open('apfeed_results.xml', 'rb') as f:
    soup = BeautifulSoup(f.read(), 'xml')

# create a boto3 session (should load your stored credentials from env)
session = boto3.Session()

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

race_types = {}

# loop over the <ElectionInfo> tags:
for election in soup.find_all('ElectionInfo'):
    # loop over the <TypeRace> tags
    for type_race in election.find_all('TypeRace'):
        type_name = type_race.find('Type').text.strip().replace(' ', '_').lower()

        # if the race_type isn't in our dict yet, add it
        if type_name not in race_types:
            race_types[type_name] = {
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
                if type_name == "ballot_issues":
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
            # append race_output to races list of the race_type
            race_types[type_name]['races'].append(race_output)

# go back over all the races to aggregate vote tallies
for race_type, data in race_types.iteritems():
    for race in data['races']:
        if race_type == 'ballot_issues':
            race['yes_votes'] = 0
            race['no_votes'] = 0

            for county in race['counties']:
                race['yes_votes'] += county['yes_votes']
                race['no_votes'] += county['no_votes']
        else:
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

    # add the race_type string to the data to save
    data['race_type'] = race_type
    table.put_item(Item=data)
