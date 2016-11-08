import boto3
import csv
import hashlib
import os
from bs4 import BeautifulSoup
from datetime import datetime
from dateparser import parse as parsedate
from time import mktime
from random import randint

# create and load a fips_lookup dict
fips_lookup = {}
with open('counties_FIPS.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        fips_lookup[row['County_Name']] = row['FIPS']


class ElectionResults(object):
    _soup = None
    _last_updated = None
    _md5hash = None
    _other_hash = None
    _races = None

    def __init__(self, xml):
        self.xml = xml

    @property
    def file_name(self):
        return 'apfeed{dt:%Y-%m-%d_%H-%M-%S}.xml'.format(dt=self.last_updated)

    @property
    def soup(self):
        if not self._soup:
            self._soup = BeautifulSoup(self.xml, 'xml')
        return self._soup            

    @property
    def last_updated(self):
        if not self._last_updated:
            self._last_updated = parsedate(
                self.soup.find('ElectionResults')['LastUpdated'],
                settings={
                    'TIMEZONE': 'CST',
                    'RETURN_AS_TIMEZONE_AWARE': True,
                }
            )

        return self._last_updated

    @property
    def md5hash(self):
        if not self._md5hash:
            self._md5hash = hashlib.md5()
            self._md5hash.update(str(self.soup.find_all('ElectionInfo')))

        return self._md5hash.digest()

    @property
    def other_hash(self):
        if not self._other_hash:
            self._other_hash = hash(str(self.soup.find('ElectionInfo')))

        return self._other_hash

    @property
    def races(self):
        if not self._races:
            self.parse_races()

        return self._races

    def parse_races(self):
        self._races = []
        for election in self.soup.find_all('ElectionInfo'):
            for race_type in election.find_all('TypeRace'):
                type_tag = race_type.find('Type')
                type_name = type_tag.text.strip().replace(' ', '_').lower()
                for race in race_type.find_all('Race'):
                    if type_name == 'ballot_issues':
                        new_results = BallotIssueResults(race, type_name)
                    else:
                        new_results = CandidateRaceResults(race, type_name)
                    new_results.calculate_totals()

                    self._races.append(new_results)

        return self._races

    def cache_xml(self):
        os.path.exists('.cache/') or os.makedirs('.cache/')

        with open(os.path.join('.cache', self.file_name), 'wb') as f:
            f.write(self.xml)

    def save_to_dynamodb(self):
        # create a boto3 session (should load your stored credentials from env)
        session = boto3.Session()

        # create a client for interacting with dynamodb
        dynamodb = session.resource('dynamodb')
        # get the election_results dynamodb table
        table = dynamodb.Table(os.environ['DYNAMO_DB_RESULTS_TABLE'])

        items_to_save = {}

        for race in self.races:
            try:
                items_to_save[race.type]['races'].append(race.data_dict)
            except KeyError:
                items_to_save[race.type] = {
                    'last_updated': int(mktime(self.last_updated.timetuple())),
                    'races': [race.data_dict],
                }

        for race_type, data in items_to_save.iteritems():
            data['race_type'] = race_type
            table.put_item(Item=data)
    
    def upload_xml_to_s3(self):
        # create a boto3 session (should load your stored credentials from env)
        session = boto3.Session()
        
        # create a client for interacting with s3
        s3 = session.client('s3')
        
        cached_file_name = os.path.join('.cache/', self.file_name)

        if not os.path.exists(cached_file_name):
            self.cache_xml

        # upload the xml results to s3
        s3.upload_file(
            cached_file_name, 
            os.environ['S3_BUCKET_NAME'],
            self.file_name,
        )


class RaceResults(object):
    _counties = None
    _reporting_precincts = None
    _total_precincts = None

    def __init__(
            self,
            soup,
            type_name,
            fake=datetime.now().isoformat() > '2016-11-08',
        ):
        self.soup = soup
        self.type = type_name
        self.fake = fake
        self.title = self.soup.find('RaceTitle').text.strip()

    @property
    def data_dict(self):
        if not self._counties:
            self.parse_counties()
        
        data_dict = {}

        for k, v in self.__dict__.iteritems():
            if k not in ['soup', 'fake']:
                data_dict[(k.replace('_', '', 1))] = v

        return data_dict

    @property
    def counties(self):
        if not self._counties:
            self.parse_counties()

        return self._counties

    @property
    def reporting_precincts(self):
        if not self._reporting_precincts:
            self.calculate_totals()

        return self._reporting_precincts

    @property
    def total_precincts(self):
        if not self._total_precincts:
            self.calculate_totals()

        return self._total_precincts

    def calculate_totals(self):
        self._reporting_precincts = 0
        self._total_precincts = 0

        for county in self.counties:
            self._reporting_precincts += county['reporting_precincts']
            self._total_precincts += county['total_precincts']

        return dict(
            reporting_precincts = self._reporting_precincts,
            total_precincts = self._total_precincts,
        )

    def parse_counties(self):
        self._counties = []
        for county in self.soup.find_all('Counties'):

            results_tag = county.find('CountyResults')

            county_output = {
                'name': county.find('CountyName').text.strip(),
                'reporting_precincts': int(
                    results.find('ReportingPrecincts').text.strip()
                ),
                'total_precincts': int(
                    results.find('TotalPrecincts').text.strip()
                ),
            }

            # look up the fips by county name and add the k/v to output
            county_output['fips'] = fips_lookup[
                county_output['name']
            ]

            self._counties.append(county_output)

        return self._counties


class CandidateRaceResults(RaceResults):
    _candidates = None

    def __init__(self, *args, **kwargs):
        super(CandidateRaceResults, self).__init__(*args, **kwargs)

    @property
    def candidates(self):
        if not self._candidates:
            self.parse_candidates()

        return self._candidates

    def calculate_totals(self):
        super(CandidateRaceResults, self).calculate_totals()
        self.parse_candidates()

    def parse_candidates(self):
        cand_dict = {}
        for county in self.counties:
            for candidate in county['candidates']:
                try:
                    cand_dict[candidate['id']]
                except KeyError:
                    cand_dict[candidate['id']] = candidate.copy()
                else:
                    cand_dict[candidate['id']]['votes'] += candidate['votes']
        self._candidates = [v for v in cand_dict.itervalues()]

    def parse_counties(self):
        self._counties = []
        for county in self.soup.find_all('Counties'):

            results = county.find('CountyResults')

            county_output = {
                'name': county.find('CountyName').text.strip(),
                'reporting_precincts': int(
                    results.find('ReportingPrecincts').text.strip()
                ),
                'total_precincts': int(
                    results.find('TotalPrecincts').text.strip()
                ),
                'candidates': []
            }

            # look up the fips by county name and add the k/v to output
            county_output['fips'] = fips_lookup[
                county_output['name']
            ]

            # loop over the <Party> tags inside the <CountyResults> tag
            for party in results.find_all('Party'):
                # find the <Candidate> tag
                candidate = party.find('Candidate')

                candidate_output = {
                    'party': party.find('PartyName').text.strip(),
                    'id': party.find('CandidateID').text.strip(),
                    'name': candidate.find('LastName').text.strip(),
                }
                if self.fake:
                    candidate_output['votes'] = randint(100,1000)
                else:
                    candidate_output['votes'] = int(candidate.find('YesVotes').text)
                # append candidate dict to candidates list of county_output
                county_output['candidates'].append(candidate_output)

            self._counties.append(county_output)

        return self._counties


class BallotIssueResults(RaceResults):
    _yes_votes = None
    _no_votes = None

    def __init__(self, *args, **kwargs):
        super(BallotIssueResults, self).__init__(*args, **kwargs)

    @property
    def yes_votes(self):
        if not self._yes_votes:
            self.calculate_totals()

        return self._yes_votes

    @property
    def no_votes(self):
        if not self._no_votes:
            self.calculate_totals()

        return self._no_votes

    def calculate_totals(self):
        super(BallotIssueResults, self).calculate_totals()
        self._yes_votes = 0
        self._no_votes = 0
        
        for county in self.counties:
            self._yes_votes += county['yes_votes']
            self._no_votes += county['no_votes']

    def parse_counties(self):
        self._counties = []
        for county in self.soup.find_all('Counties'):

            results = county.find('CountyResults')

            county_output = {
                'name': county.find('CountyName').text.strip(),
                'reporting_precincts': int(
                    results.find('ReportingPrecincts').text.strip()
                ),
                'total_precincts': int(
                    results.find('TotalPrecincts').text.strip()
                ),
            }

            # look up the fips by county name and add the k/v to output
            county_output['fips'] = fips_lookup[
                county_output['name']
            ]

            if self.fake:
                county_output['yes_votes'] = randint(100,1000)
                county_output['no_votes'] = randint(100,1000)
            else:
                county_output['yes_votes'] = int(
                    results.find('Party').find('Candidate').find('YesVotes').text
                )
                county_output['no_votes'] = int(
                    results.find('Party').find('Candidate').find('NoVotes').text
                )

            self._counties.append(county_output)        

        return self._counties


def get_latest_results():
    import requests
    
    url = 'http://enrarchives.sos.mo.gov/APFeed/Apfeed.asmx/GetElectionResults?'

    payload = {'AccessKey': os.environ['APFEED_LIVE_KEY']}

    # make a get request for results from the APfeed
    response = requests.get(url, params=payload)

    return ElectionResults(response.content)