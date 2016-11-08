"""
Parse local copy of APfeed .xml and save to dynamo_db table.
"""
import glob
import os
import apfeed
import boto3

latest = apfeed.get_latest_results()

# sort the xml cache from most recent to earliest
cache = sorted(
    [f for f in glob.glob(os.path.join('.cache', '*.xml'))],
    key=os.path.getmtime,
    reverse=True
)

# read in the previous xml and compare
try:
    previous_xml = cache[0]
except IndexError:
    pass
else:
    with open(previous_xml, 'rb') as f:
        previous = apfeed.ElectionResults(f.read())

    if latest.md5hash != previous.md5hash:
        latest.cache_xml()
        latest.save_to_dynamodb()
        latest.upload_xml_to_s3()
