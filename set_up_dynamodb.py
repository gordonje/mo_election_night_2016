import os
import boto3

# create a boto3 session (should load your stored credentials automatically)
session = boto3.Session()

# create a client for interacting with dynamodb
dynamodb = session.resource('dynamodb')

table = dynamodb.create_table(
    TableName=os.environ['DYNAMO_DB_RESULTS_TABLE'],
    KeySchema=[
                {
            'AttributeName': 'race_type',
            'KeyType': 'HASH' 
        },
        {
            'AttributeName': 'last_updated',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'race_type',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'last_updated',
            'AttributeType': 'N'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 20,
        'WriteCapacityUnits': 5,
    }
)

table.meta.client.get_waiter(
    'table_exists'
).wait(
    TableName=os.environ['DYNAMO_DB_RESULTS_TABLE']
)
