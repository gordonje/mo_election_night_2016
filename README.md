# mo_election_night_2016
For fetching, parsing and storing data that will power interactive graphics published leading up to and the night of the November 2016 election.

Code for the actual interactive graphics belongs elsewhere.

## Setup

Install the requirements:

```sh
pip install -r requirements.txt
```

You need to have set up an AWS account, created IAM user, generated and downloaded an access key for that user and assigned a permissions policy to the user.

Then, you can configure the Amazon Web Services command-line interface:
```sh
$ aws configure
```

Now, we can create an S3 bucket for archiving the XML results:
```sh
$ aws s3api create-bucket --bucket 2016-election-results-archive
```

And then we can create a DynamoDB instance where we will stored the parsed XML results as JSON:
```sh
$ python set_up_dynamodb
```

