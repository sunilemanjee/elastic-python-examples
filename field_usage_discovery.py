from elasticsearch import Elasticsearch
import argparse

'''
Pass in arguments..example
-username elastic
-password xxxx
-sourceindex kibana_sample_data_flights
-targetindex field-usage-stats-indx
-cloudid myelasticcloudid
'''


# Create argument parser
parser = argparse.ArgumentParser(description='arg parser')

# Add argument
parser.add_argument('-password', type=str, help='password')
parser.add_argument('-username', type=str, help='username')
parser.add_argument('-sourceindex', type=str, help='source index')
parser.add_argument('-targetindex', type=str, help='index where results will be stored')
parser.add_argument('-cloudid', type=str, help='cloud id of your es deployment')

# Parse arguments
args = parser.parse_args()

# Access argument by name
ELASTIC_PASSWORD = args.password

# Define the index to query
INDEX_NAME = args.sourceindex

##index where results will be stored
FUS_INDEX_NAME = args.targetindex

ELASTIC_USER = args.username

# Found in the 'Manage Deployment' page
CLOUD_ID = args.cloudid

# Create the client instance
es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD)
)

# Successful response!
es.info()

query = {
    "fields": ["*"],
    "filter_path": "kibana_sample_data_flights.shards.stats.fields.*.any"
}

# Define the fields to include in the response
fields = ["*"]

# Define the filter path to use
filter_path = "kibana_sample_data_flights.shards.stats.fields.*.any"

# Perform the query
response = es.indices.field_usage_stats(index=INDEX_NAME, fields=fields, filter_path=filter_path)

# response = es.indices.field_usage_stats(index='kibana_sample_data_flights')

# Assuming that the document you provided is stored in the `doc` variable
field_stats = response['kibana_sample_data_flights']['shards']
field_totals = {}
for shard in field_stats:
    for field, stats in shard['stats']['fields'].items():
        if field not in ['_id', '_source']:
            if field not in field_totals:
                field_totals[field] = 0
            field_totals[field] += stats['any']

print(field_totals)

# Create document to be indexed
document = {
    "index_name": FUS_INDEX_NAME,
    **field_totals
}

print(document)

response = es.index(index=FUS_INDEX_NAME, document=document)
