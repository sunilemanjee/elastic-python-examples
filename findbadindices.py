from elasticsearch import Elasticsearch

ELASTIC_USER = "xxxxxx"
ELASTIC_PASSWORD = "xxxx"

# Found in the 'Manage Deployment' page
CLOUD_ID = "xxxxxx"

# Create the client instance
es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD)
)


# Helper function to convert size to bytes
def convert_to_bytes(size):
    size_number = float(size[:-2])
    if size.endswith("kb"):
        size_number *= 1024  # convert kb to bytes
    elif size.endswith("mb"):
        size_number *= 1024 * 1024  # convert mb to bytes
    elif size.endswith("gb"):
        size_number *= 1024 * 1024 * 1024  # convert gb to bytes
    return size_number


# Get a list of all indices
all_indices = es.cat.indices(format="json")

# Filter indices where average shard size is less than 40GB or greater than 60GB
indices_with_specific_shard_size = []
for index in all_indices:
    avg_shard_size = convert_to_bytes(index['pri.store.size']) / int(index['pri'])
    if avg_shard_size < 40*(1024**3):
        indices_with_specific_shard_size.append((index['index'], index['pri.store.size'], index['pri'], "less than 40GB"))
    elif avg_shard_size > 60*(1024**3):
        indices_with_specific_shard_size.append((index['index'], index['pri.store.size'], index['pri'], "greater than 60GB"))


print("Indices where average shard size is less than 40GB or greater than 60GB:")
for index_info in indices_with_specific_shard_size:
    index_name, primary_store_size, shards, shard_size_info = index_info
    print(f"Index Name: {index_name}, Primary Store Size: {primary_store_size}, Number of Shards: {shards}, Shard Size is {shard_size_info}")
