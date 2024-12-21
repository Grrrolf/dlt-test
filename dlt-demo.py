import dlt
from dlt.sources.helpers import requests

# Create a dlt pipeline that will load
# pouet production data to the DuckDB destination
pipeline = dlt.pipeline(
    pipeline_name='pouet_prod',
    destination='duckdb',
    dataset_name='main'
)


# Grab pouet.net data from api.pouet.net
data = []
for prod_id in range(1, 101):
    print(f'https://api.pouet.net/v1/prod/?id={prod_id}')
    response = requests.get(f'https://api.pouet.net/v1/prod/?id={prod_id}')
    response.raise_for_status()
    data.append(response.json())


for bla in data:
    print(bla)


# Extract, normalize, and load the data
pipeline.run(data, table_name='pouet_prod')
