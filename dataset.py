from google.cloud import bigquery

PROJECT_ID = "internship-project-428207"  

def list_tables(dataset_id):
    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = client.dataset(dataset_id)
    tables = client.list_tables(dataset_ref)
    for table in tables:
        print(f"Table: {table.table_id}")

def query_data(query):
    client = bigquery.Client(project=PROJECT_ID)
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        print(row)

# List tables in dataset
list_tables('securedata_dataset')

# Query data
query = f"""
SELECT * FROM `{PROJECT_ID}.securedata_dataset.cybersecurity_data`
LIMIT 10
"""
query_data(query)
