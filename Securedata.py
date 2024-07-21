from google.cloud import bigquery
from google.cloud import kms_v1
from google.api_core import exceptions
import time

PROJECT_ID = "internship-project-428207"
KEY_RING_ID = "securedata-keyring"
CRYPTO_KEY_ID = "securedata-key"

def configure_bigquery():
    client = bigquery.Client(project=PROJECT_ID)
    
    # Check if the dataset exists
    dataset_id = f"{PROJECT_ID}.securedata_dataset"
    dataset = bigquery.Dataset(dataset_id)
    try:
        client.get_dataset(dataset)  
        print(f"Dataset {dataset.dataset_id} already exists.")
    except exceptions.NotFound:
        # Create the dataset if it doesn't exist
        dataset.location = "US"  
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset.dataset_id}")

def create_table():
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.securedata_dataset"
    table_id = f"{dataset_id}.cybersecurity_data"

    try:
        table = client.get_table(table_id)  # Check if table exists
        print(f"Table {table.table_id} already exists.")
    except exceptions.NotFound:
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("source_ip", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("destination_ip", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("threat_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("threat_level", "INTEGER", mode="REQUIRED"),
        ]

        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(f"Created table {table.table_id}")

def load_data():
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.securedata_dataset.cybersecurity_data"
    rows_to_insert = [
        {"timestamp": "2023-01-01T00:00:00Z", "source_ip": "192.168.1.1", "destination_ip": "192.168.1.2", "threat_type": "malware", "threat_level": 5},
        {"timestamp": "2023-01-01T01:00:00Z", "source_ip": "192.168.1.3", "destination_ip": "192.168.1.4", "threat_type": "phishing", "threat_level": 3},
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

def encrypt_data(plaintext):
    client = kms_v1.KeyManagementServiceClient()
    key_name = client.crypto_key_path(PROJECT_ID, "global", KEY_RING_ID, CRYPTO_KEY_ID)

    plaintext_bytes = plaintext.encode("utf-8")

    response = client.encrypt(
        request={"name": key_name, "plaintext": plaintext_bytes}
    )

    encrypted_bytes = response.ciphertext
    return encrypted_bytes

def run_performance_test(query):
    client = bigquery.Client(project=PROJECT_ID)
    start_time = time.time()

    query_job = client.query(query)

    results = query_job.result()
    end_time = time.time()

    duration = end_time - start_time
    print(f"Query completed in {duration:.2f} seconds")
    
    return duration

def document_best_practices():
    best_practices = """
    ## BigQuery Performance Best Practices

    1. **Use Partitioning and Clustering**: Partition tables by date or other relevant columns to reduce the amount of data scanned. Use clustering to improve query performance on selective queries.
    
    2. **Avoid SELECT * Queries**: Only select the columns you need to reduce the amount of data processed.
    
    3. **Use Appropriate Data Types**: Choose the most efficient data types for your columns to save storage and improve query performance.
    
    4. **Optimize Joins**: Use WITH clauses and ensure you are joining on indexed columns. Avoid cross joins unless necessary.
    
    5. **Monitor Query Execution Plans**: Use the query execution plan to understand how your queries are executed and identify bottlenecks.
    
    6. **Cache Results**: Use the cache wherever possible to avoid re-running expensive queries.
    
    7. **Use Table Wildcards**: For queries that need to scan multiple tables, use table wildcards to minimize the number of tables scanned.
    
    8. **Limit the Amount of Data Processed**: Use filters and conditions to process only the necessary data.
    
    9. **Understand Pricing and Quotas**: Be aware of BigQuery’s pricing model and quotas to avoid unexpected costs.
    
    10. **Regularly Review and Optimize**: Regularly review your queries and schema to optimize for performance based on the latest usage patterns.
    """

    with open("best_practices.md", "w") as file:
        file.write(best_practices)

def main():
    print("Configuring BigQuery...")
    configure_bigquery()

    print("Creating table...")
    create_table()

    print("Loading data into BigQuery...")
    load_data()

    sample_query = f"""
    SELECT threat_type, COUNT(*) as threat_count
    FROM `{PROJECT_ID}.securedata_dataset.cybersecurity_data`
    GROUP BY threat_type
    ORDER BY threat_count DESC
    LIMIT 10
    """

    print("Running performance test...")
    duration = run_performance_test(sample_query)
    print(f"Performance test completed. Duration: {duration:.2f} seconds")

    print("Encrypting data...")
    encrypted_text = encrypt_data("sensitive data")
    print(f"Encrypted text: {encrypted_text}")

    print("Documenting best practices...")
    document_best_practices()
    print("Best practices documented in best_practices.md")

if __name__ == "__main__":
    main()
