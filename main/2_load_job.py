from google.oauth2 import service_account
from google.cloud import bigquery

# Configuration
SERVICE_ACCOUNT_FILE = "bewerbungsumgebung-3c77bf79d4b7.json"
GCS_URI = "gs://rumble_bewerber_de_daniel/RN-LF-Ereignisse_202502_transformed.csv"
DATASET_ID = "rumble_bewerber_de_daniel"
TABLE_ID = "RN_LF_Ereignisse_202502_transformed"

creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
client = bigquery.Client(credentials=creds)

# Define table reference
table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

# Define schema
schema = [
    bigquery.SchemaField("URL", "STRING"),
    bigquery.SchemaField("Zeitstempel", "TIMESTAMP"),
    bigquery.SchemaField("Remote_Host", "STRING"),
    bigquery.SchemaField("Methode", "STRING"),
    bigquery.SchemaField("HTTP_Version", "STRING"),
    bigquery.SchemaField("Antwortcode", "STRING"),
    bigquery.SchemaField("Bytes", "INTEGER"),
    bigquery.SchemaField("Benoetigte_Zeit_ms", "INTEGER"),
    bigquery.SchemaField("Benutzeragent", "STRING"),
    bigquery.SchemaField("Referent", "STRING"),
    bigquery.SchemaField("Verifizierungsstatus", "STRING"),
    bigquery.SchemaField("Laendercode", "STRING"),
    bigquery.SchemaField("CO2_mg", "FLOAT")
]

# Create table with partitioning
table = bigquery.Table(table_ref, schema=schema)
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="Zeitstempel"
)

# Create table if not exists
client.create_table(table, exists_ok=True)
print(f"Table `{TABLE_ID}` created or already exists.")

# Load job configuration
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1, 
    autodetect=False,  # Use manual schema
    write_disposition="WRITE_TRUNCATE",  # Overwrite existing table data
    max_bad_records=15000  # Omit malformed rows as long as they don't exceed 15000 rows
) 


# Start load job
load_job = client.load_table_from_uri(
    GCS_URI,
    table_ref,
    job_config=job_config
)

load_job.result()
print(f"Loaded {load_job.output_rows} rows into {DATASET_ID}.{TABLE_ID}")
