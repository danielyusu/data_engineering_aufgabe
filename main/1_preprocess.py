"""
In the original files, we have 4 lines of meta data that disrupt the overall structure of CSV file and prevent being ingested to BigQuery.

For this reason, those 4 lines at the top must be removed in order to ingest data at BigQuery
and also Zeitstempel must be converted to a proper TIMESTAMP format.
"""

import polars as pl
from google.cloud import storage
from google.oauth2 import service_account
from io import StringIO
from tqdm import tqdm

def preprocess_blob(
    source_bucket_name,
    source_blob_name,
    dest_bucket_name,
    dest_blob_name,
    skip_lines=4,
    chunk_size=2_000_000  # Adjust chunk size as needed
):
    creds = service_account.Credentials.from_service_account_file(
        "bewerbungsumgebung-3c77bf79d4b7.json"
    )
    client = storage.Client(credentials=creds, project=creds.project_id)

    source_blob = client.bucket(source_bucket_name).blob(source_blob_name)
    dest_blob = client.bucket(dest_bucket_name).blob(dest_blob_name)
    dest_blob.content_type = "text/csv"

    with source_blob.open("r", encoding="utf-8") as src_file, \
         dest_blob.open("w", encoding="utf-8") as dst_file:

        # Skip metadata lines
        for _ in range(skip_lines):  # Skip everything before header row
            next(src_file)
        header = next(src_file).strip()  # This is the actual column header
        print(f"Header: {header}")

        buffer = []
        total_rows = 0
        header_written = False

        for line in tqdm(src_file, desc="Processing lines"):
            buffer.append(line)
            if len(buffer) >= chunk_size:
                df = _process_chunk(buffer, header)
                df.write_csv(dst_file, include_header=not header_written)
                header_written = True
                total_rows += df.height
                buffer.clear()

        # Final chunk
        if buffer:
            df = _process_chunk(buffer, header)
            df.write_csv(dst_file, include_header=not header_written, quote_style="always")
            total_rows += df.height

    print(f"Finished processing {total_rows} rows.")

def _process_chunk(buffer, header):
    # Prepend header to buffer
    csv_text = header + "\n" + "".join(buffer)

    df = pl.read_csv(
        StringIO(csv_text),
        separator=",",
        ignore_errors=True,
    )

    # Ensure Zeitstempel is parsed correctly
    if "Zeitstempel" not in df.columns:
        raise ValueError("Zeitstempel column missing in chunk")
    
    # Rename columns to be BigQuery-safe
    df = df.rename({
        "Remote-Host": "Remote_Host",
        "HTTP-Version": "HTTP_Version",
        "Benötigte Zeit (ms)": "Benoetigte_Zeit_ms",
        "Ländercode": "Laendercode",
        "CO2 (mg)": "CO2_mg"
    })

    df = df.with_columns([
        pl.col("Zeitstempel").str.strip_chars().str.strptime(pl.Datetime, "%d.%m.%Y, %H:%M:%S", strict=False).alias("Zeitstempel"),
    ])

    return df

preprocess_blob(
    source_bucket_name="raw_screaming_frog_data",
    source_blob_name="RN-LF-Ereignisse_202502.csv",
    dest_bucket_name="rumble_bewerber_de_daniel",
    dest_blob_name="RN-LF-Ereignisse_202502_transformed.csv",
)
