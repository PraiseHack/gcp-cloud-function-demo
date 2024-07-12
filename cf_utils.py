from google.cloud import bigquery
import logging

def load_to_bigquery(file_uri, dataset_id, table_id):
    try:
        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        job_config = bigquery.LoadJobConfig(
            autodetect = True,
            skip_leading_rows=1,
            source_format = bigquery.SourceFormat.CSV,
        )


        load_job = bigquery_client.load_table_from_uri(file_uri, table_ref, job_config=job_config)
        load_job.result()
        logging.info(f"Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}.")

    except Exception as e:
        logging.error(f"Failed to load data into BigQuery: {e}")