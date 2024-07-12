import csv
import io
import random
from google.cloud import storage, bigquery
import uuid
import logging
import os
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv("BUCKET_NAME", "alt-ordes-demo")

# Configure logging
logging.basicConfig(level=logging.INFO)


# process to generate orders
def custom_generate_orders(num_orders=1000):
    orders = []

    for order_id in range(1, num_orders + 1):
        customer_id = random.randint(1000, 9999)
        product_id = random.randint(1, 100)
        quantity = random.randint(1, 10)
        total_amount = round(random.uniform(10, 100), 2)

        order = (order_id, customer_id, product_id, quantity, total_amount)
        orders.append(order)

    return orders    

# a function to write orders to a gcs bucket
def upload_to_gcs(bucket_name, file_name, data):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        unique_id = str(uuid.uuid4())[:8]  # Generates a random 8-character UUID
        
        full_file_name = f"{file_name}_{unique_id}.csv"

        # Convert list of lists to CSV string
        csv_string = io.StringIO()
        csv_writer = csv.writer(csv_string)
        csv_writer.writerow(['OrderID', 'CustomerID', 'ProductID', 'Quantity', 'TotalAmount'])
        csv_writer.writerows(data)

        # Upload the CSV to GCS
        blob = bucket.blob(full_file_name)
        blob.upload_from_string(csv_string.getvalue(), content_type='text/csv')

        print(f"Successfully uploaded file {full_file_name}")

        # gs://alt-ordes-demo/customer_orders_75fe68c5.csv
        return f"gs://{bucket_name}/{full_file_name}"
    
    except Exception as e:
        logging.error(f"Failed to upload file to GCS: {e}")
        return None


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

if __name__ == "__main__":
    orders_data = custom_generate_orders()
    file_uri = upload_to_gcs(bucket_name=BUCKET, file_name="customer_orders", data=orders_data)
    
    # if file_uri:
    #     load_to_bigquery(file_uri, dataset_id="alt_school_commerce", table_id="customer_orders")
    # else:
    #     logging.error("File upload failed. Skipping BigQuery load.")
