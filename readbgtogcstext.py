from google.cloud import bigquery,storage
from pyspark.sql import SparkSession

from datetime import datetime

from pyspark.sql.functions import *
from pyspark.sql.types import *


# Initialize Spark session
spark = SparkSession.builder \
    .appName("BigQuery to GCS Example") \
    .getOrCreate()

def query_bigquery_to_gcs(project_id, dataset_id, table_id, bucket_name, output_path):
    """
    Query data from BigQuery and save it to Google Cloud Storage (GCS)
    :param project_id: GCP project ID
    :param dataset_id: BigQuery dataset ID
    :param table_id: BigQuery table ID
    :param bucket_name: GCS bucket name
    :param output_path: GCS output path
    """
    # Construct BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Build the SQL query
    query = f"""
    SELECT word,word_count
    FROM `{project_id}.{dataset_id}.{table_id}`
    """
    
    # Execute the query and convert to DataFrame
    df = client.query(query).to_dataframe()
    
    #display(df)
    
    
    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df)
    
    #spark_df=spark_df.withColumn('word_count',col('word_count').cast(StringType()))
    
    
    # Save Spark DataFrame to GCS as CSV
    gcs_url = f"gs://{bucket_name}/{output_path}"
    #spark_df.coalesce(1).write.format("csv").option("header", "true").option("lineSep", "\r\n").mode("overwrite").save(gcs_url)
    spark_df.write.format('text').mode('overwrite').save(gcs_url)
    #spark_df.write.mode('overwrite').csv(gcs_url)
    print(f"Data saved to {gcs_url}")

def rename_gcs_files(source_bucket_name, prefix):
    """
    Read all files in GCS bucket, find files starting with "part-0000", and rename them
    :param bucket_name: GCS bucket name
    :param prefix: Prefix to check (e.g., "part-0000")
    """
    # Initialize GCS client
    client = storage.Client()
    bucket = client.bucket(source_bucket_name)
    # List all blobs in the bucket
    blobs = bucket.list_blobs()
    
    for blob in blobs:
        print(f'list of blobs{blob}')
        old_file_path = blob.name
        old_file_name = old_file_path.split('/')[-1]
        
        # Check if the file name starts with the specified prefix and suffix
        suffix=old_file_name.split('.')[-1]
        if old_file_name.startswith(prefix) and old_file_name.endswith(suffix):
            
            if suffix == 'csv':
                suffix='csv'
            else:
                suffix='txt'
            
            new_file_path = old_file_path.replace(old_file_name, f"Test_output_file{datetime.now():%Y%m%d}.{suffix}")
            new_blob = bucket.blob(new_file_path)
            
            # Copy the file to the new location
            new_blob.rewrite(blob)
            
            # Delete the original file
            blob.delete()
            
            print(f"File renamed from {old_file_path} to {new_file_path}")
            
# Example usage
project_id = "fiery-melody-436217-c7"
dataset_id = "word_count_dataset"
table_id = "wordcount_output"
bucket_name = "my-newbq-bucket1"
output_path = "outputrwbg/datatext"
prefix = 'part-'



# Query data from BigQuery and save it to GCS
query_bigquery_to_gcs(project_id, dataset_id, table_id, bucket_name, output_path)
rename_gcs_files(bucket_name, prefix)