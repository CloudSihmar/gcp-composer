'''
Author: @Sandeep Sihmar

This script is created to demo below concepts
  1. Create Spark session on Dataproc cluster
  2. Read CSV data from specified GCS bucket
  3. Apply Transformations to group and aggregate data
  4. Write resultant data to BigQuery Table
'''

# Import required modules and packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# Define temporary GCS bucket for Dataproc to write its process data
bucket = 'sandeep-dataproc'
spark.conf.set('temporaryGcsBucket', bucket)

# Read data into Spark dataframe from CSV file available in GCS bucket
df = spark.read.option("header", True).csv('gs://sandeep-dataproc/customers-100.csv')

# Since the requirement is to transform the data, let's assume we want to select certain columns
# and perform any necessary transformations.
# For demonstration, let's assume we want to filter customers from a specific country and transform the date format.

# Example transformation: Selecting specific columns and converting date format
transformed_df = df.select(
    col('Index'),
    col('Customer Id'),
    col('First Name'),
    col('Last Name'),
    col('Company'),
    col('City'),
    col('Country'),
    col('Phone 1'),
    col('Phone 2'),
    col('Email'),
    col('Subscription Date'),
    col('Website')
).withColumnRenamed('Subscription Date', 'Subscription_Date')

# You can add any specific transformation logic as per your requirement.
# For now, let's just filter data for customers from a specific country, say 'Chile'.

filtered_df = transformed_df.filter(col('Country') == 'Chile')

# Writing the data to BigQuery
filtered_df.write.format('bigquery') \
    .option('table', 'customer_dataset.customer_data') \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .save()

spark.stop()
