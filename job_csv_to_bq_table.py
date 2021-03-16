## Job 1
print('Job 1')
from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName('Automated Data Engineer Workflow') \
.config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest.jar') \
.config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.18.0") \
.getOrCreate()

# variables
project_id = 'thatistoomuchdata'
gcs_bucket = project_id + '-data'
train_data_path = 'gs://thatistoomuchdata-data/transaction_data_train.csv'
table_name = project_id + '-raw.transaction_data_workflow'
table_name = table_name.replace('-', '_')

df_transaction_data_from_csv = spark \
  .read \
  .option ("inferSchema" , "true") \
  .option ("header" , "true") \
  .csv (train_data_path)

df_transaction_data_from_csv.write \
.format("bigquery") \
.option("table", table_name) \
.option("temporaryGcsBucket", gcs_bucket) \
.mode('overwrite') \
.save()