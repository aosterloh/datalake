from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

path_to_predict_csv = 'gs://thatistoomuchdata-data/transaction_data_test.csv'
project_id = 'thatistoomuchdata'
gcs_bucket = project_id + '-data'
model_path = 'gs://' + gcs_bucket + '/model/'
table_name = project_id + '-annotated.transaction_data_workflow'
table_name = table_name.replace('-', '_')

spark = SparkSession.builder \
.appName('Automated Data Scientist Workflow') \
.config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest.jar') \
.config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.18.0") \
.getOrCreate()

# Load the test data into a spark dataframe 
# thatistoomuchdata:thatistoomuchdata_raw.transaction_data_train
df_test_data = spark.read \
.format("bigquery") \
.option("table", "thatistoomuchdata:thatistoomuchdata_raw.transaction_data_train") \
.load()
df_test_data = df_test_data.drop('transactionID')
df_test_data.cache()



# Load the previously trained pipeline model 

loaded_pipeline_model = PipelineModel.load('gs://thatistoomuchdata-data/model/')
# Run predictions on the test data
path_to_predict_csv = "gs://thatistoomuchdata-data/transaction_data_test.csv"
df_transaction_data_predict_from_csv = spark \
.read \
.option("inferSchema" , "true") \
.option("header" , "true") \
.csv(path_to_predict_csv)
df_transaction_data_predict_from_csv.printSchema()
predictions = loaded_pipeline_model.transform(df_transaction_data_predict_from_csv)

# Save the following fields into BQ table: 'type', 'amount', 'oldbalanceOrg', 'newbalanceOrig', 'isFraud', 'transactionID', 'prediction' 
# Use the bq table name defined previously {table_name}
predictions.write \
.format("bigquery") \
.option("table", table_name) \
.option("temporaryGcsBucket", gcs_bucket) \
.mode('overwrite') \
.save()
