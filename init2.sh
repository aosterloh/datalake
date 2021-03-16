#!/bin/bash

# set variables
export project_id=thatistoomuchdata
export region=europe-west1
export workflow_id=automate-bank-transaction-workflow
export bucket_name=thatistoomuchdata-data 
export cluster_name=spark-workflow-cluster
export num_workers=3


gcloud config set project $project_id

#delete datasets
bq --location europe-west1 rm -r -f -d $project_id:raw
bq --location europe-west1 rm -r -f -d $project_id:annotated

# create datasets
bq --location europe-west1 mk --dataset raw
bq --location europe-west1 mk --dataset annotated

#create tables
bq --location europe-west1 mk --table raw.transaction_data_workflow type:string,amount:float,oldbalanceOrg:float,newbalanceOrig:float,isFraud:integer,transactionID:string,prediction:integer
bq --location europe-west1 mk --table annotated.transaction_data_workflow type:string,amount:float,oldbalanceOrg:float,newbalanceOrig:float,isFraud:integer,transactionID:string,prediction:integer

# re-create workflow template
gcloud dataproc workflow-templates delete $workflow_id --region $region
gcloud dataproc workflow-templates create $workflow_id --region $region

#create managed cluster for template above
gcloud beta dataproc workflow-templates set-managed-cluster $workflow_id \
--cluster-name $cluster_name \
--region $region \
--image-version=1.5-ubuntu18 \
--master-machine-type n1-standard-2 \
--master-boot-disk-size=128GB \
--num-workers $num_workers \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size=128GB \
--scopes https://www.googleapis.com/auth/cloud-platform \
--bucket $bucket_name \
--properties spark:spark.jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.18.0.jar,spark:spark=gs://spark-lib/bigquery/spark-bigquery-latest.jar

#cp spark jobs to gcs
gsutil cp job_csv_to_bq_table.py gs://${project_id}-data/workflows/python-scripts/
gsutil cp job_ml_predictions.py gs://${project_id}-data/workflows/python-scripts/

#remove old jobs and add job to workflow template 
gcloud dataproc workflow-templates remove-job $workflow_id --region $region --step-id csv_to_bq
gcloud dataproc workflow-templates remove-job $workflow_id --region $region --step-id predict

gcloud dataproc workflow-templates add-job pyspark \
gs://${project_id}-data/workflows/python-scripts/job_csv_to_bq_table.py \
--region $region \
--step-id csv_to_bq \
--workflow-template $workflow_id

gcloud dataproc workflow-templates add-job pyspark \
gs://${project_id}-data/workflows/python-scripts/job_ml_predictions.py \
--region $region \
--start-after=csv_to_bq \
--step-id predict \
--workflow-template $workflow_id

#run the template 
gcloud dataproc workflow-templates instantiate $workflow_id \
--region $region