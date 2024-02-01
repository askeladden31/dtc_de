# Module 1 Homework
Untitled.ipynb  
terraform/terraform_basic/main.tf  
terraform/terraform_with_variables/main.tf  
terraform/terraform_with_variables/variables.tf  

# Module 2 Homework
Create a new pipeline: [mage-zoomcamp/magic-zoomcamp/pipelines/green_taxi_etl](mage-zoomcamp/magic-zoomcamp/pipelines/green_taxi_etl)  
Add a data loader block: [mage-zoomcamp/magic-zoomcamp/data_loaders/load_data.py](mage-zoomcamp/magic-zoomcamp/data_loaders/load_data.py)  
Add a transformer block: [mage-zoomcamp/magic-zoomcamp/transformers/transform_data.py](mage-zoomcamp/magic-zoomcamp/transformers/transform_data.py)  
Using a Postgres data exporter: [mage-zoomcamp/magic-zoomcamp/data_exporters/export_to_postgres.py](mage-zoomcamp/magic-zoomcamp/data_exporters/export_to_postgres.py)  
Write your data to a bucket in GCP: [mage-zoomcamp/magic-zoomcamp/data_exporters/export_to_gcs.py](mage-zoomcamp/magic-zoomcamp/data_exporters/export_to_gcs.py)  
Schedule your pipeline: [mage-zoomcamp/magic-zoomcamp/pipelines/green_taxi_etl/triggers.yaml](mage-zoomcamp/magic-zoomcamp/pipelines/green_taxi_etl/triggers.yaml)  

Question 1: answered in output of loader block  
Question 2: answered in output of transformer block  
Question 3: answered in code of transformer block  
Question 4: answer printed in transformer block  
Question 5: answer printed in transformer block  
Question 6: answer found using shell command *gsutil ls gs://dtc-de-course-412311-mage/green_taxi_2020_4Q/ | wc -l*
