# Module 1 Homework
[Untitled.ipynb](Untitled.ipynb)  
[terraform/terraform_basic/main.tf](terraform/terraform_basic/main.tf)  
[terraform/terraform_with_variables/main.tf](terraform/terraform_with_variables/main.tf)  
[terraform/terraform_with_variables/variables.tf](terraform/terraform_with_variables/variables.tf)  

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

# Module 3 Homework
Ingestion script: [ingest_data.py](ingest_data.py)  

Question 1: `bq show --format=json green_taxi_2022.green_tripdata | jq ".numRows"`  
Question 2:  
```
SELECT COUNT(DISTINCT PULocationID) FROM `dtc-de-course-412311.green_taxi_2022.green_tripdata`   
SELECT COUNT(DISTINCT PULocationID) FROM `dtc-de-course-412311.green_taxi_2022.external_green_tripdata`
```  
Question 3:  
```
SELECT COUNT(1) FROM `green_taxi_2022.green_tripdata` WHERE fare_amount = 0
```  
Question 4:  
```
create or replace table `green_taxi_2022.green_tripdata_partitioned`
partition by date(lpep_pickup_datetime)
cluster by PUlocationID
as select * from `green_taxi_2022.green_tripdata`
```  
Question 5:  
```
SELECT distinct PULocationID FROM `dtc-de-course-412311.green_taxi_2022.green_tripdata_partitioned` 
WHERE TIMESTAMP_TRUNC(lpep_pickup_datetime, DAY) between TIMESTAMP("2022-06-01") and TIMESTAMP("2022-06-30")

SELECT distinct PULocationID FROM `dtc-de-course-412311.green_taxi_2022.green_tripdata` 
WHERE TIMESTAMP_TRUNC(lpep_pickup_datetime, DAY) between TIMESTAMP("2022-06-01") and TIMESTAMP("2022-06-30")
```
Questions 6, 7 & 8: Theoretical knowledge - no code involved.  

# Workshop Homework (dlt)
[workshop_homework.ipynb](workshop_homework.ipynb)  
[workshop_homework.duckdb](workshop_homework.duckdb)  

# Module 4 Homework
dbt Project: [taxi_rides_ny/](taxi_rides_ny/)  
Report: [https://lookerstudio.google.com/s/tU2OTK_FYao](https://lookerstudio.google.com/s/tU2OTK_FYao)  

# Module 5 Homework  
[pyspark.ipynb](pyspark.ipynb)  

# Workshop Homework (risingwave)  
Question 0:  
```
WITH latest AS (SELECT max(tpep_dropoff_datetime) latest_dropoff FROM trip_data)
SELECT taxi_zone.Zone as dropoff_zone, tpep_dropoff_datetime 
 FROM latest, trip_data
 JOIN taxi_zone ON trip_data.DOLocationID = taxi_zone.location_id
 WHERE tpep_dropoff_datetime > latest_dropoff - interval '20 minutes';
```  
Questions 1 & 2:  
```
CREATE MATERIALIZED VIEW max_min_avg_trip_time AS
SELECT taxi_zone_1.Zone as pickup_zone, taxi_zone_2.Zone as dropoff_zone,
 max(tpep_dropoff_datetime - tpep_pickup_datetime) as max_trip_time,
 min(tpep_dropoff_datetime - tpep_pickup_datetime) as min_trip_time,
 avg(tpep_dropoff_datetime - tpep_pickup_datetime) as avg_trip_time,
 COUNT(1) as num_trips
 FROM trip_data
 JOIN taxi_zone as taxi_zone_1 ON trip_data.PULocationID = taxi_zone_1.location_id
 JOIN taxi_zone as taxi_zone_2 ON trip_data.DOLocationID = taxi_zone_2.location_id
 GROUP BY pickup_zone, dropoff_zone;
```
```
WITH t AS (SELECT max(avg_trip_time) AS highest_avg FROM max_min_avg_trip_time)
SELECT *
 FROM t, max_min_avg_trip_time
 WHERE avg_trip_time = highest_avg;
```
Question 3:  
```
WITH latest AS (SELECT max(tpep_pickup_datetime) latest_pickup FROM trip_data)
SELECT taxi_zone.Zone as pickup_zone, COUNT(1) as num_pickups 
 FROM latest, trip_data
 JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
 WHERE tpep_pickup_datetime > latest_pickup - interval '17 hours'
 GROUP BY pickup_zone
 ORDER BY num_pickups DESC
 LIMIT 3;
```  

# Module 6 Homework  
[week6.ipynb](week6.ipynb)
