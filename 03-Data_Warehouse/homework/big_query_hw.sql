CREATE OR REPLACE EXTERNAL TABLE `fluid-vector-445118-g3.first_dataset_tl.yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://first-bucket-tl/yellow_tripdata_2024-*.parquet']
);


CREATE OR REPLACE TABLE `fluid-vector-445118-g3.first_dataset_tl.regular_yellow_tripdata` AS
SELECT * FROM `fluid-vector-445118-g3.first_dataset_tl.yellow_tripdata`;

-- Q1
SELECT COUNT(*) FROM `fluid-vector-445118-g3.first_dataset_tl.yellow_tripdata`;

-- Q2
SELECT COUNT(DISTINCT(PULocationID)) FROM `fluid-vector-445118-g3.first_dataset_tl.yellow_tripdata`;
SELECT COUNT(DISTINCT(PULocationID)) FROM `fluid-vector-445118-g3.first_dataset_tl.regular_yellow_tripdata`;

-- Q3
SELECT PULocationID FROM `fluid-vector-445118-g3.first_dataset_tl.regular_yellow_tripdata`;

SELECT PULocationID, DOLocationID FROM `fluid-vector-445118-g3.first_dataset_tl.regular_yellow_tripdata`;

--Q4
SELECT COUNT(VendorID) FROM `fluid-vector-445118-g3.first_dataset_tl.regular_yellow_tripdata` WHERE fare_amount=0 ;

--Q5
CREATE OR REPLACE TABLE `fluid-vector-445118-g3.first_dataset_tl.yellow_partitioned_tripdata`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `fluid-vector-445118-g3.first_dataset_tl.yellow_tripdata`
);

--Q6 retrieve the distinct VendorIDs between tpep_dropoff_timedate 03/01/2024 and 03/15/2024 (inclusive)
SELECT DISTINCT VendorID FROM `fluid-vector-445118-g3.first_dataset_tl.yellow_tripdata`
WHERE tpep_dropoff_datetime BETWEEN TIMESTAMP '2024-03-01 00:00:00' AND TIMESTAMP '2024-03-15 23:59:59';

SELECT DISTINCT VendorID FROM `fluid-vector-445118-g3.first_dataset_tl.yellow_partitioned_tripdata`
WHERE tpep_dropoff_datetime BETWEEN TIMESTAMP '2024-03-01 00:00:00' AND TIMESTAMP '2024-03-15 23:59:59';




SELECT * FROM `fluid-vector-445118-g3.first_dataset_tl.yellow_tripdata` LIMIT 5;













CREATE OR REPLACE TABLE `taxi-rides-ny.nytaxi.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `taxi-rides-ny.nytaxi.fhv_tripdata`;

CREATE OR REPLACE TABLE `taxi-rides-ny.nytaxi.fhv_partitioned_tripdata`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `taxi-rides-ny.nytaxi.yellow_tripdata`
);

SELECT count(*) FROM  `taxi-rides-ny.nytaxi.fhv_nonpartitioned_tripdata`
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');


SELECT count(*) FROM `taxi-rides-ny.nytaxi.fhv_partitioned_tripdata`
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');
