
CREATE OR REPLACE EXTERNAL TABLE `fluid-vector-445118-g3.trips_data_all.ext_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = [
    'gs://first-bucket-tl/fhv/fhv_tripdata_2019-*.csv.gz'
    ]
); 

CREATE OR REPLACE EXTERNAL TABLE `fluid-vector-445118-g3.trips_data_all.ext_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = [
    'gs://first-bucket-tl/yellow/yellow_tripdata_2019-*.csv.gz',
    'gs://first-bucket-tl/yellow/yellow_tripdata_2020-*.csv.gz'
    ]
); 


CREATE OR REPLACE EXTERNAL TABLE `fluid-vector-445118-g3.trips_data_all.ext_green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = [
    'gs://first-bucket-tl/green/green_tripdata_2019-*.csv.gz',
    'gs://first-bucket-tl/green/green_tripdata_2020-*.csv.gz'
    ]
); 

CREATE OR REPLACE TABLE `fluid-vector-445118-g3.trips_data_all.fhv_tripdata` 
AS SELECT * FROM `fluid-vector-445118-g3.trips_data_all.ext_fhv_tripdata`;

CREATE OR REPLACE TABLE `fluid-vector-445118-g3.trips_data_all.yellow_tripdata` 
AS SELECT * FROM `fluid-vector-445118-g3.trips_data_all.ext_yellow_tripdata`;

CREATE OR REPLACE TABLE `fluid-vector-445118-g3.trips_data_all.green_tripdata` 
AS SELECT * FROM `fluid-vector-445118-g3.trips_data_all.ext_green_tripdata`;


