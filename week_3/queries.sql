CREATE OR REPLACE EXTERNAL TABLE gtc-flow-375407.trips_data_all.fhv_2019_external
OPTIONS(
format = 'CSV',
uris = ['gs://dtc_data_lake_gtc-flow-375407/files/fhv_tripdata_2019-*.csv.gz']);

select count(1) from `trips_data_all.fhv_2019`;

CREATE TABLE gtc-flow-375407.trips_data_all.fhv_2019 AS 
SELECT * FROM `trips_data_all.fhv_2019_external`;


SELECT DISTINCT(Affiliated_base_number)
FROM `trips_data_all.fhv_2019`;

SELECT DISTINCT(Affiliated_base_number)
FROM trips_data_all.fhv_2019_external;

SELECT COUNT(1) 
FROM `trips_data_all.fhv_2019`
WHERE PUlocationID IS NULL  AND DOlocationID IS NULL;


CREATE TABLE gtc-flow-375407.trips_data_all.fhv_2019_partioned
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `trips_data_all.fhv_2019`;

SELECT COUNT(DISTINCT(affiliated_base_number)) AS distinct_aff_base_num
FROM `trips_data_all.fhv_2019`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' and '2019-03-31';



SELECT COUNT(DISTINCT(affiliated_base_number)) AS distinct_aff_base_num
FROM `trips_data_all.fhv_2019_partioned`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' and '2019-03-31';


