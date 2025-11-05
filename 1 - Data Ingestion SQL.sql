-- CREATE DATABASE AND SCHEMA
-- THIS IS NEEDED FOR THE LLM MODELS TO WORK
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
CREATE OR REPLACE DATABASE NYC_TAXI;
CREATE OR REPLACE SCHEMA YELLOW_TAXI;

-- ENSURE CONTEXT SETTINGS ARE RIGHT
USE DATABASE NYC_TAXI;
USE SCHEMA YELLOW_TAXI;

--CREATE FILE FORMAT

CREATE OR REPLACE FILE FORMAT TAXI_CSV
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '\"';
;

-- CREATE EXTERNAL STAGE
CREATE OR REPLACE STAGE STG_NYC_TAXI_TRIPS
URL = 's3://snowflake-demo-inexus/'
FILE_FORMAT = TAXI_CSV;

-- CREATE / LOAD ALL TABLES FROM STAGING
CREATE OR REPLACE TABLE DIM_TBL_VENDORS (
VENDOR_CODE VARCHAR,
VENDOR_DESCRIPTION VARCHAR
);

COPY INTO DIM_TBL_VENDORS
FROM '@STG_NYC_TAXI_TRIPS/tables/VENDORS.csv'
FILE_FORMAT = TAXI_CSV
;

CREATE OR REPLACE TABLE DIM_TBL_ZONES (
LOCATIONID VARCHAR,
BOROUGH VARCHAR,
ZONE VARCHAR,
SERVICE_ZONE VARCHAR
);

COPY INTO DIM_TBL_ZONES
FROM '@STG_NYC_TAXI_TRIPS/tables/ZONES.csv'
FILE_FORMAT = TAXI_CSV
;

CREATE OR REPLACE TABLE DIM_TBL_RATES (
RATE_CODE VARCHAR,
RATE_DESCRIPTION VARCHAR
);

COPY INTO DIM_TBL_RATES
FROM '@STG_NYC_TAXI_TRIPS/tables/RATES.csv'
FILE_FORMAT = TAXI_CSV
;

CREATE OR REPLACE TABLE DIM_TBL_PAYMENTS (
PAYMENT_CODE VARCHAR,
PAYMENT_DESCRIPTION VARCHAR
);

COPY INTO DIM_TBL_PAYMENTS
FROM '@STG_NYC_TAXI_TRIPS/tables/PAYMENT_TYPES.csv'
FILE_FORMAT = TAXI_CSV
;

CREATE OR REPLACE TABLE FCT_YELLOW_TRIPS (
    VENDORID VARCHAR, 
    PICKUP_TIME TIMESTAMP, 
    DROPOFF_TIME TIMESTAMP, 
    passenger_count NUMBER,
    trip_distance FLOAT, 
    RatecodeID VARCHAR, 
    PICKUP_ZONE VARCHAR,
    DROPOFF_ZONE VARCHAR,
    payment_type VARCHAR, 
    fare_amount FLOAT, 
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    cbd_congestion_fee FLOAT
    );

COPY INTO FCT_YELLOW_TRIPS
FROM '@STG_NYC_TAXI_TRIPS/tables/TRIPS.csv'
FILE_FORMAT = TAXI_CSV
;

-------------------------------------------------------------------------------------------------------------------------------------------
--*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*----*-*--
-------------------------------------------------------------------------------------------------------------------------------------------------

