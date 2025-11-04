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

CREATE DATABASE IF NOT EXISTS snowflake_intelligence;
CREATE SCHEMA IF NOT EXISTS snowflake_intelligence.agents;
GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE ACCOUNTADMIN;

CREATE SEMANTIC VIEW yellow_trip_analysis

  TABLES (
    trips AS FCT_YELLOW_TRIPS
      PRIMARY KEY (VENDORID, PICKUP_TIME)
      COMMENT = 'Fact table containing yellow taxi trip records',
    
    vendors AS DIM_TBL_VENDORS
      PRIMARY KEY (VENDOR_CODE)
      COMMENT = 'Vendor reference data',
    
    rates AS DIM_TBL_RATES
      PRIMARY KEY (RATE_CODE)
      COMMENT = 'Rate code reference data',
    
    payments AS DIM_TBL_PAYMENTS
      PRIMARY KEY (PAYMENT_CODE)
      COMMENT = 'Payment type reference data',
    
    zones AS DIM_TBL_ZONES
      PRIMARY KEY (LOCATIONID)
      COMMENT = 'Zone and borough reference data'
  )

  RELATIONSHIPS (
    trip_to_vendor AS trips (VENDORID) REFERENCES vendors (VENDOR_CODE),
    trip_to_rate AS trips (RATECODEID) REFERENCES rates (RATE_CODE),
    trip_to_payment AS trips (PAYMENT_TYPE) REFERENCES payments (PAYMENT_CODE),
    trip_to_pickup_zone AS trips (PICKUP_ZONE) REFERENCES zones (LOCATIONID),
    trip_to_dropoff_zone AS trips (DROPOFF_ZONE) REFERENCES zones (LOCATIONID)
  )

  FACTS (
    trips.trip_id AS CONCAT(VENDORID, '-', PICKUP_TIME),
    trips.total_charges AS FARE_AMOUNT + EXTRA + MTA_TAX + TIP_AMOUNT + TOLLS_AMOUNT + IMPROVEMENT_SURCHARGE + CONGESTION_SURCHARGE + AIRPORT_FEE + CBD_CONGESTION_FEE
      COMMENT = 'Total charges including all surcharges and fees'
  )

  DIMENSIONS (
    vendors.vendor_name AS VENDOR_DESCRIPTION
      COMMENT = 'Name of the vendor',
    
    rates.rate_description AS RATE_DESCRIPTION
      COMMENT = 'Description of the rate code',
    
    payments.payment_description AS PAYMENT_DESCRIPTION
      COMMENT = 'Description of the payment type',
    
    zones.pickup_borough AS zones.BOROUGH
      COMMENT = 'Borough of pickup location',
    
    trips.pickup_date AS DATE(PICKUP_TIME)
      COMMENT = 'Date of pickup',
    
    trips.pickup_hour AS HOUR(PICKUP_TIME)
      COMMENT = 'Hour of pickup time'
  )

  METRICS (
    trips.total_trips AS COUNT(*)
      COMMENT = 'Total number of trips',
    
    trips.average_fare AS AVG(FARE_AMOUNT)
      COMMENT = 'Average fare amount per trip',
    
    trips.average_tip AS AVG(TIP_AMOUNT)
      COMMENT = 'Average tip amount per trip',
    
    trips.average_distance AS AVG(TRIP_DISTANCE)
      COMMENT = 'Average trip distance'
  )

  COMMENT = 'Semantic view for analyzing yellow taxi trip data';
