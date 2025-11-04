# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()

import streamlit as st
import snowflake.snowpark as snowpark

# Write directly to the app
st.title("🚕 NYC Yellow Taxi Data Setup")
st.write(
  """This app is designed to do the following:
  Create your database, schema, tables and then load data into them from an S3 bucket. 
  It will display some visualizations
  Most importantly, it will create an important semantic view for you to use AI to undertand your data. 
  """
)

import streamlit as st
import time

# Display spinner while performing a task
with st.spinner("Before we start, click on the databases in the top left corner so you can see the creation of you tables and data"):
    time.sleep(10) # Simulate a long-running task

# -------------------------
# STEP 1: Environment Setup
# -------------------------
st.subheader("🔧 Step 1: Environment Setup")
with st.spinner("Creating file format, and stage..."):
    session.sql("""
        CREATE OR REPLACE FILE FORMAT NYC_TAXI.YELLOW_TAXI.TAXI_CSV
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    """).collect()
    session.sql("""
        CREATE OR REPLACE STAGE NYC_TAXI.YELLOW_TAXI.STG_NYC_TAXI_TRIPS
        URL = 's3://snowflake-demo-inexus/'
        FILE_FORMAT = TAXI_CSV
    """).collect()
st.success("✅ Environment setup complete.")

# -------------------------
# STEP 2: Create & Load Tables
# -------------------------
st.subheader("📦 Step 2: Creating and Loading Tables")
with st.spinner("Creating tables and loading data..."):
    # Vendors
    session.sql("""
        CREATE OR REPLACE TABLE NYC_TAXI.YELLOW_TAXI.DIM_TBL_VENDORS (
            VENDOR_CODE VARCHAR,
            VENDOR_DESCRIPTION VARCHAR
        )
    """).collect()
    session.sql("""
        COPY INTO NYC_TAXI.YELLOW_TAXI.DIM_TBL_VENDORS
        FROM 'NYC_TAXI.YELLOW_TAXI.@STG_NYC_TAXI_TRIPS/tables/VENDORS.csv'
        FILE_FORMAT = TAXI_CSV
    """).collect()

    # Zones
    session.sql("""
        CREATE OR REPLACE TABLE NYC_TAXI.YELLOW_TAXI.DIM_TBL_ZONES (
            LOCATIONID VARCHAR,
            BOROUGH VARCHAR,
            ZONE VARCHAR,
            SERVICE_ZONE VARCHAR
        )
    """).collect()
    session.sql("""
        COPY INTO NYC_TAXI.YELLOW_TAXI.DIM_TBL_ZONES
        FROM '@NYC_TAXI.YELLOW_TAXI.STG_NYC_TAXI_TRIPS/tables/ZONES.csv'
        FILE_FORMAT = TAXI_CSV
    """).collect()

    # Rates
    session.sql("""
        CREATE OR REPLACE TABLE NYC_TAXI.YELLOW_TAXI.DIM_TBL_RATES (
            RATE_CODE VARCHAR,
            RATE_DESCRIPTION VARCHAR
        )
    """).collect()
    session.sql("""
        COPY INTO NYC_TAXI.YELLOW_TAXI.DIM_TBL_RATES
        FROM 'NYC_TAXI.YELLOW_TAXI.@STG_NYC_TAXI_TRIPS/tables/RATES.csv'
        FILE_FORMAT = TAXI_CSV
    """).collect()

    # Payments
    session.sql("""
        CREATE OR REPLACE TABLE NYC_TAXI.YELLOW_TAXI.DIM_TBL_PAYMENTS (
            PAYMENT_CODE VARCHAR,
            PAYMENT_DESCRIPTION VARCHAR
        )
    """).collect()
    session.sql("""
        COPY INTO NYC_TAXI.YELLOW_TAXI.DIM_TBL_PAYMENTS
        FROM '@NYC_TAXI.YELLOW_TAXI.STG_NYC_TAXI_TRIPS/tables/PAYMENT_TYPES.csv'
        FILE_FORMAT = NYC_TAXI.YELLOW_TAXI.TAXI_CSV
    """).collect()

    # Trips
    session.sql("""
        CREATE OR REPLACE TABLE NYC_TAXI.YELLOW_TAXI.FCT_YELLOW_TRIPS (
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
        )
    """).collect()
    session.sql("""
        COPY INTO NYC_TAXI.YELLOW_TAXI.FCT_YELLOW_TRIPS
        FROM '@NYC_TAXI.YELLOW_TAXI.STG_NYC_TAXI_TRIPS/tables/TRIPS.csv'
        FILE_FORMAT = TAXI_CSV
    """).collect()
st.success("✅ Tables created and data loaded.")

# -------------------------
# STEP 3: Create Semantic View
# -------------------------
st.subheader("🧠 Step 3: Creating Semantic View")
with st.spinner("Creating semantic view..."):
    session.sql("ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'").collect()
    session.sql("CREATE DATABASE IF NOT EXISTS snowflake_intelligence").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS snowflake_intelligence.agents").collect()
    session.sql("GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE ACCOUNTADMIN").collect()
    session.sql("""
        CREATE OR REPLACE SEMANTIC VIEW NYC_TAXI.YELLOW_TAXI.yellow_trip_analysis
        TABLES (
            trips AS FCT_YELLOW_TRIPS PRIMARY KEY (VENDORID, PICKUP_TIME),
            vendors AS DIM_TBL_VENDORS PRIMARY KEY (VENDOR_CODE),
            rates AS DIM_TBL_RATES PRIMARY KEY (RATE_CODE),
            payments AS DIM_TBL_PAYMENTS PRIMARY KEY (PAYMENT_CODE),
            zones AS DIM_TBL_ZONES PRIMARY KEY (LOCATIONID)
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
        )
        DIMENSIONS (
            vendors.vendor_name AS VENDOR_DESCRIPTION,
            rates.rate_description AS RATE_DESCRIPTION,
            payments.payment_description AS PAYMENT_DESCRIPTION,
            zones.pickup_borough AS zones.BOROUGH,
            trips.pickup_date AS DATE(PICKUP_TIME),
            trips.pickup_hour AS HOUR(PICKUP_TIME)
        )
        METRICS (
            trips.total_trips AS COUNT(*),
            trips.average_fare AS AVG(FARE_AMOUNT),
            trips.average_tip AS AVG(TIP_AMOUNT),
            trips.average_distance AS AVG(TRIP_DISTANCE)
        )
    """).collect()
st.success("✅ Semantic view created successfully.")

# -------------------------
# STEP 4: Dashboard
# -------------------------
st.subheader("📊 Step 4: Dashboard")

# Query data for charts
borough_df = session.sql("""
    SELECT BOROUGH, COUNT(*) AS TOTAL_TRIPS
    FROM NYC_TAXI.YELLOW_TAXI.FCT_YELLOW_TRIPS t
    JOIN NYC_TAXI.YELLOW_TAXI.DIM_TBL_ZONES z ON t.PICKUP_ZONE = z.LOCATIONID
    GROUP BY BOROUGH
""").to_pandas()

fare_hour_df = session.sql("""
    SELECT HOUR(PICKUP_TIME) AS HOUR, AVG(fare_amount) AS AVG_FARE
    FROM NYC_TAXI.YELLOW_TAXI.FCT_YELLOW_TRIPS
    GROUP BY HOUR(PICKUP_TIME)
    ORDER BY HOUR
""").to_pandas()

tip_payment_df = session.sql("""
    SELECT p.PAYMENT_DESCRIPTION AS PAYMENT_TYPE, AVG(tip_amount) AS AVG_TIP
    FROM NYC_TAXI.YELLOW_TAXI.FCT_YELLOW_TRIPS t
    JOIN NYC_TAXI.YELLOW_TAXI.DIM_TBL_PAYMENTS p ON t.payment_type = p.PAYMENT_CODE
    GROUP BY p.PAYMENT_DESCRIPTION
""").to_pandas()

# Display charts using Streamlit built-ins
st.write("### Total Trips per Borough")
st.bar_chart(borough_df.set_index("BOROUGH"))

st.write("### Average Fare by Hour")
st.line_chart(fare_hour_df.set_index("HOUR"))

