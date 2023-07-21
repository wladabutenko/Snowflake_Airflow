# Snowflake_Airflow

Task
To build a Snowflake based ELT pipeline using Airflow. The overall design of the architecture should look like the illustration below.

Technical description

Parsing the file, getting rid of indexes (otherwise Snowflake will not read it).
Create two data streams in Snowflake and configure them for two tables (RAW_TABLE, STAGE_TABLE).
Write data from CSV to RAW_TABLE
Write data from RAW_STREAM to STAGE_TABLE
Write data from STAGE_STREAM to MASTER_TABLE

