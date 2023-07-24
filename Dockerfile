# Start with the official Apache Airflow image for version 2.0.0
FROM apache/airflow:2.0.0

# Install dependencies
RUN pip install python-dotenv
RUN pip install snowflake-connector-python
RUN pip install pandas