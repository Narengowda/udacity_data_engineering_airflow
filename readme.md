Project to demonstrate the Airflow capabalities by loading data from S3 to Redshift using Star schema

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

Technologies used
1. Python3
2. Apache Airflow
3. AWS redshift
4. AWS s3

Data Sources

    Log data: s3://udacity-dend/log_data
    Song data: s3://udacity-dend/song_data

Files and explantion
    create_tables.sql - Contains the DDL to create tables used in this projects
    udac_example_dag.py - The Airflow DAG file
    stage_redshift.py - Operator to load data from s3 to redshift
    load_fact.py - Operator to load the fact tables to redshift
    load_dimension.py - Operator to load dimension table
    data_quality.py - Operator for data quality check

Data quality is checked in the end of the DAG to verify data is loaded succesfllly