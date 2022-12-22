# Project Purpose
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

Source: Udacity Project Introduction

# Database Schema Design
The schema design being used in this project is the STAR schema. This includes a single fact table called songplays and 4 dimension tables: songs, artists, users, and time.

# ETL Pipeline
The ETL pipeline first extracts data from two S3 buckets which contain JSON logs on user activity and JSON meta data on the songs. This data is then loaded into redshift staging tables before it is transformed and loaded into the fact and dimension tables.