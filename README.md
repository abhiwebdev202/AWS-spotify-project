# AWS-spotify-project
A modern data lake pipeline on AWS

AWS Data Lake Platform (S3 | Glue | Athena | QuickSight)

A scalable cloud-based data lake architecture on AWS designed to ingest, catalog, query, and visualize data using a serverless data stack.

Project Overview

This project implements a modern data lake pipeline on AWS, enabling automated data ingestion, schema discovery, SQL querying, and business intelligence dashboards.

The architecture follows a decoupled, serverless design using AWS-native analytics services.

Architecture

Core AWS Services Used:

  -Amazon S3 → Data lake storage layer (raw + processed data)
  -AWS Glue Crawlers → Automated schema discovery & cataloging
  -AWS Glue Data Catalog → Central metadata repository
  -Amazon Athena → Serverless SQL query engine
  -Amazon QuickSight → Interactive dashboards & visualization layer

Data Pipeline Flow
1. Data Ingestion
  -Raw data is uploaded to Amazon S3 (raw zone)
2. Schema Discovery
  -AWS Glue Crawlers scan S3 datasets
  -Automatically infer schema and partition structure
3. Data Cataloging
  -Metadata stored in Glue Data Catalog
4. Query Layer
  -Amazon Athena runs SQL queries directly on S3 data
5. Visualization
  -Insights delivered via Amazon QuickSight dashboards

Key Features
  -Fully serverless data lake architecture
  -Automated schema detection using Glue Crawlers
  -SQL-based querying without infrastructure management (Athena)
  -Scalable storage using S3 data lake design
  -BI dashboards using QuickSight
  -Cost-efficient analytics workflow

            Tech Stack
Layer                          Service
Storage	                       Amazon S3
ETL / Metadata	               AWS Glue
Query Engine	                 Amazon Athena
Visualization	                 Amazon QuickSight
Catalog	                       AWS Glue Data Catalog

Example Use Cases
  -Weather data analytics
  -IoT data processing
  -Log analytics
  -Business reporting dashboards
  -Real-time-ish batch analytics on S3 data
