Fire Hydrant Data Pipeline

A daily ETL pipeline for processing Cincinnati fire hydrant data from the city's Open Data API. This project ingests, validates, transforms, and stores fire hydrant location and insurance rating data 
in a structured, analysis-ready format.

Overview:

This pipeline automates the extraction of fire hydrant data from Cincinnati's public data portal, applies validation rules to ensure data quality, transforms the data for analysis, 
and stores processed results in Parquet format for downstream consumption.

Current Status: Local file-based pipeline

Planned Evolution: AWS migration (S3, Lambda, Glue, Step Functions)

Architecture: The pipeline follows a four-stage ETL pattern:


Ingestion -  Fetch raw JSON data from Cincinnati Open Data API

Validation -   Verify data completeness and quality

Transformation -   Clean, standardize, and enrich data

Storage -   Persist processed data in Parquet format

Each run is assigned a unique identifier for traceability through all stages.
