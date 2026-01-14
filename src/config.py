"""
====================================================================================================
FILE: config.py
PURPOSE: Configuration Module - Pipeline Settings and Constants
PIPELINE: Cincinnati Fire Hydrant Insurance Rating ETL
====================================================================================================

DESCRIPTION:
Configuration settings for the Cincinnati Fire Hydrant Insurance Rating ETL pipeline.
Defines API endpoints, data directory paths, and other constants used throughout the pipeline.

AWS MIGRATION PLAN:
- Replace with: AWS Systems Manager Parameter Store or SSM
- Store sensitive values like API keys in Parameter Store
- Use environment variables for non-sensitive settings (e.g., DATA_DIR)
- Enable versioning for configuration changes

VERSION HISTORY:
- v1.0: Initial version created on 2024-06-15 by Lora Covrett
====================================================================================================
"""
import os

# Configuration settings for the data processing pipeline
API_URL = "https://data.cincinnati-oh.gov/resource/qhw6-ujsg.json"
DATA_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

# Ensure data directories exist
os.makedirs(DATA_DIR, exist_ok=True)        
os.makedirs(PROCESSED_DIR, exist_ok=True)
