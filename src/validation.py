"""
====================================================================================================
FILE: validation.py
PURPOSE: Data Validation Module - Schema and Type Validation
PIPELINE: Cincinnati Fire Hydrant Insurance Rating ETL
====================================================================================================

DESCRIPTION:
Validates raw fire hydrant records against expected schema and data types.
Separates valid records from invalid records for data quality tracking and quarantine.

AWS MIGRATION PLAN:
- Deploy as: Part of Glue ETL job or separate Lambda function
- Invalid records: Write to S3 quarantine bucket for investigation
- Metrics: Publish validation rates to CloudWatch for monitoring
- Data Quality: Use AWS Glue Data Quality rules (alternative approach)

VERSION HISTORY:
- v1.0: Initial version created on 2024-06-15 by Lora Covrett
====================================================================================================
"""

import json

# VALIDATION SCOPE:
# This module only checks that required columns exist (schema completeness).
# Type conversions and null handling are performed in transform.py using pandas.

REQUIRED_COLS = [
    "objectid",
    "assetid",
    "lifecyclestatus",
    "servicearea",
    "staticpressure",
    "latitude",
    "longitude",
    "neighborhood"
]


def validate(record):
    """
    Validate a single fire hydrant record for schema completeness.
    
    This function only checks that all required columns are present.
    It does NOT validate types - that's handled in transform.py with pandas.
   
    Parameters:
    - record (dict): Single hydrant record from API (JSON object)
                     Example: {"objectid": "123", "latitude": "39.1", ...}
    
    Returns:
    - bool: True if all required columns exist, False otherwise
    
    """

    # Check if all required columns exist in the record
    # Missing columns indicate schema drift or API changes
    for col in REQUIRED_COLS:
        if col not in record:
            return False  # Fail validation: required column missing
    
    # All required columns present
    return True


def validate_data(file_path):
    """
    Validate all records in a JSON file for schema completeness.
    
    This function reads raw JSON data from ingestion and checks that each record
    has all required columns. Records with missing columns are counted as invalid.
   
    Parameters:
    - file_path (str): Path to JSON file from ingestion step
                       Local: "data/raw/firehydrants_20250112T143000Z.json"
                       AWS: S3 key from event payload
    
    Returns:
    - tuple: (valid_records, invalid_count)
        - valid_records (list): List of dicts with complete schema
        - invalid_count (int): Number of records missing required columns
    
    Data Quality Metrics (for CloudWatch):
    - Validation Rate = len(valid_records) / (len(valid_records) + invalid_count)
    - Invalid Record Count = invalid_count
    - Total Record Count = len(valid_records) + invalid_count
    
    """
    # Initialize containers for categorizing records
    valid_records = []    # Accumulator for records with complete schema
    invalid_count = 0     # Counter for records missing required columns
    

    # Read raw JSON file from ingestion step
    # Expected format: List of JSON objects (one per hydrant)
    # 
    # AWS Migration: Replace with S3 get_object
    # response = s3.get_object(Bucket=bucket, Key=key)
    # data = json.loads(response['Body'].read().decode('utf-8'))
    with open(file_path, 'r') as f:
        data = json.load(f)
    

    # Iterate through each record and check schema completeness
    # 
    # This loop is the data quality gate for schema:
    # - Complete schema → Pass to transformation
    # - Incomplete schema → Count as invalid (missing columns)
    for record in data:
        if validate(record):
            # Record has all required columns (types will be fixed in transform)
            valid_records.append(record)
        else:
            # Record missing one or more required columns
            invalid_count += 1
            
            # AWS Migration: Write to quarantine bucket
            # Store with details about which column(s) are missing (future enhancement)
    

    # Return valid records for transformation and count of invalid records
    return valid_records, invalid_count


