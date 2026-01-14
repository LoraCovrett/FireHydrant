"""
====================================================================================================
FILE: ingestion.py
PURPOSE: Data Ingestion Module - Fetch and Save Raw Data
PIPELINE: Cincinnati Fire Hydrant Insurance Rating ETL
====================================================================================================

DESCRIPTION:
Module to fetch raw data from the Cincinnati Open Data API and save it to the local file system.
This is the first step in the ETL pipeline, responsible for extracting data from its source.

AWS MIGRATION PLAN:
- Replace local file system writes with S3 uploads
- Use AWS Lambda for data fetching and processing
- Implement versioning for raw data files in S3

VERSION HISTORY:
- v1.0: Initial version created on 2024-06-15 by Lora Covrett
====================================================================================================    
"""
import requests
from datetime import datetime
import os
from config import API_URL, DATA_DIR
from logging_config import setup_logging

# Initialize module-level logger
# AWS Migration: CloudWatch Logs with structured JSON logging
logger = setup_logging(name="ingestion")

def fetch_data():
    """
    Fetch data from Cincinnati Open Data API and save to local file system.
    Returns:
        str: Path to the saved data file.
        
    """
    logger.info("Starting data ingestion process.")
    try:
    
        # Make HTTP GET request to Cincinnati Open Data API
        # 
        # Request Configuration:
        # - timeout=30: Prevents hanging on slow networks (fail fast principle)
        # - No auth headers currently needed (public API)
        # 
        # AWS Migration: Add retry decorator
        # @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
        response = requests.get(API_URL, timeout=30)

        # Raise exception for HTTP error responses (4xx, 5xx status codes)
        # This ensures we don't save error pages as valid data
        response.raise_for_status()

        # Create ISO 8601 UTC timestamp for file naming
        # Format: YYYYMMDDTHHMMSSZ (Z indicates UTC timezone)
        # 
        # Example: 20250112T143000Z = January 12, 2025 at 2:30:00 PM UTC
        ts=datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

        # Build file path with timestamp for versioning
        # Pattern: {DATA_DIR}/firehydrants_{TIMESTAMP}.json
        # Example: firehydrants_20231005T142530Z.json
        # 
        # AWS Migration: Change to S3 key structure
        # S3 Key: raw/year=2025/month=01/day=12/firehydrants_20250112T143000Z.json
        # This enables partition pruning in Athena for faster queries
        filename = f"{DATA_DIR}/firehydrants_{ts}.json"

        # Write raw API response to local file system
        # Mode 'w' = write (create new file or overwrite existing)
        # 
        # AWS Migration: Replace with S3 upload using boto3
        with open(filename, 'w') as f:
            f.write(response.text)

        logger.info(f"Data saved to {filename}")

        # Return file path for downstream processing
        # AWS Migration: Return S3 URI (s3://bucket/key)
        return filename
    
    except requests.exceptions.Timeout as e:
        # ==================== TIMEOUT HANDLING ====================
        # API took longer than 30 seconds to respond
        # Possible causes: Network issues, API overload, maintenance
        # 
        # AWS Migration: Implement retry logic before raising
        # Log timeout for CloudWatch alarm creation
        logger.error("API request timed out after 30 seconds: %s", e)
        raise

    except requests.exceptions.HTTPError as e:
        # ==================== HTTP ERROR HANDLING ====================
        # API returned error status code (4xx client error, 5xx server error)
        # Possible causes: Invalid endpoint, rate limiting, server issues
        #
        # AWS Migration: Parse error response and publish specific metrics
        # Example: CloudWatch metric "API_Error_401" for auth failures
        logger.error("API returned HTTP error: %s", e)
        raise
        
    except IOError as e:
        # ==================== FILE SYSTEM ERROR HANDLING ====================
        # Failed to write file to local disk
        # Possible causes: Disk full, permission denied, directory doesn't exist
        # 
        # AWS Migration: S3 put_object failures (boto3.exceptions)
        # Handle ClientError for S3-specific issues (access denied, bucket not found)
        logger.error("Failed to write data to file system: %s", e)
        raise
        
    except Exception as e:
        # ==================== CATCH-ALL ERROR HANDLING ====================
        # Unexpected errors not covered by specific handlers above
        # Examples: Network errors, DNS resolution failures, encoding issues
        # 
        # Log full exception details for debugging
        # AWS Migration: Log stack trace to CloudWatch for investigation
        logger.error("Unexpected error during data ingestion: %s", e)
        raise

        
   
