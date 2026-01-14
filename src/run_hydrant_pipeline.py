"""
====================================================================================================
FILE: run_hydrant_pipeline.py
PURPOSE: Main Orchestration Module for Fire Hydrant Data Pipeline
PIPELINE: Cincinnati Fire Hydrant Insurance Rating ETL
====================================================================================================

OVERVIEW:
This module orchestrates the end-to-end data pipeline for processing fire hydrant data from Cincinnati's
Open Data API. It coordinates the following stages: data ingestion, validation, transformation, and storage.     
The pipeline is designed to run on a daily schedule.

AWS MIGRATION PLAN:
- Replace local file operations with AWS S3 for data storage.
- Use AWS Lambda or Glue for executing the pipeline on a scheduled basis.
- Integrate AWS Step Functions for orchestration and error handling.      
- Utilize AWS CloudWatch for logging and monitoring pipeline execution.  
- Implement AWS SNS for alerting on pipeline failures.  

VERSION HISTORY:
    - v1.0: Initial version created on 2024-06-15 by Lora Covrett
     
====================================================================================================
"""
import uuid
from alerts import send_alert
from logging_config import setup_logging
from ingestion import fetch_data    
from validation import validate_data
from storage import save_parquet
from transform import transform_data

# Generate unique run identifier for tracing records through pipeline
# AWS Migration: Use Step Functions execution ARN or Glue job run ID
run_id = str(uuid.uuid4())[:8]

# Initialize structured logging with run context
# AWS Migration: CloudWatch Logs with correlation ID in log group/name
logger = setup_logging(name="hydrant_pipeline", run_id=run_id)  

def run_pipeline():
    logger.info(f"Starting hydrant pipeline with run ID: {run_id}")
    try:
      # ==================== STAGE 1: DATA INGESTION ====================
        # Fetch raw data from Cincinnati Open Data API
        # AWS Migration: Lambda function triggered by EventBridge schedule (daily cron)
        # Output: S3 path s3://bucket/raw/firehydrants/YYYYMMDDTHHMMSSZ.json
      raw_file = fetch_data()
      
      # ==================== STAGE 2: DATA VALIDATION ====================
        # Validate records against expected schema and business rules
        # Returns tuple: (list of valid records, count of invalid records)
        # AWS Migration: Glue job with validation logic
        # Invalid records → Write to S3 quarantine bucket for investigation
      valid, invalid_count = validate_data(raw_file)

      # Log data quality metrics for monitoring
      # AWS Migration: Publish to CloudWatch custom metric "ValidRecordCount"
      logger.info(f"Validation complete: {valid} valid records, {invalid_count} invalid records." )

      # ==================== DATA QUALITY GATE ====================
      # Fail pipeline if no valid data exists (prevents processing empty datasets)
       # AWS Migration: Step Functions Choice state with conditional branching
      if not valid:
        logger.warning("No valid data to process after validation.")    
        raise ValueError("No valid data to process after validation.")
      
      # ==================== STAGE 3: DATA TRANSFORMATION ====================
      # Apply business logic and create derived features for analytics
      # AWS Migration: Glue job with transform.py logic
      # Input: Valid records (list of dicts)
      # Output: Pandas DataFrame with engineered features
      df = transform_data(valid)
      
      # Exit if transform produced no rows
      if df is None or df.empty:
        logger.error("transform_data returned empty DataFrame; aborting pipeline.")
        raise ValueError("transform_data returned empty DataFrame; aborting pipeline.")

      # ==================== STAGE 4: DATA STORAGE ====================
      # Save transformed data as Parquet files for efficient querying
      # AWS Migration: Glue job writing Parquet to S3 processed zone
      # S3 Path: s3://bucket/processed/load_date=YYYY-MM-DD/firehydrants.parquet
      save_parquet(df, processed_dir="data/processed")

      # ==================== PIPELINE SUCCESS ====================
      # Log successful completion for audit trail
      # AWS Migration: Emit CloudWatch metric "PipelineSuccessCount"
      logger.info("Data pipeline completed successfully.")

    except Exception as e:
      # ==================== PIPELINE FAILURE HANDLING ====================
      # Log error details for troubleshooting
      # AWS Migration: Emit CloudWatch metric "PipelineFailureCount"
     logger.error(f"Hydrant pipeline failed: {e}")
      # Send alert notification for immediate attention
      # AWS Migration: Publish to SNS topic subscribed by PagerDuty/email
     send_alert(f"Hydrant pipeline failed: {e}")
      # Re-raise exception for upstream handling
      # AWS Migration: Step Functions captures error and can trigger retry logic
     raise
  
# ==================== ENTRY POINT ====================
# Execute pipeline when run as main script
# AWS Migration: Not needed - Lambda handler or Glue main function becomes entry point
if __name__ == "__main__":
    run_pipeline()
