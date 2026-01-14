"""
====================================================================================================
FILE: storage.py
PURPOSE: Data Storage Module - Parquet File Writer with Partitioning
PIPELINE: Cincinnati Fire Hydrant Insurance Rating ETL
====================================================================================================
DESCRIPTION:
This module provides functionality to save transformed fire hydrant data into Parquet format with date-based partitioning.
It supports local file system storage for testing and AWS S3 for production deployment. 
The partitioning strategy organizes data by load date.
The main function, `save_parquet`, takes a pandas DataFrame containing transformed fire hydrant data 
and writes it to the specified storage location (local or S3) in a Hive-style partitioned directory structure.

AWS MIGRATION PLAN:
- Update the `processed_dir` parameter to use S3 paths (e.g., "s3://hydrant-data-lake/processed").
- Remove local directory creation logic as S3 does not require it.      
- Ensure that the pandas `to_parquet` method is compatible with S3 paths.
- Test the function to confirm successful writing to S3 with correct partitioning.  

VERSION HISTORY:
    - v1.0: Initial version created on 2024-06-15 by Lora Covrett
  
====================================================================================================
"""

import os
def save_parquet(df, processed_dir = "data/processed") -> None:
    """
    Save the transformed DataFrame to Parquet format with date-based partitioning.
    Parameters:
    - df: pandas
        DataFrame containing transformed fire hydrant data.
    - processed_dir: str
        Base directory for processed data storage.
        Default is "data/processed" for local testing.
        For AWS, use S3 path like "s3://hydrant-data-lake/processed".   
    Returns:    
    - None
    """
    # Extract the load date from first row to use as partition key
    # 
    # Assumptions:
    # - All rows in DataFrame have the same load_date (batch processing)
    # - load_date column exists and is datetime.date type
    # - DataFrame is not empty (validated in transform step)
    #
    # Format: Convert datetime.date to string "YYYY-MM-DD"
    # Example: datetime.date(2025, 1, 12) → "2025-01-12"
    date = df["load_date"].iloc[0].strftime("%Y-%m-%d")

  
    # Build Hive-style partition directory path
    # Format: {base_dir}/load_date={YYYY-MM-DD}
    # Example paths:
    # Local:  data/processed/load_date=2025-01-12
    # AWS S3: s3://hydrant-data-lake/processed/load_date=2025-01-12
    path = os.path.join(processed_dir, f"load_date={date}")


    # Create partition directory if it doesn't exist
    # 
    # Parameters:
    # - exist_ok=True: Don't raise error if directory already exists
    #   (allows overwrites for reprocessing scenarios)
    #
    # AWS Migration: Not needed for S3 (no directory concept)
    # S3 keys are flat namespace with "/" as delimiter
    os.makedirs(path, exist_ok=True)


    # Write DataFrame to Parquet format
    # 
    # Parameters:
    # - index=False: Exclude DataFrame index from Parquet file
    #   (index is just row numbers, not meaningful data)    
    # File naming convention:
    # File path: {partition_dir}/firehydrants.parquet
    # Example: data/processed/load_date=2025-01-12/firehydrants.parquet
    #
    # AWS Migration: Write directly to S3 using s3://bucket/path format
    df.to_parquet(os.path.join(path, "firehydrants.parquet"), index=False)