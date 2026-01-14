"""
====================================================================================================
FILE: transform.py
PURPOSE: Fire Hydrant Data Transformation Module
PIPELINE: Cincinnati Fire Hydrant Insurance Rating ETL
====================================================================================================

DESCRIPTION:
Transforms validated fire hydrant records into analytics-ready format for insurance underwriting.
Generates derived features including risk scores, quality indicators, and geographic clusters.

AWS MIGRATION PLAN:
- Deploy as: AWS Glue ETL job or Lambda function (depending on data volume)
- Input: S3 raw zone (JSON files from ingestion)
- Output: S3 processed zone (Parquet with date partitions)
- Catalog: AWS Glue Data Catalog for schema management
- Orchestration: Step Functions or AWS Glue Workflows
- Monitoring: CloudWatch Logs + Custom Metrics

VERSION HISTORY:
- v1.0: Initial version created on 2024-06-20 by Lora Covrett
====================================================================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime
from logging_config import setup_logging

logger = setup_logging(name="transform")


def transform_data(valid_records):
    """
    Transform validated fire hydrant records into a structured DataFrame
    for insurance rating analysis.
    
    AWS Migration: This function will become a Lambda handler or Glue job script.
    Consider partitioning strategy for large datasets in S3.
    
    Parameters:
    - valid_records (list): List of validated fire hydrant records
    
    Returns:
    - pd.DataFrame: Transformed DataFrame ready for downstream processing
    """
    logger.info(f"Starting transformation of {len(valid_records)} records.")
    
    # Convert list of dicts to pandas DataFrame for vectorized operations
    df = pd.DataFrame(valid_records)

    # Return early if no input records
    if df.empty:    
        logger.error("transform_data: no valid_records provided — returning empty DataFrame.")
        return df
    
    # Convert string numbers to proper numeric types
    #
    # pd.to_numeric parameters:
    # - errors='coerce': Invalid values become NaN (e.g., "abc" -> NaN)
    # - This allows data to flow through; NaNs handled by imputation below
    logger.info("Converting data types from API strings to proper types...")
    
    df['objectid'] = pd.to_numeric(df['objectid'], errors='coerce').astype('Int64')  # Int64 allows NaN
    df['assetid'] = pd.to_numeric(df['assetid'], errors='coerce')
    df['staticpressure'] = pd.to_numeric(df['staticpressure'], errors='coerce')
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    
    # Ensure text fields are strings (convert if needed)
    df['lifecyclestatus'] = df['lifecyclestatus'].astype(str)
    df['servicearea'] = df['servicearea'].astype(str)
    df['neighborhood'] = df['neighborhood'].astype(str)
    

    # Add load metadata for data lineage and incremental processing
    # AWS Note: Use partition columns (load_date) for efficient S3 queries via Athena
    df['load_date'] = pd.to_datetime(datetime.utcnow().date())
    df['load_timestamp'] = datetime.utcnow()
    
    # Standardize column names to match data lake conventions
    # AWS Best Practice: lowercase with underscores for Glue/Athena compatibility
    df.columns = df.columns.str.lower().str.strip()
    
    # Normalize text fields for consistent analytics and joins
    # Prevents case-sensitivity issues in SQL queries and group-by operations
    df['lifecyclestatus'] = df['lifecyclestatus'].str.strip().str.upper()
    df['servicearea'] = df['servicearea'].str.strip().str.title()
    df['neighborhood'] = df['neighborhood'].str.strip().str.title()
    
    # ==================== INSURANCE SPECIFIC ====================
    # 1. PRESSURE ADEQUACY CLASSIFICATION
    # Categorize hydrants by fire suppression capability
    # Business Context: Fire departments require 20+ PSI; 40-60 PSI is optimal
    # Used by: Underwriting models, risk scoring algorithms
    df['pressure_category'] = pd.cut(
        df['staticpressure'],
        bins=[-float('inf'), 20, 40, 60, float('inf')],
        labels=['INSUFFICIENT', 'MARGINAL', 'ADEQUATE', 'EXCELLENT']
    )
    
    # 2. OPERATIONAL STATUS FLAG
    # 0 = inactive, 1 = active, 2 = abandoned
    # Used by: Coverage gap analysis, risk heat maps
    df['is_active'] = np.where(
        df['lifecyclestatus'].isin(['AB', 'ABANDONED']), 2,
        np.where(df['lifecyclestatus'].isin(['ACTIVE', 'AC']), 1, 0)
    )

    # 3. PRESSURE RISK SCORE
    # Quantitative risk metric where higher score = higher risk
    # Formula: Inverse normalized pressure (0-100 scale)
    # Used by: Premium calculations, property risk scoring
    max_pressure = df['staticpressure'].max()
    df['pressure_risk_score'] = 100 - (df['staticpressure'] / max_pressure * 100)
    df['pressure_risk_score'] = df['pressure_risk_score'].fillna(100).round(2)
    
    # 4. GEOGRAPHIC CLUSTERING
    # Create spatial grid cells for proximity analysis
    # ~111 meters precision at 3 decimal places
    # Used by: Nearest neighbor searches, coverage density calculations
    df['geo_cluster'] = (
        df['latitude'].round(3).astype(str) + '_' + 
        df['longitude'].round(3).astype(str)
    )
    
    # 5. SERVICE QUALITY COMPOSITE INDICATOR
    # Combines operational status with pressure adequacy
    # Business Logic:
    #   - HIGH: Active + Pressure >= 40 PSI
    #   - MEDIUM: Active + Pressure 20-40 PSI
    #   - LOW: Active + Pressure < 20 PSI
    #   - INACTIVE: Not in service
    # Used by: Underwriting decisioning, risk tier assignment
    df['service_quality'] = 'UNKNOWN'
    df.loc[(df['is_active'] == 1) & (df['staticpressure'] >= 40), 'service_quality'] = 'HIGH'
    df.loc[(df['is_active'] == 1) & (df['staticpressure'].between(20, 40)), 'service_quality'] = 'MEDIUM'
    df.loc[(df['is_active'] == 1) & (df['staticpressure'] < 20), 'service_quality'] = 'LOW'
    df.loc[df['is_active'] == 0, 'service_quality'] = 'INACTIVE'
    
    # Impute missing pressure values with median (robust to outliers)
    # AWS Note: Track imputation rate as data quality metric in CloudWatch
    df['staticpressure'] = df['staticpressure'].fillna(df['staticpressure'].median())
    
    # Create deterministic hash for record identification
    # Used by: Change data capture (CDC), upsert operations in data lake
    # AWS Note: Enables efficient deduplication in Glue or EMR jobs
    df['record_hash'] = pd.util.hash_pandas_object(
        df[['objectid', 'latitude', 'longitude']], 
        index=False
    ).astype(str)
    
    # Reorder columns for logical grouping and query optimization
    column_order = [
        # Primary identifiers (high cardinality - good for indexing)
        'objectid', 'assetid', 'record_hash',
        # Geographic dimensions (used in spatial joins)
        'latitude', 'longitude', 'geo_cluster', 'neighborhood', 'servicearea',
        # Status and measurements (frequently filtered)
        'lifecyclestatus', 'is_active', 'staticpressure', 
        # Derived features (used in ML models and business logic)
        'pressure_category', 'pressure_risk_score', 'service_quality',
        # Metadata (partition keys for S3)
        'load_date', 'load_timestamp'
    ]
    
    df = df[column_order]
    
    # Log transformation metrics for observability
    # AWS Migration: Send these to CloudWatch Metrics for monitoring
    logger.info(f"Transformation complete. Output shape: {df.shape}")
    logger.info(f"Active hydrants: {df['is_active'].sum()} ({df['is_active'].mean()*100:.1f}%)")
    logger.info(f"Service quality distribution:\n{df['service_quality'].value_counts().to_dict()}")
    logger.info(f"Pressure categories:\n{df['pressure_category'].value_counts().to_dict()}")
    
    return df