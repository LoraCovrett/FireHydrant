"""
====================================================================================================
FILE: logging_config.py
PURPOSE: Logging Configuration Module - Setup and Management of Pipeline Logging    
PIPELINE: Cincinnati Fire Hydrant Insurance Rating ETL  
====================================================================================================

DESCRIPTION:
    This module provides a standardized logging configuration for the Cincinnati Fire Hydrant 
    Insurance Rating ETL Pipeline. It sets up logging with a specific format that includes 
    timestamps, logger names, run identifiers, log levels, and messages. This ensures consistent 
    logging across all components of the pipeline, facilitating easier debugging and monitoring.    

USAGE EXAMPLE:  
    from logging_config import setup_logging

    logger = setup_logging(name="hydrant_pipeline", level=logging.DEBUG, run_id="12345")

    logger.info("This is an informational message.")
    logger.error("This is an error message.")
    logger.debug("This is a debug message.")        
OUTPUT FORMAT:
    2024-06-01 12:00:00,000 | hydrant_pipeline | 12345 | INFO | This is an informational message.
    2024-06-01 12:00:00,001 | hydrant_pipeline | 12345 | ERROR | This is an error message.
    2024-06-01 12:00:00,002 | hydrant_pipeline | 12345 | DEBUG | This is a debug message.   

AWS MIGRATION PLAN:
    - Ensure that logging works seamlessly in both local and AWS environments.
    - Use CloudWatch Logs for centralized logging in AWS.
    - Maintain the same log format for consistency across environments.
    
VERSION HISTORY:
   - v1.0: Initial version created on 2024-06-15 by Lora Covrett   
===================================================================================================
"""

import logging
import sys

def setup_logging(name="hydrant_pipeline", level=logging.INFO, run_id=None):
    """
    Set up logging configuration.

    Parameters:
    - name (str): The name of the logger.
    - level (int): The logging level (e.g., logging.INFO, logging.DEBUG).
    - run_id (str): An optional run identifier to include in log messages.
    """
    # Add run_id to log records if provided
    if run_id:
        class ContextFilter(logging.Filter):
            def filter(self, record):
                record.run_id = run_id
                return True

        logging.getLogger().addFilter(ContextFilter())
    else:
        logging.getLogger().addFilter(lambda record: setattr(record, 'run_id', 'N/A') or True)
    
    # Configure logger
    logger = logging.getLogger(name)

    # Set logging level
    logger.setLevel(level)

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)

    # Set handler level
    handler.setLevel(level)

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s | %(name)s | %(run_id)s | %(levelname)s | %(message)s'
    )

    # Add formatter to handler
    handler.setFormatter(formatter)

    # Add handler to logger if not already added
    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger