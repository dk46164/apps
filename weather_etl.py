"""
weather_etl.py

This script executes an ETL (Extract, Transform, Load) pipeline for weather data.
It reads weather data from a JSON file, processes and analyzes the data, and
saves the results to CSV files. The pipeline includes logging for monitoring
and error handling.

Steps:
    1. Extract: Read raw weather data from a JSON file.
    2. Transform: Process the data to extract meaningful insights.
    3. Load: Save the processed data to CSV files for further use.
"""

import sys
import os
from pathlib import Path
from typing import Dict
import logging
from datetime import datetime

# Add the current working directory to the system path for module imports
sys.path.append(Path(os.getcwd()))
from src.core import pipeline

def main():
    """Main entry point for the weather pipeline."""
    # setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # run the pipeline
        pipeline.execute_pipeline()

        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()