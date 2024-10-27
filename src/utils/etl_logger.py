"""
logger.py

Logger Configuration Module

This module provides centralized logging configuration for the weather ETL pipeline.
It handles log file setup, formatting, and execution time tracking.

Functions:
    - get_logger: Configures and returns logger instance
        - Loads YAML configuration
        - Sets up log directories and files
        - Configures handlers and formatters
        - Provides fallback logging on failure
        
    - log_execution_time: Decorator for tracking function execution time
        - Records start and end times
        - Calculates execution duration
        - Logs to metrics file
        - Supports daily metrics aggregation

"""

import logging.config
import yaml
from pathlib import Path
from typing import Optional,Callable
from datetime import datetime
import logging
import os


def get_logger(
    config_path: Path
) -> logging.Logger:
    """
    Configure and return a logger instance.

    Args:
        config_path: Path to logger config YAML file

    Returns:
        Configured logger instance

    Raises:
        FileNotFoundError: If config file not found
        ValueError: If invalid config
    """
    try:
        
        # Load configuration from YAML file
        with open(config_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
            date_str = datetime.now().strftime('%Y%m%d')
            logs_dir = Path(config['paths']['root']).joinpath(config['paths']['logs']['dir'],date_str)
            logs_file_name = config['logging']['handlers']['unified_log']['filename']

            # create logs dir
            logs_dir.mkdir(exist_ok=True)

            # create  file
            Path(logs_dir).joinpath(logs_file_name).touch()

            config['logging']['handlers']['unified_log']['filename'] = Path(logs_dir).joinpath(logs_file_name)
            logging.config.dictConfig(config['logging'])

            # set enviorment variables for logging functions
            os.environ['metrics_path'] = str(Path(config['paths']['root']).joinpath(config['paths']['metrics']['dir']))
            
            # Get the logger instance
            logger = logging.getLogger(config['default']['project_name'])        
            return logger

    except Exception as e:
        # Emergency fallback logging
        basic_logger = logging.getLogger('default')
        basic_logger.setLevel(logging.INFO)

        # Ensure at least console logging works
        if not basic_logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - [%(levelname)s] - %(message)s"
                )
            )
            basic_logger.addHandler(console_handler)

        basic_logger.error("Failed to configure logging: %s", str(e))
        raise e


def log_execution_time(func:Callable) -> Callable:
    """
    Decorator to log the execution time of a function, including start and end times.
    """
    def wrapper(*args, **kwargs):
        start_time = datetime.now()  # Record start time

        # Configure logging
        logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    handlers=[
                        logging.FileHandler('metrics.log'),
                    ])
        logging.info(f'Starting {func.__name__} at {start_time}')

        result = func(*args, **kwargs)  # Execute the function

        end_time = datetime.now()  # Record end time
        execution_time = (end_time - start_time).total_seconds()  # Calculate execution time

        logging.info(f'Finished {func.__name__} at {end_time}, executed in {execution_time:.4f} seconds')

        # Log daily metrics
        date_str = datetime.now().strftime('%Y-%m-%d')
        metric_file = 'metrics.log'
        metric_file.mkdir(exist_ok=True)
        
        with open(metric_file, 'a') as f:
            f.write(f'{start_time}: Started {func.__name__}\n')
            f.write(f'{end_time}: Finished {func.__name__}, executed in {execution_time:.4f} seconds\n')
        
        return result
    return wrapper