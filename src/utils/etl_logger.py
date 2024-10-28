"""
logger.py

Logger Configuration Module

This module provides centralized logging configuration for the weather ETL pipeline.
It handles log file setup, formatting, and execution time tracking.

Functions:
    - _load_config: Helper function to load YAML configuration file
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


import yaml
from pathlib import Path
from typing import Callable,Dict
from datetime import datetime
import logging
import logging.config
import os
import functools 
from uuid import uuid4


def _load_config(config_path: Path) -> Dict:
    """
    Load and parse YAML configuration file.
    
    Args:
        config_path: Path to the YAML configuration file
        
    Returns:
        Dict containing parsed configuration
        
    Raises:
        RuntimeError: If config file cannot be loaded
    """
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        raise RuntimeError(f"Failed to load configuration file {config_path}: {str(e)}")



def get_logger(
    config_path: Path,
    runid:str
) -> logging.Logger:
    """
    Configure and return a logger instance.

    Args:
        config_path: Path to logger config YAML file
        runid: pipeline run id

    Returns:
        Configured logger instance

    Raises:
        FileNotFoundError: If config file not found
        ValueError: If invalid config
    """
    try:
        
        # Load configuration from YAML file
        config = _load_config(config_path)
        data_root_dir = config['paths']['root']
        app_name  = config['default']['app_name']
        logs_dir = Path(data_root_dir).joinpath(config['paths']['logs']['dir'],runid)

        # create logs dir
        logs_dir.mkdir(exist_ok=True,parents=True)

        # create logs file for steps
        for step,conf in config['logging']['handlers'].items():
            if step.endswith('_handler') and f'{app_name}_handler'!=step:
                step_log_file_path = Path(logs_dir).joinpath(step.replace('_handler',''))
                step_log_file_path.mkdir(exist_ok=True,parents=True)

                # create log file
                step_log_file_name = step_log_file_path.joinpath(conf['filename'])
                step_log_file_name.touch(exist_ok=True)
            
                # set logging file 
                config['logging']['handlers'][step]['filename'] = step_log_file_name

        # for main logger 
        app_log_file_name =  Path(logs_dir).joinpath(config['logging']['handlers'][f'{app_name}_handler']['filename'])
        app_log_file_name.touch(exist_ok=True)
        config['logging']['handlers'][f'{app_name}_handler']['filename'] = app_log_file_name

        logging.config.dictConfig(config['logging'])

        # Get the logger instance
        logger = logging.getLogger(app_name)
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