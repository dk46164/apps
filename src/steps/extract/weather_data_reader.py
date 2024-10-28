"""
weather_data_reader.py

This script provides functionality to read weather data from a JSON file and validate it
using Pydantic models.

Functions:
- read_single_json_file: Reads a JSON file and returns its contents as a dictionary,
  validating each entry against the CityWeather model.
"""

import sys
import os 

# Ensure the module path is included for imports
sys.path.append(os.getcwd())

import json
import logging
from pathlib import Path
from typing import Dict
from pydantic import PydanticInvalidForJsonSchema
from .weather_datamodel import CityWeather 


def _read_json_file(file_path: Path) -> Dict[str, dict]:
    """
    Read a single JSON file and return its contents as a dictionary.

    This function reads weather data from a JSON file, validates each entry
    against the CityWeather model, and returns the parsed and validated data.

    Args:
        file_path (Path): The path to the JSON file to be read.

    Returns:
        Dict[str, dict]: A dictionary where each key is a city name and 
                        each value is the validated weather data.

    Raises:
        PydanticInvalidForJsonSchema: If the JSON data fails validation
        FileNotFoundError: If the specified file doesn't exist
        JSONDecodeError: If the file contains invalid JSON
        Exception: For other unexpected errors
    """
    # Get logger instance with app-specific naming
    logger = logging.getLogger(f"{os.environ.get('APP_NAME')}.extract")
    
    try:
        logger.info(f"Starting to read JSON file from: {file_path}")
        logger.debug(f"Checking if file exists at path: {file_path}")
        
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"No file found at {file_path}")

        # Read and parse the JSON file
        logger.debug("Opening file for reading")
        with open(file_path, "r", encoding="utf-8") as file:
            weather_data = json.load(file)
            logger.info(f"Successfully loaded JSON data containing {len(weather_data)} entries")
            
            # Initialize dictionary for parsed data
            parsed_data = {}
            logger.debug("Starting validation of individual city entries")
            
            # Process each city's weather data
            for city, content in weather_data.items():
                logger.debug(f"Validating data for city: {city}")
                city_weather = CityWeather(**content)
                parsed_data[city] = city_weather.model_dump()
                logger.debug(f"Successfully validated data for city: {city}")
            
            logger.info(f"Successfully parsed and validated all {len(parsed_data)} city entries")
            return parsed_data
            
    except PydanticInvalidForJsonSchema as e:
        logger.error(f"Data validation error for file {file_path}: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format in file {file_path}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while reading file {file_path}: {str(e)}")
        raise

def execute_extract(data: Dict, config_dict: Dict, logger: logging.Logger) -> Dict:
    """
    Execute the data extraction step by coordinating the JSON file reading process.

    This function serves as the main entry point for the extraction process,
    handling the configuration and orchestrating the file reading operation.

    Args:
        data (Dict): Input dictionary containing any pre-existing data (not used)
        config_dict (Dict): Configuration dictionary containing the 'input' file path
        logger (logging.Logger): Logger instance for tracking execution progress

    Returns:
        Dict: Dictionary containing the extracted data under the 'raw_data' key

    Raises:
        KeyError
            If 'input' key is missing from config_dict
        Exception
            For any errors during file reading or data processing
    """
    logger.info("Starting execute_extract function")
    
    try:
        # Get input path from config
        logger.debug("Retrieving input path from config dictionary")
        input_path = config_dict['input']
        logger.info(f"Input path configured as: {input_path}")
        
        # Read and process the JSON file
        logger.debug("Initiating JSON file reading process")
        raw_data = _read_json_file(Path(input_path))
        
        # Log success and data statistics
        logger.info(f"Successfully extracted data for {len(raw_data)} cities")
        logger.debug(f"Extracted data keys: {', '.join(raw_data.keys())}")
        logger.info("Completed execute_extract function")

        return {'raw_data': raw_data}
        
    except KeyError as e:
        logger.error(f"Configuration error: Missing 'input' key in config_dict: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Failed to execute extract step: {str(e)}")
        raise
        