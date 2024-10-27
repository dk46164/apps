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
from .weather_datamodel import CityWeather 
from pydantic import PydanticInvalidForJsonSchema



def _read_json_file(file_path: Path, logger: logging.Logger) -> Dict[str, dict]:
    """
    Read a single JSON file and return its contents as a dictionary.

    Args:
        file_path (Path): The path to the JSON file to be read.
        logger (logging.Logger): The logger instance for logging messages.

    Returns:
        Dict[str, dict]: A dictionary where each key is a city name and each value is the validated weather data.

    Raises:
        DataValidationError: If the JSON data does not conform to the expected schema.
        FileOperationError: If there is an issue reading the file.
    """
    try:
        logger.debug(f"Attempting to read file: {file_path}")
        with open(file_path, "r", encoding="utf-8") as file:
            weather_data = json.load(file)
            parsed_data = {}
            for city, content in weather_data.items():
                # Validate and parse each city's weather data using CityWeather model
                city_weather = CityWeather(**content)
                parsed_data[city] = city_weather.model_dump()
        logger.info(f"Successfully read file: {file_path}")
        return parsed_data
    except PydanticInvalidForJsonSchema as e:
        logger.error(f"Validation error for file {file_path}: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"Failed to read JSON from {file_path}: {str(e)}")
        raise e
    

def execute_extract(data: Dict, config_dict: Dict, logger: logging.Logger) -> Dict:
    """Execute extract step."""
    input_path = config_dict['input']
    raw_data = _read_json_file(input_path, logger)
    return {'raw_data': raw_data}