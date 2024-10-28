"""
weather_data_processing.py

This module provides functions to process weather data by combining location, 
current weather, and forecast information into pandas DataFrames.

Functions:
    - combine_weather_data: Combines location and current weather data with forecast data.
    - get_transformed_dataframes: Processes multiple weather data inputs concurrently.
    - execute_transform: Execute the transformation step for weather data processing.
"""

from typing import Any, Dict, List,Tuple
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

def _combine_weather_data(key:int,data: Dict[str, Any]) -> Tuple[pd.DataFrame]:
    """
    Combines location and current weather data with forecast data into a single DataFrame.

    Args:
        data (Dict[str, Any]): The input data containing location, current weather, and forecast information.

    Returns:
        pd.DataFrame: A DataFrame with combined weather data.
    """
    # get logger for step.app_name
    logger = logging.getLogger(f'{os.environ['APP_NAME']}.transform')
    logger.info(f"Starting to combine weather data for key: {key}")
    
    try:
        # Extract location and current weather data
        location = data['location']
        current = data['current']
        logger.debug(f"Successfully extracted location and current weather data for {key}")
        
        current_weather_df = pd.json_normalize(current, sep='_')
        location_df = pd.json_normalize(location, sep='_')
        logger.debug(f"Successfully normalized location and current weather data for {key}")

        # Add city columns
        city, country = [val.strip() for val in key.split(',')]
        current_weather_df['name'] = city
        current_weather_df['country'] = country
        current_weather_df['region'] = data['location']['region']
        logger.debug(f"Added location columns for {key}")

        # Extract forecast data
        forecast = data['forecast']['forecastday']
        forecast_data: List[Dict[str, Any]] = []
        for day in forecast:
            forecast_data.append(dict(**day))
        forecasted_weather_df = pd.json_normalize(forecast_data, sep='_')
        logger.debug(f"Successfully processed forecast data for {key}")

        # Add city columns to forecast DataFrame
        forecasted_weather_df['name'] = city
        forecasted_weather_df['country'] = country
        
        logger.info(f"Successfully combined all weather data for {key}")
        return (current_weather_df, forecasted_weather_df, location_df)

    except KeyError as e:
        logger.error(f"Missing key {e} in the input data for {key}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while combining weather data for {key}: {str(e)}")
        raise

def _get_parsed_weather_dataframes(data: Dict[str, Any]) -> Tuple[pd.DataFrame]:
    """
    Processes multiple weather data inputs using a process pool to combine individual data into DataFrames.

    Args:
        data (Dict[str, Any]): A dictionary of weather data keyed by location identifiers.

    Returns:
        List[pd.DataFrame]: A list of combined DataFrames from the weather data.
    """
    # get logger for step.app_name
    logger = logging.getLogger(f'{os.environ['APP_NAME']}.transform')

    try:
        keys = list(data.keys())
        logger.debug(f"Processing {len(keys)} locations")
        
        with ProcessPoolExecutor(max_workers=len(keys)) as executor:
            logger.debug(f"Created process pool with {len(keys)} workers")
            futures = {executor.submit(_combine_weather_data, key, data[key]): key for key in keys}
            
            results = []
            for future in as_completed(futures):
                key = futures[future]
                try:
                    results.append(future.result())
                    logger.debug(f"Successfully processed data for {key}")
                except Exception as e:
                    logger.error(f"Failed to process data for {key}: {str(e)}")
                    raise

            # Concatenate results
            logger.debug("Concatenating results into final DataFrames")
            current_df = pd.concat([dataframe[0] for dataframe in results])
            forecast_df = pd.concat([dataframe[1] for dataframe in results])
            location_df = pd.concat([dataframe[2] for dataframe in results])
            
            logger.info("Successfully completed weather data parsing")
            return (current_df, forecast_df, location_df)

    except Exception as e:
        logger.error(f"Failed to parse weather dataframes: {str(e)}")
        raise
    
def execute_transform(data: Dict, config_dict: Dict, logger: logging.Logger) -> Dict:
    """
    Execute the transformation step for weather data processing.
    
    This function serves as the main entry point for the transformation pipeline,
    coordinating the parsing and processing of raw weather data into structured DataFrames.
    
    Args:
        data (Dict): Input dictionary containing raw weather data under the 'raw_data' key.
                
        config_dict (Dict): Configuration parameters for the transformation process.
                           Currently unused but maintained for future extensibility.
        logger (logging.Logger): Logger instance for tracking the transformation process.
    
    Returns:
        Dict: A dictionary containing three processed DataFrames:
              - 'raw_location': Location-specific information
              - 'raw_current': Current weather conditions
              - 'raw_forecast': Weather forecast data
    
    Raises:
        KeyError: If 'raw_data' key is missing from input dictionary
        Exception: For any other processing errors during transformation
    """
    
    logger.info("Starting transform execution")
    
    try:
        raw_data = data['raw_data']
        logger.debug("Retrieved raw data for transformation")
        
        raw_current_df, raw_forecast_df, raw_location_df = _get_parsed_weather_dataframes(raw_data)
        logger.debug("Successfully parsed weather dataframes")
        
        result = {
            'raw_location': raw_location_df,
            'raw_current': raw_current_df,
            'raw_forecast': raw_forecast_df
        }
        
        logger.info("Successfully completed transform execution")
        return result
        
    except Exception as e:
        logger.error(f"Transform execution failed: {str(e)}")
        raise
    
 