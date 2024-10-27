"""
weather_data_processing.py

This module provides functions to process weather data by combining location, 
current weather, and forecast information into pandas DataFrames. It utilizes 
concurrent processing to handle multiple datasets efficiently.

Functions:
- combine_weather_data: Combines location and current weather data with forecast data.
- get_transformed_dataframes: Processes multiple weather data inputs concurrently.

"""

from typing import Any, Dict, List,Tuple
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

def _combine_weather_data(key:int,data: Dict[str, Any],logger:logging.Logger) -> Tuple[pd.DataFrame]:
    """
    Combines location and current weather data with forecast data into a single DataFrame.

    Args:
        data (Dict[str, Any]): The input data containing location, current weather, and forecast information.

    Returns:
        pd.DataFrame: A DataFrame with combined weather data.
    """
    try:
        # Extract location and current weather data
        location = data['location']
        current = data['current']
        current_weather_df = pd.json_normalize(current, sep='_')
        location_df = pd.json_normalize(location,sep='_')

        # add city columns
        current_weather_df['name'],current_weather_df['country'] = [val.strip() for val in  key.split(',')]
        current_weather_df['region'] = data['location']['region']

        # Extract forecast data
        forecast = data['forecast']['forecastday']
        forecast_data: List[Dict[str, Any]] = []
        for day in forecast:
            forecast_data.append(dict(**day))
        forecasted_weather_df = pd.json_normalize(forecast_data, sep='_')

        # add city columns
        forecasted_weather_df['name'],forecasted_weather_df['country'] = [val.strip() for val in  key.split(',')]

        return (current_weather_df,forecasted_weather_df,location_df)

    except KeyError as e:
        # Handle missing keys in the input data
        logger.error(f'Missing key {e} in the input data. Returning empty DataFrame.')
        return pd.DataFrame()
    except Exception as e:
        # Handle any other exceptions
        logger.error(f'An error occurred: {e}. Returning empty DataFrame.')
        return pd.DataFrame()

def _get_parsed_weather_dataframes(data: Dict[str, Any],logger:logging.Logger) -> Tuple[pd.DataFrame]:
    """
    Processes multiple weather data inputs using a process pool to combine individual data into DataFrames.

    Args:
        data (Dict[str, Any]): A dictionary of weather data keyed by location identifiers.

    Returns:
        List[pd.DataFrame]: A list of combined DataFrames from the weather data.
    """
    try:
        # Create a process pool to handle multiple data inputs concurrently
        keys = list(data.keys())
        with ProcessPoolExecutor(max_workers=len(keys)) as executor:
            # Submit tasks to combine weather data for each key
            futures = {executor.submit(_combine_weather_data, key,data[key],logger): key for key in keys}
            
            # Collect completed results from futures
            results = []
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    logger.error(f'An error occurred while processing key {futures[future]}: {e}')
                    results.append(pd.DataFrame())

            # concat current, location, forecasted weather 
            current_df = pd.concat([dataframe[0] for dataframe in results])
            forecast_df = pd.concat([dataframe[1] for dataframe in results])
            location_df = pd.concat([dataframe[2] for dataframe in results])


            # return concatnated df
            return (current_df,forecast_df,location_df)

    except Exception as e:
        # Handle any exceptions that occurred during processing
        logger.error(f'An error occurred during data processing: {e}. Returning empty list.')
        return []
    

def execute_transform(data: Dict, config_dict: Dict, logger: logging.Logger) -> Dict:
    """Execute transform step."""
    raw_data = data['raw_data']
    raw_current_df, raw_forecast_df, raw_location_df = _get_parsed_weather_dataframes(raw_data, logger)
    return {
        'raw_location': raw_location_df,
        'raw_current': raw_current_df,
        'raw_forecast': raw_forecast_df
    }
    
 