"""
weather_data_profiling.py

This script contains a function to fill missing values in a DataFrame based on column data types.

Functions: 
    - fill_missing_values:
        - Fill integer columns with 0.
        - Fill float columns with 0.0.
        - Fill string/object columns with an empty string.
    - profile_weather_data: profile weather data with correct dtypes
"""
import pandas as pd
from typing import List,Dict
import logging
import os

def _fill_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills missing values in a DataFrame based on column data types:
    - Integer columns: filled with 0
    - Float columns: filled with 0.0 and rounded to 2 decimal places
    - String/Object columns: filled with empty string and converted to uppercase

    Args:
        df (pd.DataFrame): Input DataFrame with potential missing values

    Returns:
        pd.DataFrame: Processed DataFrame with filled missing values
    """
    # get logger for step.app_name
    logger = logging.getLogger(f'{os.environ['APP_NAME']}.profile')

    # Define fill values for each data type
    fill_logic = {
        'int': 0,
        'float': 0.0,
        'object': '',
        'str': ''
    }
    
    # Create a copy to preserve the original DataFrame
    df = df.copy()
    logger.info(f"Starting missing value filling for DataFrame with {len(df)} rows")
    
    # Process each column based on its data type
    for column in df.columns:
        null_count = df[column].isnull().sum()
        if null_count > 0:
            logger.debug(f"Found {null_count} missing values in column '{column}'")
            
        if pd.api.types.is_integer_dtype(df[column]):
            df[column] = df[column].fillna(fill_logic['int'])
            logger.debug(f"Filled integer column '{column}' with {fill_logic['int']}")
            
        elif pd.api.types.is_float_dtype(df[column]):
            df[column] = df[column].fillna(fill_logic['float']).round(2)
            logger.debug(f"Filled float column '{column}' with {fill_logic['float']}")
            
        elif pd.api.types.is_object_dtype(df[column]) or pd.api.types.is_string_dtype(df[column]):
            df[column] = df[column].fillna(fill_logic['object']).str.strip()
            df[column] = df[column].str.upper()
            logger.debug(f"Filled string column '{column}' with empty string and converted to uppercase")

    logger.info("Completed missing value filling process")
    return df

def _profile_weather_data(current_df: pd.DataFrame, forecast_df: pd.DataFrame, location_df: pd.DataFrame) -> List[pd.DataFrame]:
    """
    Processes multiple weather-related DataFrames by filling missing values and standardizing formats.

    Args:
        current_df (pd.DataFrame): Current weather conditions data
        forecast_df (pd.DataFrame): Weather forecast data
        location_df (pd.DataFrame): Location reference data

    Returns:
        List[pd.DataFrame]: List of processed DataFrames [current_df, forecast_df, location_df]
    """
    # get logger for step.app_name
    logger = logging.getLogger(f'{os.environ['APP_NAME']}.profile')
    logger.info("Starting weather data profiling")
    
    # Process each DataFrame and track the operation
    processed_dfs = []
    for df_name, df in [("current", current_df), ("forecast", forecast_df), ("location", location_df)]:
        logger.info(f"Processing {df_name} DataFrame")
        processed_df = _fill_missing_values(df)
        processed_dfs.append(processed_df)
        logger.info(f"Completed processing {df_name} DataFrame")
    return processed_dfs

def execute_profile(data: Dict[str, pd.DataFrame], config_dict: Dict, logger: logging.Logger):
    """
    Main execution function for weather data profiling.

    Args:
        data (Dict[str, pd.DataFrame]): Dictionary containing raw DataFrames
        config_dict (Dict): Configuration parameters
        logger (logging.Logger): Logger instance

    Returns:
        Dict: Dictionary containing processed DataFrames
    """
    logger.info("Starting weather data profiling execution")
    
    # Extract raw DataFrames from input dictionary
    raw_current_df = data['raw_current']
    raw_forecast_df = data['raw_forecast']
    raw_location_df = data['raw_location']
    
    logger.info(f"Processing {len(raw_current_df)} current records, "
                f"{len(raw_forecast_df)} forecast records, "
                f"{len(raw_location_df)} location records")
    
    # Process the DataFrames
    current_df, forecast_df, location_df = _profile_weather_data(
        raw_current_df, raw_forecast_df, raw_location_df
    )
    
    logger.info("Weather data profiling completed successfully")
    
    return {
        'current_df': current_df,
        'forecast_df': forecast_df,
        'location_df': location_df
    }