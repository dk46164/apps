"""
weather_data_profiling.py

This script contains a function to fill missing values in a DataFrame based on column data types.

Functions: 
    - _fill_missing_values:
        - Fill integer columns with 0.
        - Fill float columns with 0.0.
        - Fill string/object columns with an empty string.
    - _validate_location_data: Perform Data quality checks on location data
    - _validate_forecast_weather_data: Perform Data quality checks on forecasted weather data
    - _validate_current_weather_data: Perform Data quality checks on current weather data
    - profile_weather_data: profile weather data with correct dtypes
"""
import pandas as pd
from typing import List,Dict,Tuple
import logging
import os
from pathlib import Path


def _validate_location_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Performs data quality validation on weather location data and separates valid and invalid records.

    Args:
        df (pd.DataFrame): Input DataFrame containing location information with columns:
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing:
            - valid_df: DataFrame with location records that passed validation
            - invalid_df: DataFrame with location records that failed validation
    """
    # Initialize invalid records tracker
    invalid_records = pd.Series(False, index=df.index)

    # Coordinate validation
    lat_invalid = ~df['lat'].between(-90, 90)
    invalid_records |= lat_invalid

    lon_invalid = ~df['lon'].between(-180, 180)
    invalid_records |= lon_invalid

    # Name validation (should be non-empty strings)
    name_invalid = (df['name'].str.len() == 0) | (df['region'].str.len() == 0) | (df['country'].str.len() == 0)
    invalid_records |= name_invalid

    # Split into valid and invalid records
    valid_df = df[~invalid_records]
    invalid_df = df[invalid_records]

    return  valid_df, invalid_df


def _validate_forecast_weather_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Performs data quality validation on forecast weather data and separates valid and invalid records.

    Args:
        df (pd.DataFrame): Input DataFrame containing forecast weather readings with columns:

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing:
            - valid_df: DataFrame with forecast records that passed validation
            - invalid_df: DataFrame with forecast records that failed validation
    """
    # Initialize invalid records tracker
    invalid_records = pd.Series(False, index=df.index)

    # Temperature range validations
    temp_max_invalid = ~(df['day_maxtemp_c'].between(0, 50) & 
                        df['day_maxtemp_f'].between(32, 122))
    invalid_records |= temp_max_invalid

    temp_min_invalid = ~(df['day_mintemp_c'].between(-50, 50) & 
                        df['day_mintemp_f'].between(-58, 122))
    invalid_records |= temp_min_invalid

    temp_avg_invalid = ~df['day_avgtemp_c'].between(-50, 50)
    invalid_records |= temp_avg_invalid

    # Humidity validation
    humidity_invalid = ~df['day_avghumidity'].between(0, 100)
    invalid_records |= humidity_invalid

    # Precipitation validation
    precip_invalid = df['day_totalprecip_mm'] < 0
    invalid_records |= precip_invalid

    # Split into valid and invalid records
    valid_df = df[~invalid_records]
    invalid_df = df[invalid_records]

    return valid_df, invalid_df

def _validate_current_weather_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Performs data quality validation on current weather data and separates valid and invalid records.

    Args:
        df (pd.DataFrame): Input DataFrame containing current weather

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing:
            - valid_df: DataFrame with current weather records that passed validation
            - invalid_df: DataFrame with current weather records that failed validation
    """
    # Initialize a boolean Series to track invalid records
    invalid_records = pd.Series(False, index=df.index)

    # Check if temperatures are within valid ranges:
    # Celsius between -50 and 50
    # Fahrenheit between -58 and 122
    temp_invalid = ~(df['temp_c'].between(-50, 50) & df['temp_f'].between(-58, 122))
    invalid_records |= temp_invalid

    # Verify temperature conversion accuracy
    # Convert Celsius to Fahrenheit and check if difference is > 0.1
    temp_conversion_invalid = abs((df['temp_c'] * 9/5 + 32) - df['temp_f']) > 0.1 
    invalid_records |= temp_conversion_invalid

    # Validate is_day column only contains 0 or 1
    invalid_is_day = ~df['is_day'].isin([0, 1])
    invalid_records |= invalid_is_day

    # Split data into valid and invalid records
    valid_df = df[~invalid_records]
    invalid_df = df[invalid_records]

    return valid_df, invalid_df

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
    logger = logging.getLogger(f'{os.environ['APP_NAME']}.dq_checks')

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

def _profile_weather_data(current_df: pd.DataFrame, forecast_df: pd.DataFrame, location_df: pd.DataFrame) -> List[List[pd.DataFrame]]:
    """
    Processes multiple weather-related DataFrames by filling missing values and standardizing formats.

    Args:
        current_df (pd.DataFrame): Current weather conditions data
        forecast_df (pd.DataFrame): Weather forecast data
        location_df (pd.DataFrame): Location reference data

    Returns:
        List[List[pd.DataFrame]]: List of processed DataFrames and data quality checks failed DataFrames [current_df, forecast_df, location_df]
    """
    # get logger for step.app_name
    logger = logging.getLogger(f'{os.environ['APP_NAME']}.dq_checks')
    logger.info("Starting weather data profiling")
    
    # Process each DataFrame and track the operation
    processed_dfs = []
    dq_failed_dfs = []

    for df_name, df in [("current", current_df), ("forecast", forecast_df), ("location", location_df)]:
        logger.info(f"Processing {df_name} DataFrame")
        processed_df = _fill_missing_values(df)
        if df_name=='current':
            valid_df,invalid_df = _validate_current_weather_data(processed_df)
            processed_dfs.append(valid_df)
            dq_failed_dfs.append(invalid_df)
        elif df_name=='forecast':
            valid_df,invalid_df = _validate_forecast_weather_data(processed_df)
            processed_dfs.append(valid_df)
            dq_failed_dfs.append(invalid_df)
        else:
            valid_df,invalid_df = _validate_location_data(processed_df)
            processed_dfs.append(valid_df)
            dq_failed_dfs.append(invalid_df)

        logger.info(f"Completed processing {df_name} DataFrame")
    return (processed_dfs,dq_failed_dfs)

def execute_dq_checks(data: Dict[str, pd.DataFrame], config_dict: Dict, logger: logging.Logger):
    """
    Main execution function for weather data quality checks.

    Args:
        data (Dict[str, pd.DataFrame]): Dictionary containing raw DataFrames
        config_dict (Dict): Configuration parameters
        logger (logging.Logger): Logger instance

    Returns:
        Dict: Dictionary containing processed DataFrames
    """
    logger.info("Starting weather data quality checks")
    
    # Extract raw DataFrames from input dictionary
    raw_current_df = data['raw_current']
    raw_forecast_df = data['raw_forecast']
    raw_location_df = data['raw_location']
    
    logger.info(f"Processing {len(raw_current_df)} current records, "
                f"{len(raw_forecast_df)} forecast records, "
                f"{len(raw_location_df)} location records")
    
    # Process the DataFrames
    dq_passed_df ,dq_failed_df = _profile_weather_data(
        raw_current_df, raw_forecast_df, raw_location_df
    )
    current_df, forecast_df, location_df = dq_passed_df

    # sandbox the dq failed df to /sandbox dir 
    sandbox_dir = Path(config_dict['output']).joinpath('sandbox')
    sandbox_dir.mkdir(parents=True,exist_ok=True)

    # prepare the dq failed df
    dq_failed = {'current':dq_failed_df[0],'forecast':dq_failed_df[1],'location':dq_failed_df[0]}

    for name,df in dq_failed.items():
        df.to_csv(sandbox_dir.joinpath(f'{name}.csv'))
    
    logger.info("Weather data quality checks completed successfully")
    
    return {
        'current_df': current_df,
        'forecast_df': forecast_df,
        'location_df': location_df
    }