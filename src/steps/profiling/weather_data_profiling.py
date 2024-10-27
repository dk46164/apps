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

def _fill_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills missing values in a DataFrame based on column data types.

    Parameters:
    df (pd.DataFrame): The DataFrame to process.

    Returns:
    pd.DataFrame: A DataFrame with missing values filled.
    """
    # Define fill logic for different data types
    fill_logic = {
        'int': 0,
        'float': 0.0,
        'object': '',
        'str': ''
    }
    
    # Create a copy of the DataFrame to avoid modifying the original
    df = df.copy()
    
    # Iterate over each column and fill missing values based on data type
    for column in df.columns:
        if pd.api.types.is_integer_dtype(df[column]):
            df[column] = df[column].fillna(fill_logic['int'])
        elif pd.api.types.is_float_dtype(df[column]):
            df[column] = df[column].fillna(fill_logic['float']).round(2)
        elif pd.api.types.is_object_dtype(df[column]) or pd.api.types.is_string_dtype(df[column]):
            df[column] = df[column].fillna(fill_logic['object']).str.strip()
            df[column] = df[column].str.upper()

    
    return df

def _profile_weather_data(current_df: pd.DataFrame, forecast_df: pd.DataFrame, location_df: pd.DataFrame) -> List[pd.DataFrame]:
    """
    Profiles weather data by filling missing values in multiple DataFrames.

    Parameters:
    current_df (pd.DataFrame): The current weather data DataFrame.
    forecast_df (pd.DataFrame): The forecast weather data DataFrame.
    location_df (pd.DataFrame): The location data DataFrame.

    Returns:
    List[pd.DataFrame]: A list of DataFrames with missing values filled.
    """
    
    # Apply _fill_missing_values to each DataFrame and return the list of processed DataFrames
    return [_fill_missing_values(df) for df in [current_df, forecast_df, location_df]]


def execute_profile(data: Dict[str,pd.DataFrame], config_dict: Dict, logger: logging.Logger):

    raw_current_df,raw_forecast_df ,raw_location_df= data['raw_current'],data['raw_forecast'],data['raw_location'],
    current_df,forecast_df,location_df = _profile_weather_data(raw_current_df,raw_forecast_df ,raw_location_df)
    return dict(current_df=current_df,forecast_df=forecast_df,location_df=location_df)
