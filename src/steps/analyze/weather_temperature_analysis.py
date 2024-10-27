"""
temperature_analysis.py

This script provides functions to analyze temperature data, specifically focusing on
forecasted and current temperatures.

Functions:
- calculate_forecasted_to_current_temp_diff: Computes the difference between forecasted 
  and current temperatures in both Celsius and Fahrenheit.
- get_max_temperature_per_city: Aggregates maximum, mean, and minimum temperatures per city.
- get_day_of_max_temperature: Determines the day with the highest recorded maximum temperature 
  for each city.

"""

import pandas as pd
from typing import Dict
import logging

def _calculate_forecasted_to_current_temp_diff(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the difference between forecasted and current temperatures in Celsius and Fahrenheit.

    Args:
        df (pd.DataFrame): DataFrame containing columns 'day_avgtemp_c', 'temp_c', 'day_avgtemp_f', and 'temp_f'.

    Returns:
        pd.DataFrame: Updated DataFrame with added temperature difference columns.
    """
    df['forecasted_celsius_diff'] = df.apply(lambda row: row['day_avgtemp_c'] - row['temp_c'], axis=1)
    df['forecasted_fahrenheit_diff'] = df.apply(lambda row: row['day_avgtemp_f'] - row['temp_f'], axis=1)
    return df

def _get_max_temperature_per_city(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get maximum, mean, and minimum temperatures per city.

    Args:
        df (pd.DataFrame): DataFrame containing temperature data with columns 'name', 'region', 'country',
                           'day_avgtemp_c', and 'day_avgtemp_f'.

    Returns:
        pd.DataFrame: Aggregated DataFrame with max, mean, and min temperatures for each city.
                      Column names are formatted to indicate the type of aggregation.
    """
    aggregation_columns = ['name', 'region', 'country']
    aggregated_df = df.groupby(aggregation_columns).agg({
        'day_avgtemp_c': ['max', 'mean', 'min'],
        'day_avgtemp_f': ['max', 'mean', 'min']
    }).reset_index()
    
    # Flatten MultiIndex columns
    aggregated_df.columns = ['_'.join(col).strip() for col in aggregated_df.columns]
    
    return aggregated_df

def _get_day_of_max_temperature(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get the day with the maximum temperature for each city.

    Args:
        df (pd.DataFrame): DataFrame containing temperature data with columns 'name', 'region',
                           'date', 'day_maxtemp_c', and 'day_maxtemp_f'.

    Returns:
        pd.DataFrame: DataFrame with the day of maximum temperature for each city,
                      including columns for city name, region, date, maximum temperatures,
                      and day name.
    """
    max_temp_df = df.loc[df.groupby('name')['day_maxtemp_c'].idxmax()][['name', 'region', 'date', 'day_maxtemp_c', 'day_maxtemp_f']]
    
    # Convert date to day name
    max_temp_df['day_name'] = pd.to_datetime(max_temp_df['date'], format='%Y-%m-%d', errors='coerce').dt.day_name()
    
    return max_temp_df

def execute_analyze(data: Dict[str,pd.DataFrame], config_dict: Dict, logger: logging.Logger) -> Dict:
    """Execute extract step."""

    location_df, current_df,forecast_df = data['location_df'],data['current_df'],data['forecast_df']

    # merge current_df, forecasted_df
    merged_curr_forc_df = current_df.merge(forecast_df,on  = ['name','country'])
    return dict(
        location_df=location_df,
        current_df=current_df,
        forecast_df=forecast_df,
        max_temp_df = _get_day_of_max_temperature(merged_curr_forc_df),
        forcasted_agg_df = _get_max_temperature_per_city(merged_curr_forc_df),
        curr_forc_temp_diff_df = _calculate_forecasted_to_current_temp_diff(merged_curr_forc_df)
    )