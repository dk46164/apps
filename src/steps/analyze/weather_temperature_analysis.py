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
import os

def _calculate_forecasted_to_current_temp_diff(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the difference between forecasted and current temperatures in Celsius and Fahrenheit.

    Args:
        df (pd.DataFrame): DataFrame containing columns 'day_avgtemp_c', 'temp_c', 
                          'day_avgtemp_f', and 'temp_f'.

    Returns:
        pd.DataFrame: Updated DataFrame with added temperature difference columns:
                     - forecasted_celsius_diff: difference in Celsius
                     - forecasted_fahrenheit_diff: difference in Fahrenheit
    """
    # get logger for step.app_name
    logger = logging.getLogger(f'{os.environ['APP_NAME']}.analyze')
    logger.info("Starting temperature difference calculations")

    try:
        # Verify required columns exist
        required_columns = ['day_avgtemp_c', 'temp_c', 'day_avgtemp_f', 'temp_f']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Calculate temperature differences
        logger.debug("Calculating Celsius temperature differences")
        df['forecasted_celsius_diff'] = df.apply(
            lambda row: row['day_avgtemp_c'] - row['temp_c'], axis=1
        )

        logger.debug("Calculating Fahrenheit temperature differences")
        df['forecasted_fahrenheit_diff'] = df.apply(
            lambda row: row['day_avgtemp_f'] - row['temp_f'], axis=1
        )

        # Log summary statistics
        logger.info(f"Temperature differences calculated for {len(df)} records")
        logger.debug(f"Celsius diff range: {df['forecasted_celsius_diff'].min():.2f} to {df['forecasted_celsius_diff'].max():.2f}")
        logger.debug(f"Fahrenheit diff range: {df['forecasted_fahrenheit_diff'].min():.2f} to {df['forecasted_fahrenheit_diff'].max():.2f}")

        return df

    except Exception as e:
        logger.error(f"Error calculating temperature differences: {str(e)}")
        raise

def _get_max_temperature_per_city(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get maximum, mean, and minimum temperatures per city.

    Args:
        df (pd.DataFrame): DataFrame containing temperature data with columns 'name', 
                          'region', 'country', 'day_avgtemp_c', and 'day_avgtemp_f'.

    Returns:
        pd.DataFrame: Aggregated DataFrame with max, mean, and min temperatures for each city.
    """
    # get logger for step.app_name
    logger = logging.getLogger(f'{os.environ['APP_NAME']}.analyze')
    logger.info("Starting temperature aggregation per city")

    try:
        # Define grouping columns and verify their existence
        aggregation_columns = ['name', 'region', 'country']
        missing_columns = [col for col in aggregation_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Perform aggregation
        logger.debug(f"Aggregating temperatures for {df['name'].nunique()} cities")
        aggregated_df = df.groupby(aggregation_columns).agg({
            'day_avgtemp_c': ['max', 'mean', 'min'],
            'day_avgtemp_f': ['max', 'mean', 'min']
        }).reset_index()

        # Flatten column names
        logger.debug("Flattening multi-index columns")
        aggregated_df.columns = ['_'.join(col).strip() for col in aggregated_df.columns]

        # Log summary statistics
        logger.info(f"Temperature aggregation completed for {len(aggregated_df)} cities")
        logger.debug(f"Temperature ranges (Celsius) - Max: {aggregated_df['day_avgtemp_c_max'].max():.2f}, "
                    f"Min: {aggregated_df['day_avgtemp_c_min'].min():.2f}")

        return aggregated_df

    except Exception as e:
        logger.error(f"Error during temperature aggregation: {str(e)}")
        raise

def _get_day_of_max_temperature(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get the day with the maximum temperature for each city.

    Args:
        df (pd.DataFrame): DataFrame containing temperature data with columns 'name', 
                          'region', 'date', 'day_maxtemp_c', and 'day_maxtemp_f'.

    Returns:
        pd.DataFrame: DataFrame with the day of maximum temperature for each city.
    """
    # get logger for step.app_name
    logger = logging.getLogger(f'{os.environ['APP_NAME']}.analyze')
    logger.info("Starting maximum temperature day analysis")

    try:
        # Verify required columns
        required_columns = ['name', 'region', 'date', 'day_maxtemp_c', 'day_maxtemp_f']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Find days with maximum temperature
        logger.debug("Identifying days with maximum temperature")
        max_temp_df = df.loc[df.groupby('name')['day_maxtemp_c'].idxmax()][
            ['name', 'region', 'date', 'day_maxtemp_c', 'day_maxtemp_f']
        ]

        # Convert dates to day names
        logger.debug("Converting dates to day names")
        max_temp_df['day_name'] = pd.to_datetime(
            max_temp_df['date'], 
            format='%Y-%m-%d', 
            errors='coerce'
        ).dt.day_name()

        # Log summary
        logger.info(f"Maximum temperature days identified for {len(max_temp_df)} cities")
        logger.debug(f"Date range: {max_temp_df['date'].min()} to {max_temp_df['date'].max()}")
        logger.debug(f"Maximum temperature range: {max_temp_df['day_maxtemp_c'].min():.2f}°C to "
                    f"{max_temp_df['day_maxtemp_c'].max():.2f}°C")

        return max_temp_df

    except Exception as e:
        logger.error(f"Error during maximum temperature day analysis")

def execute_analyze(data: Dict[str, pd.DataFrame], config_dict: Dict, logger: logging.Logger) -> Dict:
    """
    Execute temperature analysis on weather data, including temperature differences,
    maximum temperatures, and temperature aggregations per city.

    Args:
        data (Dict[str, pd.DataFrame]): Dictionary containing:
            - location_df: DataFrame with location information
            - current_df: DataFrame with current weather data
            - forecast_df: DataFrame with forecast weather data
        config_dict (Dict): Configuration parameters
        logger (logging.Logger): Logger instance for tracking execution

    Returns:
        Dict: Dictionary containing processed DataFrames:
            - location_df: Original location DataFrame
            - current_df: Original current weather DataFrame
            - forecast_df: Original forecast DataFrame
            - max_temp_df: DataFrame with maximum temperature days per city
            - forcasted_agg_df: DataFrame with temperature aggregations per city
            - curr_forc_temp_diff_df: DataFrame with temperature differences
    """
    logger.info("Starting temperature analysis execution")

    # Extract input DataFrames
    location_df = data['location_df']
    current_df = data['current_df']
    forecast_df = data['forecast_df']

    logger.info(f"Processing data for {len(current_df)} locations")
    
    try:
        # Merge current and forecast DataFrames
        logger.debug("Merging current and forecast weather data")
        merged_curr_forc_df = current_df.merge(
            forecast_df,
            on=['name', 'country']
        )
        logger.info(f"Successfully merged DataFrames, resulting in {len(merged_curr_forc_df)} records")

        # Calculate maximum temperature days
        logger.debug("Calculating days with maximum temperatures")
        max_temp_df = _get_day_of_max_temperature(merged_curr_forc_df)
        logger.info(f"Identified maximum temperature days for {len(max_temp_df)} cities")

        # Calculate temperature aggregations
        logger.debug("Computing temperature aggregations per city")
        forcasted_agg_df = _get_max_temperature_per_city(merged_curr_forc_df)
        logger.info(f"Generated temperature aggregations for {len(forcasted_agg_df)} cities")

        # Calculate temperature differences
        logger.debug("Computing forecasted vs current temperature differences")
        curr_forc_temp_diff_df = _calculate_forecasted_to_current_temp_diff(merged_curr_forc_df)
        logger.info("Successfully calculated temperature differences")

        # Prepare output dictionary
        result_dict = {
            'location_df': location_df,
            'current_df': current_df,
            'forecast_df': forecast_df,
            'max_temp_df': max_temp_df,
            'forcasted_agg_df': forcasted_agg_df,
            'curr_forc_temp_diff_df': curr_forc_temp_diff_df
        }

        # Log summary statistics
        logger.info("Analysis summary:")
        for df_name, df in result_dict.items():
            logger.info(f"- {df_name}: {len(df)} records")

        logger.info("Temperature analysis completed successfully")
        return result_dict

    except pd.errors.MergeError as e:
        logger.error(f"Error merging DataFrames: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during temperature analysis: {str(e)}")
        raise