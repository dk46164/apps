"""
weather_csv_writer.py

This script provides functionality to write weather data to CSV files. 

Functions:
- save_dataframe_to_csv: Writes a DataFrame to a specified CSV file path.
- export_weather_data_to_csv: Writes multiple weather data DataFrames to CSV files in a specified directory.

"""

from pathlib import Path
import pandas as pd
from typing import Dict
import logging
import os 

def save_dataframe_to_csv(dataframe: pd.DataFrame, file_path: Path) -> None:
    """
    Write a DataFrame to a CSV file.

    Args:
        dataframe (pd.DataFrame): The DataFrame to be written.
        file_path (Path): The file path where the CSV will be saved.

    Raises:
        Exception: If an error occurs during the file writing process.
    """
    try:
        dataframe.to_csv(file_path,index=False)
    except Exception as e:
        raise e

def _export_weather_data_to_csv(directory_path: str, max_temp_data: pd.DataFrame, 
                               aggregated_data: pd.DataFrame, forecasted_vs_current_data: pd.DataFrame,
                               current_df: pd.DataFrame, forcast_df: pd.DataFrame,
                               location_df: pd.DataFrame) -> None:
    """
    Write weather data DataFrames to CSV files in the specified directory.

    Args:
        directory_path (str): The directory path where CSV files will be saved.
        max_temp_data (pd.DataFrame): DataFrame containing date of maximum temperature data.
        aggregated_data (pd.DataFrame): DataFrame containing aggregated temperature data per City.
        forecasted_vs_current_data (pd.DataFrame): DataFrame containing forecasted vs current temperature differences.
        current_df (pd.DataFrame): DataFrame containing current weather data.
        forcast_df (pd.DataFrame): DataFrame containing forecast weather data.
        location_df (pd.DataFrame): DataFrame containing location information.

    Raises:
        Exception: If an error occurs during directory creation or file writing.
    """
    # Create a dictionary mapping DataFrames to their file names for better organization
    dataframe_mapping = {
        'max_temp.csv': max_temp_data,
        'aggregated_temp.csv': aggregated_data,
        'forecasted_current_temp.csv': forecasted_vs_current_data,
        'current_weather.csv': current_df,
        'forecast_weather.csv': forcast_df,
        'location.csv': location_df
    }

    # Write each DataFrame to a CSV file in the specified directory
    for filename, df in dataframe_mapping.items():
        file_path = Path(directory_path, filename)
        save_dataframe_to_csv(df, file_path)


def execute_load(data: Dict[str, pd.DataFrame], config_dict: Dict, logger: logging.Logger) -> None:
    """
    Execute the data loading process by exporting DataFrames to CSV files.

    Args:
        data (Dict[str, pd.DataFrame]): Dictionary containing various weather DataFrames.
        config_dict (Dict): Configuration dictionary containing output path information.
        logger (logging.Logger): Logger instance for recording execution information.

    Returns:
        Dict: Dictionary containing an empty DataFrame for the load operation.
    """
    try:
        # Log the start of the loading process
        logger.info("Starting data export process")
        
        # Create output path using configuration and environment variable
        output_path = Path(config_dict['output'])
        logger.info(f"Output directory set to: {output_path}")
        
        # Create directory if it doesn't exist
        output_path.mkdir(exist_ok=True, parents=False)
        logger.info("Output directory created/verified")
        
        # Export all DataFrames to CSV files
        _export_weather_data_to_csv(
            output_path,
            data['max_temp_df'],
            data['forcasted_agg_df'],
            data['curr_forc_temp_diff_df'],
            data['current_df'],
            data['forecast_df'],
            data['location_df']
        )
        logger.info("Successfully exported all weather data to CSV files")
        
        return dict(load=pd.DataFrame())
    
    except Exception as e:
        logger.error(f"Error during data export process: {str(e)}")
        raise
