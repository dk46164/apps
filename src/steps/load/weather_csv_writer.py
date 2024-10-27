"""
weather_csv_writer.py

This script provides functionality to write weather data to CSV files. 

Functions:
- save_dataframe_to_csv: Writes a DataFrame to a specified CSV file path.
- export_weather_data_to_csv: Writes multiple weather data DataFrames to CSV files in a specified directory.

"""

from pathlib import Path
import pandas as pd
from typing import Dict,List
import logging
from datetime import datetime 

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

def _export_weather_data_to_csv(directory_path: str, max_temp_data: pd.DataFrame, aggregated_data: pd.DataFrame, forecasted_vs_current_data: pd.DataFrame,current_df:pd.DataFrame,forcast_df:pd.DataFrame,location_df:pd.DataFrame) -> None:
    """
    Write weather data DataFrames to CSV files in the specified directory.

    Args:
        directory_path (str): The directory path where CSV files will be saved.
        max_temp_data (pd.DataFrame): DataFrame containing date of maximum temperature data.
        aggregated_data (pd.DataFrame): DataFrame containing aggregated temperature data per City .
        forecasted_vs_current_data (pd.DataFrame): DataFrame containing forecasted vs current temperature differences.

    Raises:
        Exception: If an error occurs during directory creation or file writing.
    """

    # Write each DataFrame to a CSV file in the specified directory
    save_dataframe_to_csv(max_temp_data, Path(directory_path, 'max_temp.csv'))
    save_dataframe_to_csv(aggregated_data,Path(directory_path,'aggregated_temp.csv'))
    save_dataframe_to_csv(forecasted_vs_current_data,Path(directory_path, 'forecasted_current_temp.csv'))

    # processed raw data
    save_dataframe_to_csv(current_df, Path(directory_path, 'current_weather.csv'))
    save_dataframe_to_csv(forcast_df, Path(directory_path, 'forecast_weather.csv'))
    save_dataframe_to_csv(location_df, Path(directory_path, 'location.csv'))


def execute_load(data: Dict[str,pd.DataFrame], config_dict: Dict, logger: logging.Logger) -> None:
    # output path
    output_path = Path(config_dict['output']).joinpath(datetime.now().strftime('%Y%m%d'))
    # create dir if not exist
    output_path.mkdir(exist_ok=True,parents=False)

    _export_weather_data_to_csv(output_path,data['max_temp_df'],data['forcasted_agg_df'],data['curr_forc_temp_diff_df'],data['current_df'],data['forecast_df'],data['location_df'])
    return dict(load = pd.DataFrame())
