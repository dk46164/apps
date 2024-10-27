"""
state.py

Pipeline State Management Module

This module handles state management for the ETL pipeline, including:
    - Saving and loading step data
    - Managing metadata and execution history
    - Tracking pipeline state transitions
    - Directory cleanup and maintenance

Functions:
    - save_step_data: Persists step output data
    - load_step_data: Retrieves step data from storage
    - log_metadata: Creates execution metadata records
    - save_step_metadata: Stores step metadata history
    - get_previous_state: Determines previous pipeline state
    - clean_directory: Removes temporary files and directories

"""

from pathlib import Path
from typing import Dict, List,Optional
import pandas as pd
import logging
import json
from datetime import datetime
import os
import shutil

def save_step_data(data: Dict, step: str, state_dir: Path) -> None:
    """
    Save step data to state directory.
    
    Args:
        data: Dictionary containing step output data
        step: Name of the pipeline step
        state_dir: Directory to save state
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Saving data for step: {step}")
    
    step_dir = Path(state_dir).joinpath(step)
    step_dir.mkdir(exist_ok=True, parents=True)
    logger.debug(f"Created state directory: {step_dir}")

    try:
        if step == 'extract':
            with open(step_dir / 'raw_data.json', 'w') as f:
                json.dump(data, f)
            logger.debug("Saved extract step data as JSON")
        else:
            for name, df in data.items():
                output_file = step_dir / f"{name}.csv"
                df.to_csv(output_file, index=False)
                logger.debug(f"Saved DataFrame {name} to {output_file}")
    except Exception as e:
        logger.error(f"Failed to save step data: {str(e)}")
        raise

def load_step_data(step: str, state_dir: Path, logger: logging.Logger) -> Dict:
    """
    Load data for a specific step.
    
    Args:
        step: Name of the pipeline step
        state_dir: Directory containing state data
        logger: Logger instance
        
    Returns:
        Dictionary containing step data
        
    Raises:
        Exception if data cannot be loaded
    """
    logger.info(f"Loading data for step: {step}")
    
    try:
        if step == 'extract':
            data_file = state_dir / step / 'raw_data.json'
            logger.debug(f"Loading JSON data from {data_file}")
            with open(data_file, 'r') as f:
                return json.load(f)
        else:
            data = {}
            step_dir = Path(state_dir).joinpath(step)
            logger.debug(f"Loading CSV files from {step_dir}")
            
            for csv_file in step_dir.glob('*.csv'):
                name = csv_file.name
                logger.debug(f"Loading {name}")
                data[name.replace('.csv','')] = pd.read_csv(csv_file)
            return data
            
    except Exception as e:
        logger.error(f"Failed to load data for {step} step: {str(e)}")
        raise

def log_metadata(
    step: str,
    status: str,
    start_time: datetime,
    duration: int,
    end_time: Optional[datetime] = None,
    input_files: Optional[list] = None,
    output_files: Optional[list] = None,
    error: Optional[str] = None,
    additional_info: Optional[Dict] = None
) -> dict:
    """
    Create a detailed metadata entry for a pipeline step.
    
    Args:
        step: Name of the pipeline step
        status: Status of the step (success/failed/running)
        start_time: When the step started
        end_time: When the step completed (None if still running)
        input_files: List of input files processed
        output_files: List of output files generated
        error: Error message if step failed
        additional_info: Any additional metadata to store
    
    Returns:
        Dictionary containing the metadata
    """
    metadata = {
        'step': step,
        'status': status,
        'start_time': start_time,
        'end_time': end_time if end_time else None,
        'duration': duration if end_time else 0,
        'input_files': input_files or [],
        'output_files': output_files or [],
        'error': error,
        'timestamp': datetime.now().isoformat(),
        'stacktrace':None
    }
    
    if additional_info:
        metadata.update(additional_info)
    
    return metadata

def save_step_metadata(step: str, metadata: dict, metric_dir: Path) -> None:
    """
    Save metadata about a specific pipeline step to a JSON file.
    
    Args:
        step: Name of the pipeline step
        metadata: Dictionary containing the metadata to save
        state_dir: Directory where state/metadata should be stored
    """
    try:
        # Create the directory for the specific step if it doesn't exist
        step_dir = Path(metric_dir).joinpath(step)
        step_dir.mkdir(exist_ok=True, parents=True)
        
        # Define paths for metadata files
        metadata_file = Path(step_dir).joinpath('metadata.json')
        
        # Load existing metadata if the file exists
        existing_metadata = []
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                existing_metadata = json.load(f)
        
        # Append the new metadata entry
        existing_metadata.append(metadata)
        
        # Save the updated metadata history
        with open(metadata_file, 'w') as f:
            json.dump(existing_metadata, f, indent=4)
            
    except Exception as e:
        raise RuntimeError(f"Failed to save metadata for step {step}: {str(e)}")


def get_previous_state(state: str,pipeline_states:List[str]) -> str:
    """
    Returns the previous pipeline state based on the provided state.
    
    Args:
        state (str): Current pipeline state/step name
        
    Returns:
        str: Previous state name, '' for invalid states
        
    """
     
    
    try:
        # Get index of current state
        current_index = pipeline_states.index(state)
        
        # Return start state (0) if we're at the first state
        if current_index == 0:
            return ''
            
        # Return previous state for all other valid states
        return pipeline_states[current_index - 1]
        
    except ValueError:
        # Return -1 for invalid states
        return ''
    

def clean_directory(directorys: List[Path], logger: logging.Logger, keep_dir=True):
    """
    Remove contents of specified directories while optionally preserving the directories themselves.
    
    Args:
        directorys: List of directory paths to clean
        logger: Logger instance for tracking operations
        keep_dir: If True, preserve empty directories; if False, remove directories entirely
        
    Raises:
        Exception: If directory cleanup fails
    """
    try:
        # Keep directory structure but remove contents
        if keep_dir:
            # Iterate through each directory
            for dir in directorys:
                # Process each item in the directory
                for item in os.listdir(dir):
                    item_path = os.path.join(dir, item)
                    
                    # Remove files
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                        logger.debug(f"Removed file: {item_path}")
                        
                    # Remove subdirectories and their contents
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                        logger.debug(f"Removed directory: {item_path}")
                        
            logger.info(f"Cleaned contents while preserving directories: {directorys}")
            
        # Remove directories entirely    
        else:
            shutil.rmtree(directorys)
            logger.info(f"Removed directories completely: {directorys}")
            
    except Exception as e:
        # Log error and re-raise
        logger.error(f"Failed to clean directories {directorys}: {str(e)}")
        raise e