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
    - get_run_id: Generate or retrieve a run ID for the pipeline execution
    - set_env: Set essential environment variables for the application.


"""

from pathlib import Path
from typing import Dict, List,Optional,Any
import pandas as pd
import logging
import json
from datetime import datetime
import os
import shutil
from uuid import uuid4

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

def load_step_data(step: str, state_dir: Path, logger: logging.Logger) -> Dict[str,pd.DataFrame]:
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
) -> Dict[str,Any]:
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

def clean_directory(directorys: List[Path], logger: logging.Logger, keep_dir=True) -> None:
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
def get_run_id(state_dir: Path) -> str:
    """
    Generate or retrieve a run ID for the pipeline execution.
    
    This function either retrieves an existing run ID from a .run_id file
    in the state directory or generates a new one using UUID4.
    
    Args:
        state_dir (Path): Directory path where run ID state files are stored.
        
    Returns:
        str: The run ID either retrieved from existing file or newly generated.
        
    """
    # Check if any .run_id file exists in the state directory
    if list(state_dir.glob('*.run_id')):
        # If exists, return the stem of the state directory as run ID
        return Path(list(state_dir.glob('*.run_id'))[0]).stem
    else:
        # Generate new UUID4 for run ID if no existing file found
        run_id = str(uuid4())
        
        # Create state directory if it doesn't exist
        # parents=True allows creation of parent directories if needed
        state_dir.mkdir(exist_ok=True, parents=True)
        
        # Create an empty .run_id file with the generated UUID
        # exist_ok=True prevents errors if file already exists
        state_dir.joinpath(f'{run_id}.run_id').touch(exist_ok=True)
        
        return run_id


def set_env(env: str, run_id: str, app_name: str, metrics_dir: str) -> None:
    """
    Set essential environment variables for the application.
    
    This function sets up required environment variables for application
    configuration and metrics tracking.
    
    Args:
        env (str): Environment name (e.g., 'development', 'production')
        run_id (str): Unique identifier for the current execution
        app_name (str): Name of the application
        metrics_dir (str): Directory path for storing metrics
    """
    # Set environment variable for deployment environment
    os.environ['ENV'] = env
    
    # Set environment variable for current execution run ID
    os.environ['RUN_ID'] = run_id
    
    # Set environment variable for application name
    os.environ['APP_NAME'] = app_name
    
    # Set environment variable for metrics directory path
    # Convert to string in case Path object is passed
    os.environ['METRICS_DIR'] = str(metrics_dir)
