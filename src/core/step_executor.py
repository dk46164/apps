"""
step_executor.py

This script implements a pipeline execution function that runs individual data processing steps 
for weather data analysis. It handles extraction, transformation, dq_chekcs, analysis and loading
of weather data while providing state management, checkpoints and logging.

Functions:
    - execute_step: Executes a single pipeline step with state management and checkpointing

Key features:
    - Executes pipeline steps based on function mapping
    - Saves state after each step
    - Creates checkpoints for resume capability
    - Comprehensive logging
    - Error handling and reporting

"""
from typing import Dict, Any
import logging
from pathlib import Path
from src.steps.extract import weather_data_reader
from src.steps.transform import weather_data_processing
from src.steps.dq_checks import weather_data_quality_checks
from src.steps.analyze import weather_temperature_analysis 
from src.steps.load import weather_csv_writer
from src.utils import state, checkpoints

def execute_step(
    step: str,
    data: Dict[str, Any], 
    state_dir: Path,
    checkpoint_dir: Path,
    config_dict: Dict,
    logger: logging.Logger
) -> Dict[str, Any]:
    """
    Execute a single pipeline step with state management and checkpointing.
    
    Args:
        step: Name of the pipeline step to execute
        data: Input data dictionary for the step
        state_dir: Directory to save step state
        checkpoint_dir: Directory to save checkpoints
        config_dict: Configuration parameters
        logger: Logger instance for tracking execution
        
    Returns:
        Dict containing the step execution results
        
    Raises:
        Exception: If step execution fails
    """
    
    # Log start of step execution
    logger.info(f"Starting execution of pipeline step: {step}")
    
    # Map step names to their implementation functions
    step_functions = {
        'extract': weather_data_reader.execute_extract,
        'transform': weather_data_processing.execute_transform,
        'dq_checks': weather_data_quality_checks.execute_dq_checks,
        'analyze': weather_temperature_analysis.execute_analyze,
        'load': weather_csv_writer.execute_load
    }
    
    try:
        # Execute the step function
        logger.debug(f"Calling {step} function with input data")
        result = step_functions[step](data, config_dict, logger)
        
        # Save state and create checkpoint
        logger.debug(f"Saving state for step: {step}")
        state.save_step_data(result, step, state_dir)
        
        logger.debug(f"Creating checkpoint for step: {step}")
        checkpoints.create_checkpoint(checkpoint_dir, step)
        
        logger.info(f"Successfully completed step: {step}")
        return result
        
    except Exception as e:
        # Log error details and re-raise
        logger.error(f"Failed to execute step {step}: {str(e)}", exc_info=True)
        raise