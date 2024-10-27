"""
pipeline.py

Weather Data Pipeline Executor Module

This module implements a pipeline execution function that runs individual data processing steps 
for weather data analysis. The pipeline handles extraction, transformation, profiling, analysis 
and loading of weather data while providing state management, checkpoints and logging.

Functions:
    - execute_pipeline: Main pipeline execution function that:
    - _load_config: Helper function to load YAML configuration file
    - _setup_pipeline_directories: Helper function to create pipeline directories

Key Features:
    - State management for each pipeline step
    - Checkpoint-based execution tracking
    - Detailed logging of pipeline execution
    - Error handling with diagnostics
    - Configurable step execution
    - Data validation and quality checks
"""

from typing import Dict, Any, Optional
from pathlib import Path
import yaml
import time
import os
import sys 
from src.utils import checkpoints, state
from src.core import step_executor
from src.utils import etl_logger

def _load_config(config_path: Path) -> Dict:
    """
    Load and parse YAML configuration file.
    
    Args:
        config_path: Path to the YAML configuration file
        
    Returns:
        Dict containing parsed configuration
        
    Raises:
        RuntimeError: If config file cannot be loaded
    """
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        raise RuntimeError(f"Failed to load configuration file {config_path}: {str(e)}")

def _setup_pipeline_directories(config: Dict, root_dir: Path) -> tuple[Path, Path, Path]:
    """
    Create required pipeline directories based on configuration.
    
    Args:
        config: Pipeline configuration dictionary
        root_dir: Root directory path
        
    Returns:
        Tuple containing paths to checkpoint, state and metrics directories
    """
    # Extract directory paths from config
    checkpoint_dir = Path(root_dir).joinpath(config['paths']['checkpoint']['dir'])
    state_dir = Path(root_dir).joinpath(config['paths']['state']['dir'])
    output_dir = Path(root_dir).joinpath(config['paths']['output']['dir']) 
    metrics_dir = Path(root_dir).joinpath(config['paths']['metrics']['dir'])

    # Create directories if they don't exist
    for directory in [checkpoint_dir, state_dir, output_dir, metrics_dir]:
        directory.mkdir(parents=True, exist_ok=True)
        
    return checkpoint_dir, state_dir, metrics_dir

def execute_pipeline(config_path: Path = 'config/config.yaml') -> Dict[str, Any]:
    """
    Execute data processing pipeline with checkpoint-based resumability.
    
    Args:
        config_path: Path to pipeline configuration YAML file
        
    Returns:
        Dictionary containing results from executed pipeline steps
        
    Raises:
        Exception: If pipeline execution fails
    """
    # Initialize pipeline
    root_dir = Path.cwd()
    config_path = Path(root_dir).joinpath(config_path)
    config = _load_config(config_path)
    data_root_dir = config['paths']['root']
    
    # Setup logging
    logger = etl_logger.get_logger(config_path)
    logger.info(f"Initializing pipeline with config from {config_path}")
    
    # Create pipeline directories
    checkpoint_dir, state_dir, metrics_dir = _setup_pipeline_directories(config, data_root_dir)
    logger.info(f"Created pipeline directories at {data_root_dir}")
    
    # Determine required execution steps
    pipeline_steps = config['steps']['execution_order']
    required_steps = checkpoints.get_required_steps(
        pipeline_steps, state_dir, checkpoint_dir, logger
    )
    logger.info(f"Pipeline will execute steps: {', '.join(required_steps)}")
    
    data = {}
    try:
        # Load dependency data if resuming from checkpoint
        if list(Path(checkpoint_dir).glob('*.done')):
            dependencies = config['steps']['dependencies']
            current_step_deps = dependencies.get(required_steps[0], [])
            for dep_step in current_step_deps:
                logger.info(f"Loading dependency data for {dep_step}")
                data[dep_step] = state.load_step_data(dep_step, state_dir, logger)
    
        # Execute pipeline steps
        for step in required_steps:
            logger.info(f"Starting execution of step: {step}")
            
            # Configure step execution
            step_config = config['steps'].get(step, {})
            step_config.update({
                'output': Path(data_root_dir).joinpath(config['paths']['output']['dir']),
                'input': Path(data_root_dir).joinpath(
                    config['paths']['input']['dir'],
                    config['paths']['input']['weather_file']
                )
            })
            
            prev_step_name = state.get_previous_state(step, pipeline_steps)
            step_start_time = time.time()

            # Execute step with appropriate input data
            if not prev_step_name:
                logger.debug(f"Executing {step} as initial step")
                data[step] = step_executor.execute_step(
                    step=step,
                    data=data,
                    state_dir=state_dir,
                    checkpoint_dir=checkpoint_dir,
                    config_dict=step_config,
                    logger=logger
                )
            else:
                logger.debug(f"Executing {step} with input from {prev_step_name}")
                data[step] = step_executor.execute_step(
                    step=step,
                    data=data[prev_step_name],
                    state_dir=state_dir,
                    checkpoint_dir=checkpoint_dir,
                    config_dict=step_config,
                    logger=logger
                )

            # Record step metadata if enabled
            if config['steps'].get('save_metadata', False):
                step_metadata = state.log_metadata(
                    step=step,
                    start_time=step_start_time,
                    end_time=time.time(),
                    duration=int(time.time()-step_start_time),
                    status='completed',
                    input_files=[dict(key= key,rows = len(val)) for key, val in data[prev_step_name].items()] 
                               if prev_step_name 
                               else [dict(file_name=f'{step_config['input']}',
                                            file_size=f'{os.stat(step_config['input']).st_size / 1024} KB')],
                    output_files=[dict(key=key,rows=len(val)) for key, val in data[step].items()]
                )
                state.save_step_metadata(step, step_metadata, metrics_dir)
                logger.info(f"Saved metadata for step {step}")
                
                # Clean up previous step data
                if prev_step_name:
                    del data[prev_step_name]
                    logger.debug(f"Cleaned up data from {prev_step_name}")

        # Cleanup temporary files
        state.clean_directory([checkpoint_dir, state_dir], logger)
        logger.info("Pipeline completed successfully")
        
        return data
        
    except Exception as e:
        logger.error(f"Pipeline failed during step {step if 'step' in locals() else 'initialization'}")
        logger.error(f"Error details: {str(e)}")
        logger.error(f"Stack trace: {sys.exc_info()}")
        raise