"""
checkpoints.py

Pipeline State Management Module

This module provides functionality for managing pipeline execution state through 
checkpoints and state directories. It handles checkpoint creation, verification,
and determines which pipeline steps need to be executed based on current state.


Functions:
    - setup_directories: Creates checkpoint and state directories
    - create_checkpoint: Creates checkpoint file for completed step
    - check_checkpoint: Verifies if checkpoint exists for step
    - check_pipeline_state: Validates overall pipeline state
    - get_required_steps: Determines steps needing execution


"""


from pathlib import Path
from typing import Optional, List
import logging
from typing import List, Optional, Tuple

def setup_directories(root_dir: Path,steps:List[str]) -> tuple[Path, Path]:
    """Create and return checkpoint and state directories."""
    checkpoint_dir = Path(root_dir).joinpath("checkpoints")
    state_dir = Path(root_dir).joinpath("state")
    
    checkpoint_dir.mkdir(exist_ok=True, parents=True)
    state_dir.mkdir(exist_ok=True, parents=True)
    
    for step in steps:
        Path(state_dir).joinpath(step).mkdir(exist_ok=True, parents=True)
        
    return checkpoint_dir, state_dir

def create_checkpoint(checkpoint_dir: Path, step: str) -> None:
    """Create checkpoint file for completed step."""
    Path(checkpoint_dir).joinpath(f"{step}.done").touch()

def check_checkpoint(checkpoint_dir: Path, step: str) -> bool:
    """Check if checkpoint exists for step."""
    return Path(checkpoint_dir).joinpath(f"{step}.done").exists()


def check_pipeline_state(
    state_dir: Path,
    checkpoint_dir: Path,
    pipeline_steps: List[str]
) -> Tuple[bool, Optional[str]]:
    """
    Check pipeline state and determine if it needs to run from beginning.
    
    Args:
        state_dir: Directory containing state files
        checkpoint_dir: Directory containing checkpoint (.done) files
        pipeline_steps: List of pipeline steps in order
    
    Returns:
        Tuple[bool, Optional[str]]:
            - Boolean indicating if pipeline should run from beginning
            - Last completed step (if any, None if should start from beginning)
    """
    # Check if directories exist
    if not state_dir.exists() or not checkpoint_dir.exists():
        return True, None

    # Check if directories are empty
    state_empty = not any(state_dir.iterdir())
    checkpoint_empty = not any(checkpoint_dir.iterdir())

    # If both directories are empty, start from beginning
    if state_empty and checkpoint_empty:
        return True, None

    # Check for completed steps in checkpoint directory
    last_completed = None
    for step in reversed(pipeline_steps):
        if Path(checkpoint_dir).joinpath(f"{step}.done").exists():
            last_completed = step
            # Verify corresponding state exists
            if not Path(state_dir).joinpath(step).exists():
                # State missing for completed step, start from beginning
                return True, None
            break

    return False, last_completed

def get_required_steps(
    pipeline_steps: List[str],
    state_dir: Path,
    checkpoint_dir: Path,
    logger:logging.Logger,
    failed_step: Optional[str]= None,
    target_step: Optional[str]= None
) -> List[str]:
    """
    Determine which steps need to be executed based on pipeline state.
    
    Args:
        failed_step: Step from which to resume (if any)
        target_step: Step at which to stop (defaults to last step)
        pipeline_steps: List of all pipeline steps in order
        state_dir: Directory containing step state files
        checkpoint_dir: Directory containing checkpoint files
        
    Returns:
        List of steps that need to be executed
    """
    # Check pipeline state
    run_from_beginning, last_completed = check_pipeline_state(
        state_dir, 
        checkpoint_dir, 
        pipeline_steps
    )
    
    if run_from_beginning:
        logger.info("Pipeline will run from beginning due to inconsistent or completed state state")
        return pipeline_steps[:pipeline_steps.index(target_step) + 1] if target_step else pipeline_steps
    
    # Set target step to last step if not specified
    if target_step is None:
        target_step = pipeline_steps[-1]
    
    # Validate steps
    if target_step not in pipeline_steps:
        raise ValueError(f"Target step '{target_step}' not in pipeline steps")
    if failed_step and failed_step not in pipeline_steps:
        raise ValueError(f"Failed step '{failed_step}' not in pipeline steps")
    
    # Check which steps are completed
    done_files = {
        step: Path(checkpoint_dir).joinpath(f"{step}.done").exists() 
        for step in pipeline_steps
    }
    
    # Determine start index
    if failed_step:
        # Start from the step after the failed step
        start_idx = pipeline_steps.index(failed_step) + 1
    else:
        # Find first incomplete step
        try:
            start_idx = next(
                i for i, step in enumerate(pipeline_steps) 
                if not done_files[step]
            )
        except StopIteration:
            # All steps are complete
            return pipeline_steps
    
    end_idx = pipeline_steps.index(target_step) + 1
    required_steps = pipeline_steps[start_idx:end_idx]
    
    # Filter out completed steps unless they come after the failed step
    if not failed_step:
        required_steps = [
            step for step in required_steps 
            if not done_files[step]
        ]
    
    return required_steps
