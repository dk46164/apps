# def _validate_config(config: Dict) -> None:
#     """Validate the configuration file has all required fields."""
#     required_sections = ['paths', 'steps', 'logging']
#     required_paths = ['input_dir', 'output_dir', 'checkpoint_dir', 'state_dir']
    
#     if not all(section in config for section in required_sections):
#         raise ValueError(f"Config must contain sections: {required_sections}")
        
#     if not all(path in config['paths'] for path in required_paths):
#         raise ValueError(f"Paths section must contain: {required_paths}")