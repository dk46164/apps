# tests/test_weather_etl.py

import os
import json
import pytest
import yaml
import pandas as pd

# Fixtures
@pytest.fixture(scope="session")
def etl_config():
    """Session fixture for ETL configuration"""
    config_path = os.path.join('config', 'config.yaml')
    with open(config_path) as f:
        return yaml.safe_load(f)

@pytest.fixture(scope="session")
def execution_id():
    """Session fixture for execution ID"""
    return "9f01de68-eba6-4bcd-ac85-63db12f5e9f5"

# Input Validation Tests
class TestInputValidation:
    def test_input_file_exists(self):
        """Test if input JSON file exists and is valid"""
        input_file = 'data/input/ETL_developer_Case.json'
        assert os.path.exists(input_file), "Input file missing"
        
        with open(input_file) as f:
            data = json.load(f)
            assert isinstance(data, dict), "Invalid JSON format"


    def test_directory_structure(self):
        """Test if required directories exist"""
        required_dirs = [
            'data/input',
            'data/output',
            'data/logs',
            'data/metrics'
        ]
        for dir_path in required_dirs:
            assert os.path.isdir(dir_path), f"Required directory {dir_path} missing"

# ETL Step Tests
class TestETLSteps:
    def test_extract_step(self, execution_id):
        """Test data extraction step"""
        log_file = f'data/logs/{execution_id}/extract/extract.log'
        assert os.path.exists(log_file), "Extract log not created"
        
        metrics_file = f'data/metrics/{execution_id}/extract/metadata.json'
        assert os.path.exists(metrics_file), "Extract metrics not created"

    def test_transform_step(self, execution_id):
        """Test data transformation step"""
        output_files = [
            'current_weather.csv',
            'forecast_weather.csv',
            'location.csv',
            'aggregated_temp.csv',
            'max_temp.csv'
        ]
        for file in output_files:
            path = f'data/output/{execution_id}/{file}'
            assert os.path.exists(path), f"Missing output file: {file}"

    @pytest.mark.parametrize("step", ["extract", "transform", "load", "analyze", "profile"])
    def test_step_metrics(self, execution_id, step):
        """Test metrics for each ETL step"""
        metric_file = f'data/metrics/{execution_id}/{step}/metadata.json'
        assert os.path.exists(metric_file), f"Missing metrics for {step}"
        
        with open(metric_file) as f:
            metrics = json.load(f)[0]
            assert 'duration' in metrics, f"Missing execution time in {step} metrics"
        
# Pipeline Execution Tests
class TestPipelineExecution:
    def test_pipeline_logs(self, execution_id):
        """Test pipeline execution logs"""
        main_log = f'data/logs/{execution_id}/weather_etl.log'
        assert os.path.exists(main_log), "Main pipeline log missing"
        
        with open(main_log) as f:
            log_content = f.read()
            assert "Pipeline completed successfully" in log_content, "Pipeline execution failed"

    def test_execution_metrics(self, execution_id):
        """Test overall execution metrics"""
        metrics_files = [
            f'data/metrics/{execution_id}/{step}/metadata.json'
            for step in ['extract', 'transform', 'load']
        ]
        
        total_time = 0
        for file in metrics_files:
            with open(file) as f:
                metrics = json.load(f)[0]
                total_time += metrics['duration']
        
        assert total_time < 60, "Pipeline execution exceeded time limit"

    def test_output_completeness(self, execution_id):
        """Test if all required output files are generated"""
        required_files = {
            'current_weather.csv': ['last_updated','temp_c','temp_f','is_day','condition_text','condition_code','name','country','region'],
            'forecast_weather.csv': ['date','date_epoch','day_maxtemp_c','day_maxtemp_f','day_mintemp_c','day_mintemp_f','day_avgtemp_c','day_avgtemp_f','day_maxwind_mph','day_maxwind_kph','day_totalprecip_mm','day_totalprecip_in','day_totalsnow_cm','day_avgvis_km','day_avgvis_miles','day_avghumidity','day_daily_will_it_rain','day_daily_chance_of_rain','day_daily_will_it_snow','day_daily_chance_of_snow','day_condition_text','day_condition_code','day_uv','name','country'],
            'location.csv': ['name','region','country','lat','lon','tz_id','localtime_epoch','localtime'],
            'aggregated_temp.csv': ['name_','region_','country_','day_avgtemp_c_max','day_avgtemp_c_mean','day_avgtemp_c_min','day_avgtemp_f_max','day_avgtemp_f_mean','day_avgtemp_f_min'],
            'max_temp.csv': ['name','region','date','day_maxtemp_c','day_maxtemp_f','day_name']
        }
        
        for file, required_columns in required_files.items():
            path = f'data/output/{execution_id}/{file}'
            assert os.path.exists(path), f"Missing output file: {file}"
            
            df = pd.read_csv(path)
            assert all(col in df.columns for col in required_columns), \
                f"Missing required columns in {file}"