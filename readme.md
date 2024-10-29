
## Clone the repository
git clone https://github.com/dk46164/weather_analyzer.git


## Directory Structure
```text
weather-etl/
├── src/
│   ├── core/
│   ├── steps/
│   ├── utils/
├── config/
├── data/
│   ├── input/
│   ├── output/
│   ├── logs/
│   ├── metrics/
│   └── checkpoints/
├── tests/
└── README.md
```

## Installation

### Prerequisites

1. **Python 3.12.4**
2. **Dependencies**
   ```bash
   pip install -r requirements.txt

## Documentation
The ETL process documentation can be found at:
```text
/docs/readme.md
```

### Setting Up the Environment

**Clone the repository**:
    ```bash
   git clone <repository-url>
   cd weather_analyzer

## Run the pipeline
To run the pipeline:

```bash
python weather_etl.py 
```

## Validate the pipeline run
```bash
pytest /tests/test_weather_etl.py 
```