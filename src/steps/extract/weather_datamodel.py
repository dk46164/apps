"""
weather_datamodel.py

Main weather data models for representing weather information using Pydantic
These models are designed to parse and validate weather responses
"""


from pydantic import BaseModel, Field
from typing import List, Dict, Optional

class Condition(BaseModel):
    """
    Represents weather condition information
    text: Description of the weather condition (e.g., "Partly cloudy")
    code: Numeric code identifying the weather condition
    """
    text: str
    code: int

class DayWeather(BaseModel):
    """
    Contains detailed weather metrics for a single day
    Includes temperature, wind, precipitation, visibility and other weather parameters
    All measurements are provided in both metric and imperial units
    """
    maxtemp_c: float
    maxtemp_f: float  
    mintemp_c: float
    mintemp_f: float
    avgtemp_c: float
    avgtemp_f: float
    maxwind_mph: float 
    maxwind_kph: float
    totalprecip_mm: float
    totalprecip_in: float
    totalsnow_cm: float
    avgvis_km: float
    avgvis_miles: float
    avghumidity: int
    daily_will_it_rain: int  # Binary indicator (0/1) for rain prediction
    daily_chance_of_rain: int  # Percentage chance of rain
    daily_will_it_snow: int  # Binary indicator (0/1) for snow prediction  
    daily_chance_of_snow: int  # Percentage chance of snow
    condition: Condition
    uv: float  # UV index

class ForecastDay(BaseModel):
    """
    Represents weather forecast for a specific date
    Combines date information with detailed day weather metrics
    """
    date: str  # Date in YYYY-MM-DD format
    date_epoch: int  # Unix timestamp
    day: DayWeather

class Location(BaseModel):
    """
    Contains geographical and timezone information for a weather location
    Includes coordinates, timezone, and location names
    """
    name: str  # City/location name
    region: str
    country: str
    lat: float  # Latitude
    lon: float  # Longitude
    tz_id: str  # Timezone identifier
    localtime_epoch: int  # Local time as Unix timestamp
    localtime: str  # Local time in readable format

class CurrentWeather(BaseModel):
    """
    Represents current weather conditions
    Includes temperature and current weather state
    """
    last_updated: str
    temp_c: Optional[float]  # Temperature in Celsius, optional
    temp_f: float  # Temperature in Fahrenheit
    is_day: int  # Binary indicator for daytime (1) or nighttime (0)
    condition: Condition

class CityWeather(BaseModel):
    """
    Main weather model that combines location, current conditions and forecast
    Provides a complete weather information package for a city/location
    """
    location: Location
    current: CurrentWeather
    forecast: Dict[str, List[ForecastDay]]  # Key-value pairs of date and forecast
