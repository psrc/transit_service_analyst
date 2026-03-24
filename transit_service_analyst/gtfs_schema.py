"""
GTFS Schema Validation Module

This module provides Pandera data validation schemas for GTFS (General Transit Feed Specification) data.
Each schema class validates the structure and data types of GTFS files according to the specification.
"""

from numpy import float64
from pandera.typing import Series
import pandera as pa


class GTFS_Schema(object):
    """
    Container class for GTFS data validation schemas.
    
    This class contains nested DataFrameModel classes that validate GTFS files
    according to the General Transit Feed Specification. Each nested class
    corresponds to a GTFS file and defines the expected columns and data types.
    """
    class Agency(pa.DataFrameModel):
        """Schema for agency.txt - Transit agencies with service represented in this dataset."""
        agency_name: Series[str] = pa.Field(coerce=True)
        agency_url: Series[str] = pa.Field(coerce=True)
        agency_timezone: Series[str] = pa.Field(coerce=True)

    class Stops(pa.DataFrameModel):
        """Schema for stops.txt - Stops where vehicles pick up or drop off passengers."""
        stop_id: Series[str] = pa.Field(coerce=True)
        stop_lat: Series[float64] = pa.Field(coerce=True, nullable=True)
        stop_lon: Series[float64] = pa.Field(coerce=True, nullable=True)

    class Routes(pa.DataFrameModel):
        """Schema for routes.txt - Transit routes (lines) in the GTFS feed."""
        route_id: Series[str] = pa.Field(coerce=True)
        route_type: Series[int] = pa.Field(isin=[0, 1, 2, 3, 4, 5, 6, 7, 11, 12])

    class Trips(pa.DataFrameModel):
        """Schema for trips.txt - Trips for each route."""
        route_id: Series[str] = pa.Field(coerce=True)
        service_id: Series[str] = pa.Field(coerce=True)
        trip_id: Series[str] = pa.Field(coerce=True)
        shape_id: Series[str] = pa.Field(coerce=True, nullable=True)

    class Stop_Times(pa.DataFrameModel):
        """Schema for stop_times.txt - Times that a vehicle arrives at and departs from stops."""
        trip_id: Series[str] = pa.Field(coerce=True)
        arrival_time: Series[str] = pa.Field(coerce=True, nullable=True)
        departure_time: Series[str] = pa.Field(coerce=True, nullable=True)
        stop_id: Series[str] = pa.Field(coerce=True)
        stop_sequence: Series[int] = pa.Field(coerce=True)

    class Calendar(pa.DataFrameModel):
        """Schema for calendar.txt - Service dates specified using a weekly schedule."""
        service_id: Series[str] = pa.Field(coerce=True)
        monday: Series[int] = pa.Field(isin=[0, 1])
        tuesday: Series[int] = pa.Field(isin=[0, 1])
        wednesday: Series[int] = pa.Field(isin=[0, 1])
        thursday: Series[int] = pa.Field(isin=[0, 1])
        friday: Series[int] = pa.Field(isin=[0, 1])
        saturday: Series[int] = pa.Field(isin=[0, 1])
        sunday: Series[int] = pa.Field(isin=[0, 1])

    class Calendar_Dates(pa.DataFrameModel):
        """Schema for calendar_dates.txt - Exceptions for the services defined in calendar.txt."""
        service_id: Series[str] = pa.Field(coerce=True)
        date: Series[int] = pa.Field(coerce=True)
        exception_type: Series[int] = pa.Field(coerce=True, isin=[1, 2])

    class Shapes(pa.DataFrameModel):
        """Schema for shapes.txt - Rules for mapping vehicle travel paths."""
        shape_id: Series[str] = pa.Field(coerce=True)
        shape_pt_lat: Series[float64] = pa.Field(coerce=True)
        shape_pt_lon: Series[float64] = pa.Field(coerce=True)
        shape_pt_sequence: Series[int] = pa.Field(coerce=True)

    # Convenience attributes for accessing column lists
    trips_columns = list(Trips.__annotations__.keys())  #: List of column names for trips.txt
    calendar_dates_columns = list(Calendar_Dates.__annotations__.keys())  #: List of column names for calendar_dates.txt  
    shapes_columns = list(Shapes.__annotations__.keys())  #: List of column names for shapes.txt
