from numpy import float64
from pandera.typing import Series
import pandera as pa


class GTFS_Schema(object):
    class Agency(pa.SchemaModel):
        agency_name: Series[str] = pa.Field(coerce=True)
        agency_url: Series[str] = pa.Field(coerce=True)
        agency_timezone: Series[str] = pa.Field(coerce=True)

    class Stops(pa.SchemaModel):
        stop_id: Series[str] = pa.Field(coerce=True)
        stop_lat: Series[float64] = pa.Field(coerce=True, nullable=True)
        stop_lon: Series[float64] = pa.Field(coerce=True, nullable=True)

    class Routes(pa.SchemaModel):
        route_id: Series[str] = pa.Field(coerce=True)
        route_type: Series[int] = pa.Field(isin=[0, 1, 2, 3, 4, 5, 6, 7, 11, 12])

    class Trips(pa.SchemaModel):
        route_id: Series[str] = pa.Field(coerce=True)
        service_id: Series[str] = pa.Field(coerce=True)
        trip_id: Series[str] = pa.Field(coerce=True)
        shape_id: Series[str] = pa.Field(coerce=True, nullable=True)

    class Stop_Times(pa.SchemaModel):
        trip_id: Series[str] = pa.Field(coerce=True)
        arrival_time: Series[str] = pa.Field(coerce=True, nullable=True)
        departure_time: Series[str] = pa.Field(coerce=True, nullable=True)
        stop_id: Series[str] = pa.Field(coerce=True)
        stop_sequence: Series[int] = pa.Field(coerce=True)

    class Calendar(pa.SchemaModel):
        service_id: Series[str] = pa.Field(coerce=True)
        monday: Series[int] = pa.Field(isin=[0, 1])
        tuesday: Series[int] = pa.Field(isin=[0, 1])
        wednesday: Series[int] = pa.Field(isin=[0, 1])
        thursday: Series[int] = pa.Field(isin=[0, 1])
        friday: Series[int] = pa.Field(isin=[0, 1])
        saturday: Series[int] = pa.Field(isin=[0, 1])
        sunday: Series[int] = pa.Field(isin=[0, 1])

    class Calendar_Dates(pa.SchemaModel):
        service_id: Series[str] = pa.Field(coerce=True)
        date: Series[str] = pa.Field(coerce=True)
        exception_type: Series[int] = pa.Field(coerce=True, isin=[1, 2])

    class Shapes(pa.SchemaModel):
        shape_id: Series[str] = pa.Field(coerce=True)
        shape_pt_lat: Series[float64] = pa.Field(coerce=True)
        shape_pt_lon: Series[float64] = pa.Field(coerce=True)
        shape_pt_sequence: Series[int] = pa.Field(coerce=True)

    trips_columns = list(Trips.__annotations__.keys())
    calendar_dates_columns = list(Calendar_Dates.__annotations__.keys())
    shapes_columns = list(Shapes.__annotations__.keys())
