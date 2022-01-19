import os
import pandas as pd
from numpy import NaN
from datetime import datetime
import geopandas as gpd
from shapely.geometry import LineString
from .gtfs_schema import GTFS_Schema


class Service_Utils(object):
    """
    Documentation forthcoming.
    """

    #: File with routes.
    #: TODO document format

    def __init__(self, gtfs_dir, service_date, start_time, end_time):
        """
        Add docstring.
        """
        self.gtfs_dir = gtfs_dir
        self.service_date = service_date
        self.int_service_date = int(service_date)
        self.start_time = start_time
        self.end_time = end_time
        self._crs_epsg = 4326

        # gtfs properties:
        self.calendar_dates = self.__get_calendar_dates()
        self.calendar = self.__get_calendar()
        self.service_ids = self.__get_service_ids()
        self.trips = self.__get_trips()
        self.stop_times = self.__get_stop_times()
        # self._df_all_stops_by_trips = self.__get_trips_stop_times()
        self.routes = self.__get_routes()
        self.stop_list = self.stop_times['stop_id'].unique()
        self.stops = self.__get_stops()
        self.shapes = self.__get_shapes()

        # derived DataFrames
        self._df_all_stops_by_trips = self.__get_trips_stop_times()
        self._schedule_pattern_dict = self.__get_schedule_pattern()
        self.schedule_pattern_df = self.__get_schedule_pattern_df()
        self._df_all_stops_by_trips = self._df_all_stops_by_trips.merge(
            self.schedule_pattern_df[['orig_trip_id', 'rep_trip_id']],
            how='left', left_on='trip_id',
            right_on='orig_trip_id').drop(columns=['orig_trip_id'])
        self.__trip_list = self.__get_trip_list()
        self.__rep_trip_list = list(
            self.schedule_pattern_df.rep_trip_id.unique())

    def __get_calendar(self):
        calendar_df = pd.read_csv(os.path.join(self.gtfs_dir, 'calendar.txt'))
        return GTFS_Schema.Calendar.validate(calendar_df)

    def __get_calendar_dates(self):
        if os.path.exists(os.path.join(
                self.gtfs_dir, 'calendar_dates.txt')) is False:
            calendar_dates = pd.DataFrame(
                    columns=GTFS_Schema.calendar_dates_columns)
        else:
            calendar_dates = pd.read_csv(
                os.path.join(self.gtfs_dir, 'calendar_dates.txt'))

        return GTFS_Schema.Calendar_Dates.validate(calendar_dates)

    def __get_trips(self):
        trips_df = pd.read_csv(os.path.join(self.gtfs_dir, 'trips.txt'))
        trips_df = GTFS_Schema.Trips.validate(trips_df)
        trips_df = trips_df[trips_df['service_id'].isin(self.service_ids)]

        return trips_df

    def __get_routes(self) -> pd.DataFrame:
        routes_df = pd.read_csv(os.path.join(self.gtfs_dir, 'routes.txt'))
        routes_df = GTFS_Schema.Routes.validate(routes_df)
        routes_df = routes_df[routes_df['route_id'].isin(
            self.trips['route_id'])]
        return routes_df

    def __get_stop_times(self):
        stop_times_df = pd.read_csv(os.path.join(
            self.gtfs_dir, 'stop_times.txt'))
        stop_times_df = GTFS_Schema.Stop_Times.validate(stop_times_df)
        stop_times_df = stop_times_df[stop_times_df['trip_id'].isin(
            self.trips['trip_id'])]
        return stop_times_df

    def __get_stops(self):
        stops_gdf = pd.read_csv(os.path.join(self.gtfs_dir, 'stops.txt'))
        stops_gdf = GTFS_Schema.Stops.validate(stops_gdf)
        stops_gdf = stops_gdf[stops_gdf['stop_id'].isin(self.stop_list)]
        stops_gdf = gpd.GeoDataFrame(stops_gdf, geometry=gpd.points_from_xy(
            stops_gdf['stop_lon'], stops_gdf['stop_lat']))
        stops_gdf = stops_gdf.set_crs(epsg=self._crs_epsg)
        return stops_gdf

    def __get_shapes(self):
        gdf = pd.read_csv(os.path.join(self.gtfs_dir, 'shapes.txt'), dtype={
            'shape_id': str})
        gdf = GTFS_Schema.Shapes.validate(gdf)
        gdf = gdf[gdf['shape_id'].isin(self.trips['shape_id'])]
        gdf = gpd.GeoDataFrame(gdf, geometry=gpd.points_from_xy(
            gdf['shape_pt_lon'], gdf['shape_pt_lat']))
        gdf = gpd.GeoDataFrame(gdf.groupby('shape_id')['geometry'].apply(
            lambda x: LineString(x.tolist())))
        gdf.reset_index(inplace=True)
        gdf = gdf.set_crs(epsg=self._crs_epsg)
        return gdf

    def __get_service_ids(self):
        """
        Returns a list of valid service_id(s) from each feed
        using the user specified service_date.
        """
        my_date = datetime(int(self.service_date[0:4]), int(
            self.service_date[4:6]), int(self.service_date[6:8]))
        day_of_week = self.__get_weekday(my_date)

        regular_service_dates = self.calendar[(
            self.calendar['start_date'] <= self.int_service_date) & (
                self.calendar['end_date'] >= self.int_service_date) & (
                    self.calendar[day_of_week] == 1)]['service_id'].tolist()

        exceptions_df = self.calendar_dates[
            self.calendar_dates['date'] == self.int_service_date]

        add_service = exceptions_df.loc[
            exceptions_df['exception_type'] == 1]['service_id'].tolist()

        remove_service = exceptions_df[
            exceptions_df['exception_type'] == 2]['service_id'].tolist()

        service_id_list = [x for x in (
            add_service + regular_service_dates) if x not in remove_service]

        assert service_id_list, "No service found in feed."
        return service_id_list

    def __get_weekday(self, my_date):
        """
        Gets the day of week from user parameter
        service date.
        """
        week_days = ['monday', 'tuesday', 'wednesday',
                     'thursday', 'friday', 'saturday', 'sunday']
        return week_days[my_date.weekday()]

    def __make_sequence_col(
            self, data_frame, sort_list, group_by_col, seq_col):

        """
        Sorts a pandas dataframe using sort_list, then creates a column of
        sequential integers (1,2,3, etc.) for groups, using the group_by_col.
        Then drops the existing sequence column and re-names the new sequence
        column.
        """
        # sort on tripId, sequence
        data_frame = data_frame.sort_values(sort_list, ascending=[1, 1])
        # create a new field, set = to the position of each record in a group,
        # grouped by tripId
        data_frame['temp'] = data_frame.groupby(group_by_col).cumcount() + 1
        # drop the old sequence column
        data_frame = data_frame.drop(seq_col, axis=1)
        # rename new column:
        data_frame = data_frame.rename(columns={'temp': seq_col})

        return data_frame

    def __get_trip_list(self):
        trips = self._df_all_stops_by_trips.trip_id.tolist()
        trips = list(set(trips))
        return trips

    def __get_trips_stop_times(self):
        """
        Creates a merged dataframe consisting of trips & stop_ids for the
        start time, end time and service_id (from GTFS Calender.txt). This
        can include partial itineraries as only stops within the start and
        end time are included.
        """
        stop_times_df = self.__make_sequence_col(
            self.stop_times, ['trip_id', 'stop_sequence'],
            'trip_id', 'stop_sequence')

        stop_times_df = stop_times_df[stop_times_df['trip_id'].isin(
            self.trips['trip_id'])]

        # Add columns for arrival/departure in decimal minutes and hours:
        # Some schedules only have arrival/departure times for time points,
        # not all stops:
        if stop_times_df['departure_time'].isnull().any():
            stop_times_df['departure_time'].fillna('00:00:00', inplace=True)
            stop_times_df['departure_time_mins'] = stop_times_df.apply(
                self.__convert_to_decimal_minutes, axis=1, args=(
                    'departure_time',))
            stop_times_df['departure_time_mins'].replace(0, NaN, inplace=True)
            stop_times_df['departure_time_mins'].interpolate(inplace=True)
        else:
            stop_times_df['departure_time_mins'] = stop_times_df.apply(
                self.__convert_to_decimal_minutes, axis=1, args=(
                    'departure_time',))
        stop_times_df['departure_time_hrs'] = stop_times_df[
            'departure_time_mins']/60
        stop_times_df['departure_time_hrs'] = stop_times_df[
            'departure_time_hrs'].astype(int)
        # Get all trips for this time window
        # trips_tw_df = stop_times_df.loc[(
        #     stop_times_df.departure_time_mins >= self.start_time) & (
        #         stop_times_df.departure_time_mins < self.end_time)]
        # Only need the trip_id column
        # trips_tw_df = pd.DataFrame(trips_tw_df.trip_id)
        # trips_tw_df = trips_tw_df.drop_duplicates('trip_id')

        # Merge trips on trip_id, so we can have shape_id. Use inner so we get
        # trips for time window and service_id
        stop_times_df = stop_times_df.merge(
            self.trips, 'left', left_on=["trip_id"], right_on=["trip_id"])

        # Get all the stops for the trips that operate in this window, even if
        # some of the stops occur outside the window
        # trips_stop_times_df = stop_times_df.merge(
        #     trips_tw_df, 'right', left_on=["trip_id"],
        #     right_on=["trip_id"])
        # trips_stop_times_df = trips_stop_times_df.sort_values(
        #     ['trip_id', 'stop_sequence'])
        return stop_times_df

    def __convert_to_decimal_minutes(self, row, field):
        """
        Convert HH:MM:SS to seconds since midnight
        for comparison purposes.
        """

        H, M, S = row[field].split(':')
        seconds = float(((float(H) * 3600) + (float(M) * 60) + float(S))/60)

        return seconds

    def __convert_to_seconds(self, row, field):
        h, m, s = row[field].split(':')
        return int(h) * 3600 + int(m) * 60 + int(s)

    def __get_schedule_pattern(self, route_field='route_id'):
        """
        Returns a nested diciontary where the first level key is route_id and
        values are respresentative trip_ids that have unique stop sequences.
        These are are used as keys for the second level where each value is a
        dictinary that includes a list of trips_id's that share this stop
        pattern and a list of ordered stops.

        {route_id : trip_id {trips_ids : [list of trip ids], stops :
        [list of stops]}}

        """

        # Create a dictionary where the key is a tuple of trip_id, route_id and
        # the value is a list of stop sequences for the trip Need to use all
        # the stops for the trips within the time window, even if those stops
        # fall outside the time winwdow because we are interested in unique
        # stop sequences

        stop_sequence_dict = {k: list(v) for k, v in
                              self._df_all_stops_by_trips.groupby(
                ['trip_id', route_field])['stop_id']}

        # Empty dictionary to store unique stop sequences
        my_dict = {}
        for key, value in stop_sequence_dict.items():
            trip_id = key[0]
            route_id = key[1]
            # Handle some branching later on
            found = False
            # If this is the first trip for this route, just add it
            if route_id not in my_dict.keys():
                my_dict[route_id] = {trip_id: {
                    'stops': value, 'trip_ids': [trip_id]}}
            # Otherwise check to see if this stop sequence has already been
            # added for this route
            else:
                for k, v in my_dict[route_id].items():
                    if value == v['stops']:
                        # This stop sequence has already been added for this
                        # route, add the trip_id to the list of trip_ids that
                        # have this sequence in common.
                        my_dict[route_id][k]['trip_ids'].append(trip_id)
                        found = True
                        break
                if not found:
                    # Add the stop sequence and route, trip info
                    my_dict[route_id][trip_id] = {
                        'stops': value, 'trip_ids': [trip_id]}
            # Set back to False for next iteration
            found = False
        return my_dict

    def __get_schedule_pattern_df(self):
        rows = []
        for route_id, trips in self._schedule_pattern_dict.items():
            for trip_id, data in trips.items():
                for trip in data['trip_ids']:
                    rows.append(
                        {'route_id': route_id, 'trip_id1': trip_id,
                            'trip_id2': trip})
        df2 = pd.DataFrame(rows)
        df = self._df_all_stops_by_trips.drop_duplicates(['trip_id'])
        df2 = df2.merge(df[['trip_id', 'shape_id']], how='right',
                        left_on=["trip_id2"], right_on=["trip_id"])
        df2 = df2.rename(
            columns={'trip_id1': 'rep_trip_id', 'trip_id': 'orig_trip_id'})
        df2 = df2.drop('trip_id2', 1)
        return df2

    def get_tph_by_line(self):
        # get the first stop for every trip
        first_departure = self._df_all_stops_by_trips.sort_values(
            'stop_sequence', ascending=True).groupby(
                'trip_id', as_index=False).first()
        # this may not be necessary
        first_departure = first_departure.loc[(
            first_departure.stop_sequence == 1)]
        first_departure = first_departure.groupby(
            ['rep_trip_id', 'departure_time_hrs']
            )['departure_time_hrs'].count()
        first_departure_df = pd.DataFrame(first_departure)
        first_departure_df.reset_index(level=0, inplace=True)
        first_departure_df = first_departure_df.rename(
            columns={'departure_time_hrs': 'frequency'})
        first_departure_df.reset_index(level=0, inplace=True)
        t = pd.pivot_table(
            first_departure_df, values='frequency', index=['rep_trip_id'],
            columns=['departure_time_hrs'])
        t = t.fillna(0)
        for col in t.columns:
            if not col == 'rep_trip_id':
                t = t.rename(columns={col: 'hour_' + str(col)})
        t.reset_index(inplace=True)
        t = t.merge(self.trips[['route_id', 'trip_id',
                    'direction_id']], how='left', left_on='rep_trip_id',
                    right_on='trip_id')

        return t

    def get_tph_at_stops(self):
        df = self._df_all_stops_by_trips.groupby(
            ['stop_id', 'departure_time_hrs'])['departure_time_hrs'].count()

        df = pd.DataFrame(df)
        df.reset_index(level=0, inplace=True)
        df = df.rename(columns={'departure_time_hrs': 'frequency'})
        df.reset_index(level=0, inplace=True)
        t = pd.pivot_table(df, values='frequency', index=['stop_id'], columns=[
            'departure_time_hrs'])
        t = t.fillna(0)
        for col in t.columns:
            if not col == 'rep_trip_id':
                t = t.rename(columns={col: 'hour_' + str(col)})
        t.reset_index(inplace=True)
        return t

    def get_lines_gdf(self):
        rep_trips = self.trips[self.trips['trip_id'].isin(
            self.__rep_trip_list)]
        rep_trips = rep_trips.merge(self.routes, how='left', on='route_id')
        rep_trips = self.shapes.merge(rep_trips, how='right', on='shape_id')
        rep_trips.rename(columns={'trip_id': 'rep_trip_id'}, inplace=True)
        assert rep_trips.geometry.hasnans == False
        return rep_trips

    def get_line_stops_gdf(self):
        route_stops = self._df_all_stops_by_trips[
            self._df_all_stops_by_trips['trip_id'].isin(self.__rep_trip_list)]
        route_stops = route_stops.merge(self.stops, how='left', on='stop_id')
        route_stops = gpd.GeoDataFrame(
            route_stops, geometry=route_stops['geometry'])
        route_stops.drop(
            columns=['shape_id', 'arrival_time', 'departure_time',
                     'departure_time_mins', 'departure_time_hrs',
                     'block_id'], axis=1, inplace=True)
        route_stops = route_stops.set_crs(epsg=self._crs_epsg)
        return route_stops

    def get_line_time(self):
        first = self._df_all_stops_by_trips.groupby(
            ['trip_id'])['departure_time_mins'].first()
        first.rename('first', inplace=True)

        last = self._df_all_stops_by_trips.groupby(
            ['trip_id'])['departure_time_mins'].last()
        last.rename('last', inplace=True)

        route_id = self._df_all_stops_by_trips.groupby(
            ['trip_id'])['rep_trip_id', 'route_id'].first()

        df = pd.concat([route_id, first, last], axis=1).reset_index()
        df['total_line_time'] = df['last'] - df['first']
        return df

    def get_service_hours_by_line(self):
        df = self.get_line_time()
        df = df.groupby('rep_trip_id')['total_line_time'].sum().to_frame()
        df.reset_index(inplace=True)
        df = df.merge(self.trips[['route_id', 'trip_id', 'direction_id']],
                      how='left', left_on='rep_trip_id', right_on='trip_id')

        return df

    def get_routes_by_stops(self):
        df = self._df_all_stops_by_trips[self._df_all_stops_by_trips[
            'trip_id'] == self._df_all_stops_by_trips['rep_trip_id']]
        df = pd.DataFrame(df.groupby('stop_id')['route_id'].apply(
            lambda x: list(set(x.tolist()))))
        df.reset_index(inplace=True)
        return df

    def get_total_trips_by_line(self):
        df = self.schedule_pattern_df.groupby('rep_trip_id').count(
                 ).reset_index()[['rep_trip_id', 'orig_trip_id']]
        df = df.rename(columns={'orig_trip_id': 'total_trips'})
        df = df.merge(self.trips, how='left', left_on='rep_trip_id',
                      right_on='trip_id')
        return df[['rep_trip_id', 'route_id', 'direction_id', 'total_trips']]
