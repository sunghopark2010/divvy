import psycopg2
import os
from decimal import Decimal
from pprint import pprint
import os
from multiprocessing import Process, Pool


class Uploader(object):
    def __init__(self, host, port, dbname, user, password):
        self.conn_string = "host='%s' port=%d dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password)
        self.conn = psycopg2.connect(self.conn_string)
        self.cursor = self.conn.cursor()
        self.successes = list()
        self.errors = list()
        self._station_upload_query = "insert into stations (station_id, name, latitude, longitude, capacity, start_date)" \
                                     " values (%d, '%s', %10.8f, %10.8f, %d, '%s'::date);"
        self._trip_upload_query = "insert into trips (trip_id, start_time, end_time, bike_id, " \
                                  "from_station_id, to_station_id, user_type_cd, gender_cd, birthyear) " \
                                  "values (%d, '%s'::timestamp without time zone, " \
                                  "'%s'::timestamp without time zone, %d, %d, %d, '%s', %s, %s)"

    def upload_station(self, station):
        columns = station.split(',')
        self._execute_query(self._station_upload_query % (int(columns[0]), columns[1], Decimal(columns[2]), Decimal(columns[3]), int(columns[4]), columns[6].split('\r')[0]))

    def upload_trip(self, trip):
        columns = trip.split(',')
        gender_cd = "'%s'" % columns[10].lower() if columns[10] else 'null'
        birthyear = columns[11] if columns[11] is not None and columns[11] != '\r\n' else 'null'
        self._execute_query(self._trip_upload_query % (int(columns[0]), columns[1], columns[2], int(columns[3]), int(columns[5]), int(columns[7]), columns[9].lower(), gender_cd, birthyear))

    def update_trip_duration(self, trip):
        columns = trip.split(',')
        self._execute_query('update trips set duration=%d where trip_id=%d;' % (int(columns[4]), int(columns[0])))

    def get_successes(self):
        return self.successes

    def get_errors(self):
        return self.errors

    def clean_successes(self):
        self.successes = list()

    def clean_errors(self):
        self.errors = list()

    def _execute_query(self, query):
        try:
            self.cursor.execute(query)
            self.successes.append(query)
        except Exception as e:
            print query
            print e.message
            self.errors.append({'error': e.message, 'query': query})

    def commit(self):
        self.conn.commit()

    def __del__(self):
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    # connect to db
    results = dict()
    uploader = Uploader(host='sunghopark.info', port=5432, dbname='divvy', user=os.environ['DB_USER'], password=os.environ['DB_PASSWORD'])
    '''
    # upload station data
    for index, station in enumerate(open('../data/Divvy_Stations_2013.csv').readlines()[1:]):
        uploader.upload_station(station)
        if index % 500 == 0:
            pprint('index: %d' % index)
            uploader.commit()
    pprint(len(uploader.get_successes()))
    pprint(uploader.get_errors())

    uploader.clean_successes()
    uploader.clean_errors()

    # upload trip data
    for index, trip in enumerate(open('../data/divvy_stations.csv').readlines()[759504:]):
        uploader.upload_trip(trip)
        if len(uploader.get_errors()) >= 10:
            break
        if index % 500 == 0:
            pprint('index: %d' % index)
            uploader.commit()
    uploader.commit()
    pprint(len(uploader.get_successes()))
    pprint(uploader.get_errors())
    '''

    # upload trip duration
    for index, trip in enumerate(open('../data/divvy_trips.csv').readlines()[1:]):
        uploader.update_trip_duration(trip)
        if len(uploader.get_errors()) >= 10:
            break
        if index % 500 == 0:
            pprint('index: %d' % index)
            uploader.commit()
    uploader.commit()
    pprint(len(uploader.get_successes()))
    pprint(uploader.get_errors())

    # uploading bike distance data is done using Excel
    # refer insert_distances.sql in the sql_queries folder
    # distances are bike distances in miles