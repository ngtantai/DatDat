import json
from os.path import expanduser
import psycopg2
from psycopg2 import IntegrityError, InternalError
import sys
import time
import yaml

credentials = yaml.load(open(expanduser('~/GalvanizeU/rds_creds.yml')))


def get_vehicles(traffic):
    '''
    INPUT: JSON string of tweets
    OUTPUT: list of length 9 w/ the following values:
            vehicle_id
            vehicle_label
            trip_id
            currrent_stop_sequence
            current_status
            timestamp
            congestion_level
            latitude
            longitude
    '''
    try:
        vehicle = traffic.get('entity')[0]['vehicle']
        vehicle_id = vehicle['vehicle']['id']
        vehicle_label = vehicle['vehicle']['label']
        trip_id = vehicle['trip']['trip_id']
        css = vehicle['current_stop_sequence']
        cs = vehicle['current_status']
        timestamp = vehicle['timestamp']
        cl = vehicle['congestion_level']
        latitude = vehicle['position']['latitude']
        longitude = vehicle['position']['longitude']
        return [vehicle_id, vehicle_label, trip_id, css, cs, timestamp, cl, latitude, longitude]
    except ValueError:
        pass

def get_entities(traffic):
    '''
    INPUT: JSON string of traffic
    OUTPUT: list of length 3 w/ the following values:
            id
            is_deleted
            vehicle_id
    '''
    try:
        entity = traffic.get('entity')
        print("entity:", entity)
        entity_id = entity[0]['id']
        entity_is_deleted = entity[0]['is_deleted']
        entity_vehicle_id = entity[0]['vehicle']['vehicle']['id']
        return [entity_id, entity_is_deleted, entity_vehicle_id]
    except ValueError:
        pass

def get_trips(traffic):
    '''
    INPUT: JSON string of traffic
    OUTPUT: list of length 4 w/ the following values:
            trip_id
            route_id
            start_date
            schedule_relationship
    '''
    try:
        trip = traffic.get('entity')[0]['vehicle']['trip']
        trip_id = trip['trip_id']
        route_id = trip['route_id']
        start_date = trip['start_date']
        schedule_relationship = trip['schedule_relationship']
        return [trip_id, route_id, int(start_date), int(schedule_relationship)]
    except ValueError:
        pass

def get_positions(traffic):
    '''
    INPUT: JSON string of traffic
    OUTPUT: list of length 5 w/ the following values:
            latitude
            longitude
            bearing
            odometer
            speed
    '''
    try:
        position = traffic.get('entity')[0]['vehicle']['position']
        latitude = position['latitude']
        longitude = position['longitude']
        bearing = position['bearing']
        odometer = position['odometer']
        speed = position['speed']
        return [latitude, longitude, int(bearing), int(odometer), int(speed)]
    except ValueError:
        pass



def main(credentials, source=sys.stdin):
    '''
    INPUT: None
    OUTPUT: None
        Inserts all entities into postgres using `get_entities`
    For more on the errors see:
        http://initd.org/psycopg/docs/module.html#exceptions
    '''
    conn = psycopg2.connect(**credentials)
    cur = conn.cursor()
    total_count = 0
    row_count = 0
    exception_count = 0
    for traffic_str in source:
        if '"entity": [{' and '"is_deleted":' and '"schedule_relationship":' in traffic_str:
            total_count += 1
            row_count +=1
            single_traffic = json.loads(traffic_str)
            entity_row, trip_row, vehicle_row, position_row = get_entities(single_traffic), \
                                                            get_trips(single_traffic), \
                                                            get_vehicles(single_traffic), \
                                                            get_positions(single_traffic)
            # print(entity_row)
            # print(trip_row)
            # print()
            #print(position_row)

            try:
                cur.execute("INSERT INTO vehicles VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",vehicle_row)
                conn.commit()
                cur.execute("INSERT INTO entities VALUES (%s,%s,%s)",entity_row)
                conn.commit()
                cur.execute("INSERT INTO trips VALUES (%s,%s,%s,%s)",trip_row)
                conn.commit()
                cur.execute("INSERT INTO positions VALUES (%s,%s,%s,%s,%s)",position_row)
                conn.commit()

            except (IntegrityError, InternalError) as e:  # prevents duplicates
                exception_count +=1
                cur.execute("rollback")
                print('Entity # {}: {}'.format(exception_count, vehicle_row))

    conn.commit()
    conn.close()
    print('Inserted {} tweets'.format(total_count))

if __name__ == '__main__':
    main(credentials=credentials['rds_international_traffic'])
