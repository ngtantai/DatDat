# from queries import collect_data, data, boosting_model, data_to_htmlhtml
#import queries
import numpy as np
import matplotlib.pyplot as plt
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import AdaBoostRegressor, AdaBoostClassifier
from sklearn.metrics import accuracy_score
from sklearn.metrics import roc_auc_score
from sklearn.tree import DecisionTreeClassifier
from sklearn.cross_validation import train_test_split
from sklearn.model_selection import GridSearchCV

from boto.s3.connection import S3Connection
from boto.s3.key import Key
import pandas as pd

import json
from os.path import expanduser
import psycopg2
from psycopg2 import IntegrityError, InternalError
import sys
import time
import yaml
import codecs


def main():
    credentials = yaml.load(open(expanduser('~/GalvanizeU/rds_creds.yml')))
    credentials = credentials['rds_international_traffic']
    conn = psycopg2.connect(**credentials)
    cur = conn.cursor()
    vehicles = "SELECT * FROM vehicles"
    entities = "SELECT * FROM entities"
    trips =  "SELECT * FROM trips"
    positions = "SELECT * FROM positions"
    traffic_df = data(collect_data(vehicles,entities,trips,positions,cur))
    showcase_html = data_to_html(traffic_df,'showcase5.html')

    # use boto to upload html and image filess
    conn1 = S3Connection()
    mybucket = conn1.get_bucket('final-project-dsci6007') # Substitute in your bucket name)

    # html_str = '<h1 align="center", text="bold">REPORT</h1><h3 align="left", text="bold"> Data: International Tranportation </h3><h4> Sample data </h4> <img src="architecture_overview.png">'
    # html_file= open("showcase3.html","a")
    # html_file.write(html_str)
    # html_file.close()

    connect_s3(mybucket, 'showcase5.html')

def collect_data(vehicles, entities, trips, positions, cur):
    df = 'SELECT  entities.id,  entities.is_deleted,vehicles.vehicle_id, vehicles.trip_id, vehicles.timestamp, vehicles.latitude,vehicles.longitude,trips.route_id, trips.start_date,positions.bearing FROM entities LEFT JOIN vehicles ON vehicles.vehicle_id = entities.vehicle_id LEFT JOIN trips ON trips.trip_id = vehicles.trip_id LEFT JOIN positions ON positions.latitude = vehicles.latitude AND positions.longitude = vehicles.longitude'
    cur.execute(df)
    return cur.fetchall()

def data(df):
    traffic_df = pd.DataFrame(data=df, columns=['id','is_deleted','vehicle_id','trip_id','timestamp','latitude','longitude','route_id','start_date','bearing'])
    return traffic_df

def connect_s3(mybucket, name):

    # update html on s3 to include plot
    file_key = mybucket.new_key(name)
    file_key.content_type = 'text/html'
    file_key.set_contents_from_filename(name, policy='public-read')

def data_to_html(traffic_df, name):
    webpage = str(traffic_df) + '.html'
    htmlpage = traffic_df[:10].to_html(name)
    return htmlpage



if __name__ == '__main__':
    main()
