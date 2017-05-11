import os
import pandas as pd
import yaml
from boto.s3.connection import S3Connection
from boto.s3.key import Key
# from sqlalchemy import create_engine


# In[2]:

# get_ipython().system('pip install ipython-sql')
#
#
# # In[3]:
#
# get_ipython().magic('load_ext sql')
#
#
# # In[4]:
#
# credentials = yaml.load(open(os.path.expanduser('~/GalvanizeU/rds_creds.yml')))
#
#
# # In[5]:
#
# conn = ('postgresql://ngtantai:taido333@postgresql-i1.cby0l8sb5pld.us-west-2.rds.amazonaws.com:5432/transportation_db'.format(**credentials['rds_international_traffic']))
#
#
# # In[6]:
#
# get_ipython().magic('sql $conn')
# #conn.table_names()
#
#
# # In[7]:
#
# vehicles = get_ipython().magic('sql SELECT * FROM vehicles')
# entities = get_ipython().magic('sql SELECT * FROM entities')
# trips = get_ipython().magic('sql SELECT * FROM trips')
# positions = get_ipython().magic('sql SELECT * FROM positions')


# In[9]:

# import pandas as pd
# vehicle_df = pd.DataFrame(data = vehicles, columns=['vehicle_id','vehicle_label','trip_id',
#                                                              'current_stop_sequence', 'current_status',
#                                                              'timestamp','congestion_level','latitude','longitude'])
# entity_df = pd.DataFrame(data = entities, columns=['id','is_deleted','vehicle_id'])


# In[13]:

# #How many distinct users are in your database?
# get_ipython().magic('sql SELECT count(DISTINCT vehicle_id) FROM vehicles')
#
#
# # In[34]:
#
# get_ipython().run_cell_magic('sql', '', 'SELECT  DISTINCT(entities.id),  entities.is_deleted,\n        vehicles.vehicle_id, vehicles.trip_id, vehicles.timestamp, vehicles.latitude,vehicles.longitude, \n        trips.route_id, trips.start_date,\n        positions.bearing\nFROM entities \nLEFT JOIN vehicles ON vehicles.vehicle_id = entities.vehicle_id \nLEFT JOIN trips ON trips.trip_id = vehicles.trip_id \nLEFT JOIN positions ON positions.latitude = vehicles.latitude AND positions.longitude = vehicles.longitude LIMIT 100')


# In[62]:

def collect_data(vehicles, entities, trips, positions, cur):
    df = 'SELECT  entities.id,  entities.is_deleted,vehicles.vehicle_id, vehicles.trip_id, vehicles.timestamp, vehicles.latitude,vehicles.longitude,trips.route_id, trips.start_date,positions.bearing FROM entities LEFT JOIN vehicles ON vehicles.vehicle_id = entities.vehicle_id LEFT JOIN trips ON trips.trip_id = vehicles.trip_id LEFT JOIN positions ON positions.latitude = vehicles.latitude AND positions.longitude = vehicles.longitude'
    cur.execute(df)
    return df
def data(df):
    traffic_df = pd.DataFrame(data=df, columns=['id','is_deleted','vehicle_id','trip_id','timestamp','latitude','longitude','route_id','start_date','bearing'])
    return traffic_df


# In[29]:

# traffic_df = pd.DataFrame(data=df, columns=['id','is_deleted','vehicle_id','trip_id','timestamp','latitude','longitude','route_id','start_date','bearing'])


# In[30]:

# traffic_df


# In[67]:

# importing necessary libraries
import numpy as np
import matplotlib.pyplot as plt
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import AdaBoostRegressor, AdaBoostClassifier
from sklearn.metrics import accuracy_score
from sklearn.metrics import roc_auc_score
from sklearn.tree import DecisionTreeClassifier
from sklearn.cross_validation import train_test_split
from sklearn.model_selection import GridSearchCV

def boosting_model(traffic_df):
    rng = np.random.RandomState(2)

    y = traffic_df['route_id']
    X = traffic_df.drop('route_id', 1)
    X_train, X_test, y_train, y_test = train_test_split(X, y)
    print(y.shape, X.shape)
    # Fit regression model
    #regr_1 = DecisionTreeRegressor(max_depth=9)
    param_grid = {'max_depth': np.arange(3, 10)}
    for model in [DecisionTreeClassifier(max_depth=5)]:
        tree = GridSearchCV(model,param_grid)

        tree.fit(X_train, y_train)
        tree_preds = tree.predict(X_test)
        #print(sum(tree_preds == y_test)/y_test.shape[0])
    # tree_performance = roc_auc_score(y_test, tree_preds)

    # print ('DecisionTree: Area under the ROC curve = {}'.format(tree_performance))

    regr_2 = AdaBoostClassifier(DecisionTreeClassifier(max_depth=9),
                               n_estimators=300, random_state=rng)

    # regr_1.fit(X, y)
    regr_2.fit(X_train, y_train)

    # # Predict
    # y_1 = regr_1.predict(X)
    y_2 = regr_2.predict(X_test)

    # y_1 = [int(i) for i in y_1]
#     print(y_2[:10])
#     print(tree_preds[:10])
#     print(y[:10])
#     print("GridSearch CV accuracy: ", sum(tree_preds == y_test)/y_test.shape[0])
#     print("Adaboost Classfier accuracy:",sum(y_2 == y_test)/y_test.shape[0])

    return "GridSearch CV accuracy: " + str(sum(tree_preds == y_test)/y_test.shape[0]) + "\n" + "Adaboost Classfier accuracy:" + str(sum(y_2 == y_test)/y_test.shape[0]) + "\n"


# In[66]:

def data_to_html(traffic_df):
    webpage = str(traffic_df) + '.html'
    traffic_df.to_html(webpage)
    # use boto to upload html and image files
    conn = S3Connection()
    mybucket = conn.get_bucket('final-project-dsci6007') # Substitute in your bucket name
    # update html on s3 to include plot
    file_key = mybucket.new_key(webpage)
    file_key.content_type = 'text/html'
    file_key.set_contents_from_filename(webpage, policy='public-read')


# In[ ]:
