import pandas as pd
import numpy as np
import env
import os


def get_connection(db, user=env.user, host=env.host, password=env.password):
    '''
    get_connection will determine the database we are wanting to access, and load the database along with env stored values like username, password, and host
    to create the url needed for SQL to read the correct database.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

def get_curr_logs_data():
    '''
    get_titanic_data will determine if 'titanic.csv' exists, if it does, it will load the dataframe titanic_db,
    if it does not exist, it will write the dataframe titanic_db into a .csv
    '''
    file_name = 'curriculum_logs2.csv'
    
    
    if os.path.isfile(file_name):
        return pd.read_csv(file_name)
    else:
        query = 'SELECT * FROM logs LEFT JOIN cohorts ON logs.cohort_id = cohorts.id'
        connection = get_connection('curriculum_logs')
        df = pd.read_sql(query, connection)
        df.to_csv(file_name, index=False)
        return df
