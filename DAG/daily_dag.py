from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from my_script import my_python_function
from zipfile import ZipFile 
import db_operations as db
import pandas as pd
import requests
import re
import os
from datetime import date, timedelta
import numpy as np


def connect():
    """Connects to postgresdatabase

    Args:
        dbname (string): name of your catalog
        user (string): name of your username
        password (password): your password

    Returns:
        [type]: [description]

    """    
    conn = psycopg2.connect(host = HOST,
    dbname = DBNAME,
    user = USER,
    password = PASS,
    port = 5432
    )
    cursor = conn.cursor()
    return conn, cursor

def close(conn, cursor):  
    #closes connection to database
    conn.commit()
    cursor.close()
    conn.close()


def update_daily():
    conn, cur = connect()
    yesterday = str(date.today()- timedelta(days=1))
    valid_oslo_stations = ['SN18701','SN18315','SN18240','SN18210','SN18270','SN18500','SN18020','SN17980','SN18269','SN18815','SN76914','SN18690','SN18410','SN18700','SN18920','SN18420','SN18950','SN18980']
    query = '''CREATE TABLE IF NOT EXISTS fe_dw.DailyTemperatures (sourceId text, timek int, datek int, value float, unit text)'''
    cur.execute(query)
    query = '''TRUNCATE TABLE fe_dw.DailyTemperatures;'''
    cur.execute(query)
    query = '''insert into fe_dw.DailyTemperatures(sourceId, timek, datek, value, unit) 
                         values (%s, %s, %s, %s, %s)'''

    for station in valid_oslo_stations:
        # ['sourceId', 'time', 'date', 'value', 'unit']
        result = get_weather_on_station(station, yesterday)
        if(result is None):
            continue
        row_list = []
        for _, row in result.iterrows():
            row_list.append((str(row['sourceId']), str(row['time']), str(row['date']), str(row['value']), str(row['unit'])))
            print((str(row['sourceId']), str(row['time']), str(row['date']), str(row['value']), str(row['unit'])))
        
        cur.executemany(query, row_list)
        conn.commit()

    query = '''insert into fe_star.facttable (bkey, weatherskey, temperatur, kdate, ktime)
                select cbws.bkey, d.sourceid ,d.value, d.datek, d.timek from 
                fe_dw.DailyTemperatures d 
                join fe_star.buss_weather_station_relation cbws using(sourceId) 
                group by cbws.bkey, sourceid, value, datek, timek
                order by count(*) asc;'''
    cur.execute(query)
    close(conn, cur)

dag = DAG('tutorial', default_args=default_args)

PythonOperator(dag=dag,
               task_id='my_task_powered_by_python',
               provide_context=False,
               python_callable=my_python_function,
               op_args=['arguments_passed_to_callable'],
               op_kwargs={'keyword_argument':'which will be passed to function'})