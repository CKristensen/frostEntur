import psycopg2
from psycopg2 import sql
import getpass 
import os

PASS = os.environ['GOOLIT_AWS_PASS']
USER = os.environ['AWS_USERNAME']
DBNAME = 'postgres'
HOST = os.environ['GOOLIT_AWS_DB']


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

