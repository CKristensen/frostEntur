from zipfile import ZipFile 
import db_operations as db
import pandas as pd
import requests
import re
import os
from datetime import date, timedelta

# Insert your own client ID here
CLIENT_ID = os.environ['FROST_API_CLIENT_ID']


def get_weather_station_latlon():
    """Goes into FrostAPI and gets all the stations unique SN 
    numbers in Oslo, their names and their coordinates.
    returns: pandas Dataframe with columns 'id', 'name', 'lat', 'lon': 
    """    
    # Define endpoint and parameters
    endpoint = 'https://frost.met.no/sources/v0.jsonld'
    parameters = {
        'county': 'Oslo'
    }
    # Issue an HTTP GET request
    r = requests.get(endpoint, parameters, auth=(CLIENT_ID,''))
    # Extract JSON data
    json = r.json()
    
    # Check if the request worked, print out any errors
    if r.status_code == 200:
        data = json['data']
        print('Data retrieved from frost.met.no!')
    else:
        print('Error! Returned status code %s' % r.status_code)
        print('Message: %s' % json['error']['message'])
        print('Reason: %s' % json['error']['reason'])
        return None

    df = pd.DataFrame(columns=["id", "name", "geometry"])

    for row in data:
        df.loc[len(df)] = [row['id'], row['name'], row["geometry"]]

    #extract the latitude and longitude from the geometry column
    lat = lambda x: re.findall(r'(\d+\.\d+)', str(x))[1]
    lon = lambda x: re.findall(r'(\d+\.\d+)', str(x))[0]
    df['lat'] = df['geometry'].apply(lat)
    df['lon']= df['geometry'].apply(lon)

    return df[['id', 'name', 'lat', 'lon']]

def get_weather_on_station(sourceId, start_date):
    """Get the weather on a particular station
    Args:
        sourceId (str): sourceId for a weather station in FrostAPI
    Returns:
        Pandas Dataframe: columns = sourceID, referenceTime, elementId, value, unit
    """    
    today = str(date.today())
    endpoint = 'https://frost.met.no/observations/v0.jsonld'
    parameters = {
        'sources': f'{sourceId},', 
        'elements': 'air_temperature,',
        'referencetime': f'{start_date}/{today}',
    }
    # Issue an HTTP GET request
    r = requests.get(endpoint, parameters, auth=(CLIENT_ID,''))
    # Extract JSON data
    json = r.json()
    # Check if the request worked, print out any errors
    if r.status_code == 200:
        data = json['data']
        print('Data retrieved from frost.met.no!')
    else:
        print('Error! Returned status code %s' % r.status_code)
        print('Message: %s' % json['error']['message'])
        print('Reason: %s' % json['error']['reason'])
        return None

    df = pd.DataFrame()
    for i in range(len(data)):
        row = pd.DataFrame(data[i]['observations'])
        row['referenceTime'] = data[i]['referenceTime']
        row['sourceId'] = data[i]['sourceId']
        df = df.append(row)
    df = df.reset_index()

    datef = lambda x: x[:10].replace('-','')
    timef = lambda x: x[11:13]
    sfunc = lambda x: x[:-2]

    df['date'] = df['referenceTime'].apply(datef)
    df['time'] = df['referenceTime'].apply(timef)
    df['sourceId'] = df['sourceId'].apply(sfunc)
    df = df[df['qualityCode']==0.0]
    df = df.drop_duplicates(subset=['sourceId', 'time', 'date'])

    return df[['sourceId', 'time', 'date', 'value', 'unit']]

def get_busstop_route(): 
    '''
    Returns: Dataframe with route_id and stop_id columns
    '''
    url = 'https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_rut-aggregated-gtfs.zip'

    myfile = requests.get(url)

    current_folder = os.getcwd()

    open(current_folder+'/routes.zip', 'wb').write(myfile.content)
    stops = pd.DataFrame()
    routes = pd.DataFrame()

    with ZipFile('routes.zip') as myzip:
        with myzip.open('stops.txt') as myfile1:
            stops = pd.read_csv(myfile1)
        with myzip.open('routes.txt') as myfile2:
            routes = pd.read_csv(myfile2)
        with myzip.open('trips.txt') as myfile3:
            trips = pd.read_csv(myfile3)
        with myzip.open('stop_times.txt') as myfile4:
            stop_times = pd.read_csv(myfile4)

    stops = stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]
    routes = routes[['route_id', 'route_short_name', 'route_long_name', 'route_type']]
    trips = trips[['trip_id', 'route_id']]
    stop_times = stop_times[['trip_id', 'stop_id']]

    df_ = pd.merge(stop_times, trips, on='trip_id', how='inner')
    df_ = pd.merge(df_, stops, on='stop_id', how='inner')
    df_ = df_.drop_duplicates()

    stop_route = pd.DataFrame()

    get_id = lambda x: re.findall(r'(\w+)$', str(x))[0]

    stop_route['route_id'] = df_['route_id'].apply(get_id)
    stop_route['stop_id'] = df_['stop_id'].apply(get_id)
    stop_route['stop_id'] = stop_route['stop_id'].apply(str)
    stop_route['bkey'] = stop_route['stop_id'] + df_['stop_name']
    
    stop_route = stop_route.drop_duplicates()
    stop_route.reset_index(inplace=True)
    return stop_route

def get_routes(): 

    '''
    Returns: Dataframe with route_id and stop_id columns
    '''
    url = 'https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_rut-aggregated-gtfs.zip'

    myfile = requests.get(url)

    current_folder = os.getcwd()

    open(current_folder+'/routes.zip', 'wb').write(myfile.content)
    routes = pd.DataFrame()

    with ZipFile('routes.zip') as myzip:
        with myzip.open('routes.txt') as myfile2:
            routes = pd.read_csv(myfile2)

    routes = routes[['route_id', 'route_short_name', 'route_long_name', 'route_type']]

    get_id = lambda x: re.findall(r'(\w+)$', str(x))[0]

    routes['route_id'] = routes['route_id'].apply(get_id)

    return routes

def get_busstop_latlon():
    '''
    Returns: Dataframe with route_id, name, LATITUDE AND LONGITUDE
    '''
    url = 'https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_rut-aggregated-gtfs.zip'

    myfile = requests.get(url)

    current_folder = os.getcwd()

    open(current_folder+'/routes.zip', 'wb').write(myfile.content)
    stops = pd.DataFrame()

    with ZipFile('routes.zip') as myzip:
        with myzip.open('stops.txt') as myfile1:
            stops = pd.read_csv(myfile1)

    stops = stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]

    get_id = lambda x: re.findall(r'(\w+)$', str(x))[0]

    stops['stop_id'] = stops['stop_id'].apply(get_id)
    return stops

# def insert_weather_on_stations():
#     stations = get_weather_station_latlon()
#     stations = stations['id']
#     conn, cur = db.connect()
#     data = pd.DataFrame()
#     for sn in stations:
#         print(sn)
#         data = get_weather_on_station(sn)
#         query = '''insert into frostentur.weatherStationTemperatures(sourceId, referenceTime, date,  value, unit) 
#                         values (%s, %s, %s, %s, %s)'''
#         arg_list = []
#         if(data is None): continue
#         for _, arg in data.iterrows():
#             print(arg)
#             arg_list.append((str(arg['sourceId']), str(arg['referenceTime']), str(arg['date']), str(arg['value']), str(arg['unit'])))

#         cur.executemany(query, arg_list)
        
#         conn.commit()
#     db.close(conn, cur)
#     return 1

def insert_weather_station_latlon():
    stations = get_weather_station_latlon()
    conn, cur = db.connect()
    arg_list = []
    for _, arg in stations.iterrows():
        arg_list.append((str(arg['id']), str(arg['name']), str(arg['lat']), str(arg['lon'])))
        
    query = '''insert into frostentur.weatherStationlatlon(sourceId, name, lat, lon) 
                        values (%s, %s, %s, %s)'''
    cur.executemany(query, arg_list)
        
    conn.commit()
    db.close(conn, cur)
    return 1

def insert_busstop_route():
    stations = get_busstop_route()
    conn, cur = db.connect()
    arg_list = []
    print(len(stations))
    for index, arg in stations.iterrows():
        arg_list.append((str(arg['route_id']), str(arg['bkey'])))
        if(index%100 == 0):
            query = '''insert into datamart_star.route_busstop(route_id, bkey) 
                        values (%s, %s)'''
            cur.executemany(query, arg_list)
            print(f'inserted: {index}')
            arg_list = []
            conn.commit()

    query = '''insert into datamart_star.route_busstop(route_id, bkey)  
                        values (%s, %s)'''
    
    cur.executemany(query, arg_list)
        
    conn.commit()
    db.close(conn, cur)
    return 1

def insert_busstop_latlon():
    stations = get_busstop_latlon()
    conn, cur = db.connect()
    arg_list = []
    for _, arg in stations.iterrows():
        arg_list.append((str(arg['stop_id']), str(arg['stop_name']), str(arg['stop_lat']) ,str(arg['stop_lon'])))

    query = '''insert into frostentur.busstoplatlon(stop_id, stop_name, lat, lon) 
                        values (%s, %s, %s, %s)'''
    cur.executemany(query, arg_list)
        
    conn.commit()
    db.close(conn, cur)
    return 1

def insert_routes():

    query = '''CREATE TABLE IF NOT EXISTS frostentur.routes(route_id int,
	route_short_name text,
	route_long_name text,
	route_type text);'''

    stations = get_routes()
    conn, cur = db.connect()
    cur.execute(query)
    arg_list = []
    
    for index, arg in stations.iterrows():
        arg_list.append((str(arg['route_id']), str(arg['route_short_name']), str(arg['route_long_name']), str(arg['route_type'])))
        if(index%100 == 0):
            query = '''insert into frostentur.routes(route_id, route_short_name, route_long_name, route_type) 
                        values (%s, %s, %s, %s)'''
            cur.executemany(query, arg_list)
            print(f'inserted: {index}')
            arg_list = []
            conn.commit()

    query = '''insert into frostentur.routes(route_id, route_short_name, route_long_name, route_type) 
                        values (%s, %s, %s, %s)'''
    
    cur.executemany(query, arg_list)
        
    conn.commit()
    db.close(conn, cur)
    return 1

def update_daily():
    conn, cur = db.connect()
    yesterday = str(date.today()- timedelta(days=1))
    valid_oslo_stations = ['SN18701','SN18315','SN18240','SN18210','SN18270','SN18500','SN18020','SN17980','SN18269','SN18815','SN76914','SN18690','SN18410','SN18700','SN18920','SN18420','SN18950','SN18980']
    query = '''CREATE TABLE IF NOT EXISTS frostentur.DailyTemperatures (sourceId text, timek int, datek int, value float, unit text)'''
    cur.execute(query)
    query = '''TRUNCATE TABLE frostentur.DailyTemperatures;'''
    cur.execute(query)
    query = '''insert into frostentur.DailyTemperatures(sourceId, timek, datek, value, unit) 
                         values (%s, %s, %s, %s, %s)'''

    for station in valid_oslo_stations:
        # ['sourceId', 'time', 'date', 'value', 'unit']
        result = get_weather_on_station(station, yesterday)
        if(result is None):
            continue
        row_list = []
        for _, row in result.iterrows():
            row_list.append((str(row['sourceId']), str(row['time']), str(row['date']), str(row['value']), str(row['unit'])))
        
        cur.executemany(query, row_list)
        conn.commit()

    query = '''insert into datamart_star.facttable_1 (buss_stop_id, sourceId, temperatur, kdate, ktime)
                select bs.bkey, d.sourceid ,d.value ,d.datek, d.timek from frostentur.DailyTemperatures d 
                join datamart_star.closest_buss_weather_station cbws using(sourceId)
                join datamart_star.buss_stopp bs on bs.buss_stopp_name = cbws.stop_name ;'''
    cur.execute(query)
    db.close(conn, cur)

def get_max_temperatur_per_route():
    yesterday = str(date.today()- timedelta(days=1)).replace('-','')
    query = f'''select mt.route_long_name, max(t.buss_stopp_name), max(t.temperatur)
    from
		(select r.route_long_name , max(f.temperatur) max_temperatur
		from datamart_star.facttable_1 f 
		join datamart_star.buss_stopp bs on f.buss_stop_id = bs.bkey
		join datamart_star.route_busstop rb using(bkey)
		join datamart_star.route r using(route_id)
        where f.kdate = {yesterday}
		group by r.route_long_name order by max_temperatur desc) as mt
	join
		(select bs.buss_stopp_name, r.route_long_name, f.temperatur
		from datamart_star.facttable_1 f 
		join datamart_star.buss_stopp bs on f.buss_stop_id = bs.bkey
		join datamart_star.route_busstop rb using(bkey)
		join datamart_star.route r using(route_id)
		order by temperatur desc) as t
	on t.route_long_name = mt.route_long_name
	where mt.max_temperatur = t.temperatur
	group by mt.route_long_name order by max(t.temperatur) desc;'''
    conn, cur = db.connect()
    cur.execute(query)

    results = cur.fetchall()
    for line in results:
        print(line)

    db.close(conn, cur)

yesterday = str(date.today()- timedelta(days=1))
print('INSERTING YESTERDAYS RESULT')
try:
    update_daily()
except:
    pass
print('GETTING BEST WEATHER FOR YESTERDAYS ROUTES')
print(f'ROUTE NAME, BUSS STOP NAME, TEMPERATUR: Date {yesterday}')
get_max_temperatur_per_route()