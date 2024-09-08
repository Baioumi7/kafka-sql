import json
from confluent_kafka import Consumer, KafkaError
import pyodbc
from datetime import datetime

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'weather_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['WEATHER'])

# SQL Server Configuration
server = 'localhost,1433'
database = 'WeatherDB'
username = 'sa'
password = 'NewPassword!'
driver = '{ODBC Driver 17 for SQL Server}'

# Function to create database if not exists
def create_database_if_not_exists(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT database_id FROM sys.databases WHERE name = 'WeatherDB'")
    result = cursor.fetchone()
    if not result:
        print("Creating database WeatherDB...")
        cursor.execute("CREATE DATABASE WeatherDB")
        conn.commit()
    cursor.close()

# Establish initial connection to SQL Server (master database)
initial_conn_str = f'DRIVER={driver};SERVER={server};UID={username};PWD={password}'
conn = pyodbc.connect(initial_conn_str, autocommit=True)

# Create WeatherDB database if it doesn't exist
create_database_if_not_exists(conn)

# Close the initial connection
conn.close()

# Establish connection to WeatherDB
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Create table if not exists
create_table_query = '''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='WeatherData' AND xtype='U')
CREATE TABLE WeatherData (
    id INT IDENTITY(1,1) PRIMARY KEY,
    date DATETIME,
    temperature_2m FLOAT,
    relative_humidity_2m FLOAT,
    rain FLOAT,
    snowfall FLOAT,
    weather_code INT,
    surface_pressure FLOAT,
    cloud_cover FLOAT,
    cloud_cover_low FLOAT,
    cloud_cover_high FLOAT,
    wind_direction_10m FLOAT,
    wind_direction_100m FLOAT,
    soil_temperature_28_to_100cm FLOAT
)
'''
cursor.execute(create_table_query)
conn.commit()

# Function to insert data into SQL Server
def insert_weather_data(data):
    insert_query = '''
    INSERT INTO WeatherData (date, temperature_2m, relative_humidity_2m, rain, snowfall, weather_code, 
                             surface_pressure, cloud_cover, cloud_cover_low, cloud_cover_high, 
                             wind_direction_10m, wind_direction_100m, soil_temperature_28_to_100cm)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    cursor.execute(insert_query, (
        datetime.strptime(data['date'], '%Y-%m-%d %H:%M:%S'),
        data['temperature_2m'],
        data['relative_humidity_2m'],
        data.get('rain', None),
        data.get('snowfall', None),
        data.get('weather_code', None),
        data.get('surface_pressure', None),
        data.get('cloud_cover', None),
        data.get('cloud_cover_low', None),
        data.get('cloud_cover_high', None),
        data.get('wind_direction_10m', None),
        data.get('wind_direction_100m', None),
        data.get('soil_temperature_28_to_100cm', None)
    ))
    conn.commit()

# Main consumer loop
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of partition')
            else:
                print(f'Error: {msg.error()}')
        else:
            print(f'Received message: {msg.value().decode("utf-8")}')
            try:
                weather_data = json.loads(msg.value().decode('utf-8'))
                insert_weather_data(weather_data)
                print(f"Inserted data for date: {weather_data['date']}")
            except Exception as e:
                print(f"Error processing message: {e}")

except KeyboardInterrupt:
    print('Interrupted by user')

finally:
    consumer.close()
    cursor.close()
    conn.close()