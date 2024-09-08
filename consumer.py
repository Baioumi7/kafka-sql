import json
from confluent_kafka import Consumer, KafkaError
import pyodbc
from datetime import datetime
import time

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'weather_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['WEATHER'])

# SQL Server Configuration
server = 'localhost'  # Use 'localhost' for your local SQL Server
database = 'WeatherDB'
username = 'sa'  # Replace with your SQL Server username
password = 'NewPassword!'  # Replace with your SQL Server password
driver = '{ODBC Driver 17 for SQL Server}'  # Make sure this driver is installed

# Function to get current time as a formatted string
def get_current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Function to create database if not exists
def create_database_if_not_exists(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT database_id FROM sys.databases WHERE name = 'WeatherDB'")
    result = cursor.fetchone()
    if not result:
        print(f"{get_current_time()} Creating database WeatherDB...")
        cursor.execute("CREATE DATABASE WeatherDB")
        conn.commit()
    cursor.close()

# Establish connection to SQL Server
conn_str = f'DRIVER={driver};SERVER={server};DATABASE=master;UID={username};PWD={password}'

# Attempt to connect with retry logic
max_retries = 5
retry_delay = 5  # seconds

for attempt in range(max_retries):
    try:
        print(f"{get_current_time()} Attempting to connect to local SQL Server (Attempt {attempt + 1})...")
        conn = pyodbc.connect(conn_str, autocommit=True)
        print(f"{get_current_time()} Connected to local SQL Server successfully.")
        break
    except pyodbc.Error as e:
        print(f"{get_current_time()} Failed to connect to local SQL Server: {e}")
        if attempt < max_retries - 1:
            print(f"{get_current_time()} Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        else:
            print(f"{get_current_time()} Max retries reached. Exiting.")
            exit(1)

# Create WeatherDB database if it doesn't exist
create_database_if_not_exists(conn)

# Switch to WeatherDB
conn.close()
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

print(f"{get_current_time()} WeatherData table created or verified.")

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

# Function to read and display data from SQL Server
def read_weather_data():
    select_query = '''
    SELECT TOP 5 * FROM WeatherData ORDER BY id DESC
    '''
    cursor.execute(select_query)
    rows = cursor.fetchall()
    print(f"\n{get_current_time()} Last 5 records in WeatherData table:")
    for row in rows:
        print(row)
    print("\n")

# Main consumer loop
print(f"{get_current_time()} Starting Kafka consumer. Waiting for messages...")
message_count = 0
last_read_time = time.time()

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"{get_current_time()} Reached end of partition")
            else:
                print(f"{get_current_time()} Error: {msg.error()}")
        else:
            print(f"{get_current_time()} Received message: {msg.value().decode('utf-8')}")
            try:
                weather_data = json.loads(msg.value().decode('utf-8'))
                insert_weather_data(weather_data)
                print(f"{get_current_time()} Inserted data for date: {weather_data['date']}")
                message_count += 1

                # Read and display data every 5 messages or every 30 seconds, whichever comes first
                current_time = time.time()
                if message_count % 5 == 0 or (current_time - last_read_time) > 30:
                    read_weather_data()
                    last_read_time = current_time
                    message_count = 0

            except Exception as e:
                print(f"{get_current_time()} Error processing message: {e}")

except KeyboardInterrupt:
    print(f"{get_current_time()} Interrupted by user")

finally:
    consumer.close()
    cursor.close()
    conn.close()
    print(f"{get_current_time()} Closed all connections. Script terminated.")
