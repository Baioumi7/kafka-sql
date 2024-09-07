import pyodbc
from confluent_kafka import Consumer, KafkaException
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sql_server_config = {
    'server': 'localhost',
    'port': 1433,
    'database': 'WeatherDB',
    'username': 'sa',
    'password': 'NewPassword!'
}

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

kafka_topic = 'WEATHER'

def connect_to_sql_server(config):
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={config['server']},{config['port']};"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        f"Connection Timeout=60;"
    )
    try:
        connection = pyodbc.connect(connection_string)
        logger.info("Connected to SQL Server")
        return connection
    except pyodbc.Error as e:
        logger.error(f"Connection error: {e}")
        raise

def insert_into_sql_server(connection, data):
    cursor = connection.cursor()
    try:
        cursor.execute("""
            INSERT INTO WeatherData (
                date, temperature_2m, relative_humidity_2m, rain, snowfall,
                weather_code, surface_pressure, cloud_cover, cloud_cover_low,
                cloud_cover_high, wind_direction_10m, wind_direction_100m,
                soil_temperature_28_to_100cm
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data['date'], data['temperature_2m'], data['relative_humidity_2m'],
            data['rain'], data['snowfall'], data['weather_code'], 
            data['surface_pressure'], data['cloud_cover'], 
            data['cloud_cover_low'], data['cloud_cover_high'], 
            data['wind_direction_10m'], data['wind_direction_100m'], 
            data['soil_temperature_28_to_100cm']
        ))
        connection.commit()
        logger.info(f"Inserted data: {data}")
    except pyodbc.Error as e:
        logger.error(f"Error inserting data: {e}")
    finally:
        cursor.close()

def consume_messages(consumer, connection):
    consumer.subscribe([kafka_topic])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            
            data = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Received message: {data}")
            insert_into_sql_server(connection, data)
    except KeyboardInterrupt:
        logger.info("Consumer interrupted")
    except KafkaException as e:
        logger.error(f"Kafka error: {e}")
    finally:
        consumer.close()

def main():
    connection = None
    consumer = None

    try:
        connection = connect_to_sql_server(sql_server_config)
        consumer = Consumer(kafka_config)
        consume_messages(consumer, connection)
    finally:
        if connection:
            connection.close()
        if consumer:
            consumer.close()

if __name__ == "__main__":
    main()
