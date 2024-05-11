import os
import json
import pandas as pd
import logging
import asyncio
from datetime import datetime, timedelta
from nats.aio.client import Client as NATSClient
import redis
from src.logs import setup_logging
from src.postgresql.db_operations import insert_data_to_table, update_data_in_table, truncate_table, create_table_if_not_exists, remove_siteid_from_table
from src.postgresql.db_connections import DatabaseConnection
from src.anomaly_detection import process_new_data

# Load configuration
with open('configs/config.json', 'r') as f:
    config = json.load(f)

db_config = config["db_config"]
refill_threshold = config["refill_threshold"]
theft_threshold = config["theft_threshold"]
litre_change_threshold = config["litre_change_threshold"]
nats_servers = config["nats_servers"]
processing_subject = config["processing_subject"]
results_table_1 = config["results_table_1"]
results_table_2 = config["results_table_2"]
results_table_3 = config["results_table_3"]
results_table_4 = config["results_table_4"]
redis_config = config["redis_config"]

# Setup logging
current_dir = os.path.dirname(os.path.realpath(__file__))
log_path = os.path.join(current_dir, "logs", "data_processing_logs")
setup_logging(base_dir=log_path)

# Establish database connection
db_connection = DatabaseConnection(db_config)
db_connection.connect()

# Establish Redis connection
redis_client = redis.Redis(host=redis_config['host'], port=redis_config['port'], db=redis_config['db'])

# Initialize global variable
df4_data = pd.DataFrame()

# Utility functions
def send_data_to_redis(df, redis_client, redis_key):
    try:
        data = df.to_json(orient='records')
        redis_client.hset(redis_key, mapping={'data': data})
        logging.info(f"Data sent to Redis under key '{redis_key}': {data}")
    except Exception as e:
        logging.error(f"Error sending data to Redis under key '{redis_key}': {e}")

def get_data_from_redis(redis_client, redis_key):
    try:
        data = redis_client.hget(redis_key, 'data')
        if data:
            return pd.read_json(data, orient='records')
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error getting data from Redis under key '{redis_key}': {e}")
        return pd.DataFrame()

def remove_data_from_redis(redis_client, redis_key):
    try:
        redis_client.delete(redis_key)
        logging.info(f"Data removed from Redis under key '{redis_key}'")
    except Exception as e:
        logging.error(f"Error removing data from Redis under key '{redis_key}': {e}")

def get_last_rows(df):
    return df.groupby('siteid').tail(1)

def ensure_epoch(df, column):
    try:
        df[column] = pd.to_datetime(df[column])
        df[column] = df[column].astype('int64') // 10**9
        logging.info(f"Successfully converted {column} to epoch")
    except Exception as e:
        logging.error(f"Error converting {column} to epoch: {e}")

def ensure_double_epoch(df, column):
    try:
        df[column] = pd.to_datetime(df[column])
        df[column] = (df[column].astype('int64') // 10**9).astype(float)
        logging.info(f"Successfully converted {column} to double epoch")
    except Exception as e:
        logging.error(f"Error converting {column} to double epoch: {e}")

async def insert_df4_to_db():
    global df4_data
    if not df4_data.empty:
        try:
            logging.debug(f"DataFrame df4_data before insertion:\n{df4_data}")
            logging.debug(f"DataFrame df4_data info:\n{df4_data.info()}")

            # Ensure datetime conversion to epoch
            ensure_epoch(df4_data, 'updatetime')
            ensure_epoch(df4_data, 'day')

            # Drop duplicates
            df4_data = df4_data.drop_duplicates(subset=['siteid', 'updatetime'], keep='last')

            # Add current timestamp
            df4_data['time'] = pd.Timestamp.now().floor('s').timestamp()

            logging.info(f"Inserting aggregated daily DataFrame into {results_table_4}")

            if not df4_data.empty:
                with db_connection.engine.begin() as connection:
                    # Insert data for each siteid separately
                    for siteid, group in df4_data.groupby('siteid'):
                        logging.info(f"Inserting data for siteid {siteid}")
                        insert_data_to_table(group, results_table_4, connection)
                df4_data = pd.DataFrame()
            else:
                logging.warning("No valid df4 data to insert after processing.")
        except Exception as e:
            logging.error(f"Error inserting df4 data: {e}")
    else:
        logging.info("No df4 data to insert at this time.")

async def main():
    nc = NATSClient()
    await nc.connect(servers=nats_servers)
    logging.info(f"Connected to NATS servers at {nats_servers}")

    async def message_handler(msg):
        global df4_data
        try:
            data = json.loads(msg.data.decode('utf-8'))
            df = pd.DataFrame(data)
            logging.info(f"Received data for processing for siteid {df['siteid'].iloc[0]} and {len(df)} packets")

            try:
                df1, df2, df3, df4 = await process_new_data(df, refill_threshold, theft_threshold)
            except Exception as e:
                logging.error(f"Error processing collected data: {e}")
                return

            if df2.empty and df3.empty:
                logging.warning("No events detected")
            else:
                df2 = df2[df2['displaypoint'] != 'normal']
                df3 = df3[df3['displaypoint'] != 'normal']

            try:
                with db_connection.engine.begin() as connection:
                    if not df1.empty:
                        df1 = get_last_rows(df1)
                        logging.info(f"Inserting DataFrame 1 into {results_table_1}")
                        ensure_epoch(df1, 'updatetime')
                        insert_data_to_table(df1, results_table_1, connection)

                    if not df2.empty:
                        for _, row in df2.iterrows():
                            siteid = row['siteid']
                            displaypoint = row['displaypoint']
                            opentime = row['opentime']
                            redis_key = f'results_table_3_{siteid}_{displaypoint}'
                            
                            previous_alarm = get_data_from_redis(redis_client, redis_key)
                            
                            if previous_alarm.empty:
                                send_data_to_redis(pd.DataFrame([row]), redis_client, redis_key)
                                logging.info(f"Inserting new event into {results_table_2} and Redis for siteid {siteid}")
                                insert_data_to_table(pd.DataFrame([row]), results_table_2, connection)
                            elif not pd.isna(row['closetime']):
                                existing_event = previous_alarm.iloc[0].copy()
                                existing_event['closetime'] = row['closetime']
                                existing_event['end_fuellevel'] = row['end_fuellevel']
                                update_data_in_table(pd.DataFrame([existing_event]), results_table_2, connection, ['siteid', 'displaypoint', 'opentime'])
                                logging.info(f"Updating closetime for event: {existing_event}")
                                remove_data_from_redis(redis_client, redis_key)
                            
                    if not df3.empty:
                        for _, row in df3.iterrows():
                            siteid = row['siteid']
                            displaypoint = row['displaypoint']
                            redis_key = f'results_table_3_{siteid}_{displaypoint}'
                            
                            previous_alarm = get_data_from_redis(redis_client, redis_key)
                            
                            if previous_alarm.empty:
                                send_data_to_redis(pd.DataFrame([row]), redis_client, redis_key)
                                logging.info(f"Inserting new event into {results_table_3} and Redis for siteid {siteid}")
                                insert_data_to_table(pd.DataFrame([row]), results_table_3, connection)
                            elif row['displaypoint'] == 'normal':
                                remove_data_from_redis(redis_client, redis_key)

                    if not df4.empty:
                        df4['updatetime'] = pd.to_datetime(df4['updatetime'], unit='s')
                        if df4_data.empty:
                            df4_data = df4
                        else:
                            df4_data = pd.concat([df4_data, df4])
                            df4_data = df4_data.drop_duplicates(subset=['siteid', 'updatetime'], keep='last')

            except Exception as e:
                logging.error(f"Error loading data to the database: {e}")
                return

            logging.info("Data processing completed successfully")

        except Exception as e:
            logging.error(f"Error in message handler: {e}")

    await nc.subscribe(processing_subject, cb=message_handler)

    async def schedule_df4_insert():
        while True:
            now = datetime.now()
            next_run = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            wait_time = (next_run - now).total_seconds()
            logging.info(f"Waiting for {wait_time} seconds until the next daily insert for df4.")
            await asyncio.sleep(wait_time)

            try:
                await insert_df4_to_db()
            except Exception as e:
                logging.error(f"Error during scheduled df4 insert: {e}")

    asyncio.create_task(schedule_df4_insert())

    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass

    await nc.close()

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.realpath(__file__))
    log_path = os.path.join(current_dir, "logs", "data_processing_logs")
    setup_logging(base_dir=log_path)

    logging.info(f"Starting data processing")

    asyncio.run(main())
