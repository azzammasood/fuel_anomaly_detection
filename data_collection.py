import json
import asyncio
import logging
import os
from nats.aio.client import Client as NATSClient
from src.logs import setup_logging
from src.utils import decode_message, extract_json_data

# Load config
with open('configs/config.json', 'r') as f:
    config = json.load(f)

# Extract config variables
MAX_RECENT_DATA = config["max_recent_data"]  # Maximum number of recent data packets to store
nats_servers = config["nats_servers"]  # NATS server addresses
processing_subject = config["processing_subject"]  # NATS subject to publish combined data

recent_data = {}  # Global dictionary to store recent data for each siteid
cached_powerstate = {}  # Cache for powerstate from rectifier-1

# NATS connection and asyncio loop
async def main():
    nc = NATSClient()
    await nc.connect(servers=nats_servers)

    logging.info(f"Connected to NATS servers at {nats_servers}")

    async def message_handler(msg):
        try:
            data = decode_message(msg.data)

            # Extract JSON-like portion from the data
            json_data = extract_json_data(data)

            # Extract site and hardware info
            siteid = None
            hwcode = None
            gateway = None
            for item in json_data:
                if 'bn' in item:
                    site_info = item['bn']
                    siteid = site_info.split(":")[0]
                    hwcode_parts = site_info.split(":")[1].split('--')
                    hwcode = hwcode_parts[0]
                    gateway = hwcode_parts[1] if len(hwcode_parts) > 1 else ''
                    break

            if not siteid or not hwcode:
                logging.error("Site ID or HW Code not found in the data")
                return

            if 'rectifier-1' in hwcode:
                logging.info(f"Received rectifier-1 packet for siteid: {siteid}")
                for item in json_data:
                    if item.get('n') == 'powerstate':
                        cached_powerstate[siteid] = item.get('vs')
                        logging.info(f"Cached powerstate for siteid: {siteid} - {cached_powerstate[siteid]}")
                        break

            if 'env-1' in hwcode:
                logging.info(f"Received env-1 packet for siteid: {siteid}")
                env_keys = {'fuellevel1', 'fuellevel2', 'fuellevel3'}

                new_data = {
                    'siteid': siteid,
                    'hwcode': hwcode,
                    'gateway': gateway,  # Include gateway in the data
                    'powerstate': cached_powerstate.get(siteid, '-'),  # Use cached powerstate or default to '-'
                    'fuellevel1': 0,
                    'fuellevel2': 0,
                    'fuellevel3': 0,
                    'updatetime': 0
                }

                # Get updatetime from env-1
                for item in json_data:
                    if 'ut' in item:
                        new_data['updatetime'] = item.get('ut')
                        break

                # Process fuel values and convert negatives to zero
                for item in json_data:
                    key = item.get('n').lower()
                    if key in env_keys and item.get('v', None) is not None:
                        new_data[key] = max(item['v'], 0)  # Convert negative values to zero

                if siteid not in recent_data:
                    recent_data[siteid] = []

                if len(recent_data[siteid]) < MAX_RECENT_DATA:
                    recent_data[siteid].append(new_data)
                else:
                    # Find and remove the oldest packet
                    oldest_packet_index = min(range(len(recent_data[siteid])), key=lambda i: recent_data[siteid][i]['updatetime'])
                    removed_packet = recent_data[siteid].pop(oldest_packet_index)
                    logging.info(f"Removed oldest packet with updatetime {removed_packet['updatetime']} for siteid: {siteid}")
                    recent_data[siteid].append(new_data)

                if len(recent_data[siteid]) == MAX_RECENT_DATA:
                    logging.info(f"Collected {MAX_RECENT_DATA} packets for siteid: {siteid}")
                    await nc.publish(processing_subject, json.dumps(recent_data[siteid]).encode('utf-8'))

                # Print the final output
                print(f"Final data for siteid {siteid}:")
                print(json.dumps(new_data, indent=2))


        except Exception as e:
            logging.error(f"Error processing message: {e}")

    await nc.subscribe("channels.>", cb=message_handler)
    logging.info(f"Subscribed to 'channels.>'")
    await asyncio.Future()  # Keep the connection open
    await nc.close()

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.realpath(__file__))
    log_path = os.path.join(current_dir, "logs", "data_collection_logs")
    setup_logging(base_dir=log_path)

    logging.info(f"Starting data collection")

    asyncio.run(main())
