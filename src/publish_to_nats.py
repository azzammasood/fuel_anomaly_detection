import os
import asyncio
import time
import random
import json

from nats.aio.client import Client as NATS

# Read environment variables
NATS_IP = os.getenv("NATS_IP", "demo.nats.io")
NATS_PORT = os.getenv("NATS_PORT", 4222)
NATS_SUBJECT = os.getenv("NATS_SUBJECT", "fuel-data")

# Targets
siteid_list = ['PH-BUL-00991', 'PH-BUL-01136', 'PH-NCR-01309', 'PH-NCR-01559', 'PH-NCR-01629', 'PH-PAM-00909',
               'PH-PAM-00972']

hwcode_list = ['rectifier-1', 'aircon2em-1', 'aircon1em-1', 'aircon3em-1', 'aircon6em-1', 'aircon4em-1', 'aircon7em-1',
               'aircon5em-1', 'aircon1em-51', 'aircon3em-53', 'aircon2em-52']

powerstate = ['DG', 'mains', 'batt']
columns_list = ["siteid", "updatetime", "rect1power", "rect2power", "rect3power", "rect4power", "gridpower",
                "gridenergy", "gen1fuellevel", "gen2fuellevel", "gen3fuellevel", "hwcode", "powerstate"]

delay = 5


async def run():
    # Create a NATS client and connect to the NATS server
    nc = NATS()
    await nc.connect(f"nats://{NATS_IP}:{NATS_PORT}")

    prev_values = {}
    i = 0
    while True:
        i += 1
        print(f"\n____________________________________________________________________________________________________")
        print(f"SENDING PACKET {i} TO SUBJECT: {NATS_SUBJECT}")

        # Loop through siteid_list
        for siteid in siteid_list:
            synthetic_data = []
            # Randomly select hwcode for each site
            hwcode = random.choice(hwcode_list)
            # Loop through columns_list
            for col in columns_list:
                entry = {"name": col}
                # Check conditions for different columns
                if col == "siteid":
                    entry["value"] = siteid
                elif col == "updatetime":
                    entry["value"] = int(time.time())
                elif col.startswith("rect") or col == "gridpower" or col == "gridenergy":
                    # Generate random number for rect and grid columns
                    if col in prev_values and random.random() < 0.8:
                        entry["value"] = prev_values[col]
                    else:
                        entry["value"] = random.randint(0, 100)
                elif col.startswith("gen"):
                    # Generate random number for genfuellevel columns
                    if col in prev_values and random.random() < 0.8:
                        entry["value"] = prev_values[col]
                    else:
                        entry["value"] = random.randint(0, 1000)
                elif col == "hwcode":
                    entry["value"] = hwcode
                elif col == "powerstate":
                    # Randomly assign a power state from the list
                    entry["value"] = random.choice(powerstate)
                else:
                    # Generate random number for other columns
                    entry["value"] = random.randint(1, 100)

                # Update the previous value for the current column
                prev_values[col] = entry["value"]
                synthetic_data.append(entry)
            synthetic_data_json = json.dumps(synthetic_data)
            print(f"--> Sent {siteid} packet")

            await nc.publish(NATS_SUBJECT, synthetic_data_json.encode())

        print(f"\n____________________________________________________________________________________________________")
        # Wait for the specified delay before sending the next batch
        await asyncio.sleep(delay)


if __name__ == "__main__":
    asyncio.run(run())
