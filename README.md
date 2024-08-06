# Version 1.0 - Single threaded processing

```python data_collection.py```

```python data_processing.py```

1. Connect to nats server
2. Subscribe to all sub-channels in channels.>
3. Retrieve data
4. Filter data for rectifier-1 and collect data
5. Publish data to channel 'fuel_data_processing'
6. Recieve data from channel 'fuel_data_processing' and process it
7. Upsert data to database

Current Behavior: If data_processing.py is single-threaded, it processes one message at a time. When data_collection.py publishes a new message while data_processing.py is still processing the previous one, the new message will be queued. Once the current processing is complete, data_processing.py will pick up the next message.
Implication: This ensures messages are processed sequentially, but there could be a delay if processing takes a long time.
