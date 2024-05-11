import json
import base64
import logging

def decode_message(data):
    try:
        decoded_data = data.decode('utf-8')
    except UnicodeDecodeError:
        try:
            decoded_data = data.decode('iso-8859-1')
        except UnicodeDecodeError:
            decoded_data = base64.b64encode(data).decode('ascii')

    return decoded_data


def extract_json_data(data):
    start = data.find("[{")
    end = data.rfind("}]") + 2
    if start != -1 and end != -1:
        json_str = data[start:end]
        try:
            json_data = json.loads(json_str)
        except json.JSONDecodeError:
            logging.error("Failed to decode JSON from the extracted portion")
            return
    else:
        logging.error("No JSON-like structure found in the message data")
        return
    
    return json_data