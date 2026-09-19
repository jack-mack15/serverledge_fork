import time
import json
import random
import string

def handler(params,context):
    num_records = int(params["n"])

    data_list = []
    for i in range(num_records):
        data_list.append({
            "device_id": f"sensor-{i}",
            "token": "".join(random.choices(string.ascii_letters, k=10)),
            "temperature": random.uniform(-10.0, 50.0),
            "status": "active" if i % 2 == 0 else "standby"
        })

    json_string = json.dumps(data_list)

    parsed_data = json.loads(json_string)
    payload_mb = len(json_string) / (1024 * 1024)

    return {
        "status": "success",
        "records_processed": num_records
    }