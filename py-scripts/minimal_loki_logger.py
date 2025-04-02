import http.client
import json
import time
from datetime import datetime

def push_to_loki(message):
    # Current timestamp in nanoseconds
    timestamp = int(time.time() * 1e9)
    
    # Minimal payload without labels
    payload = {
        "streams": [
            {
                "stream": {},  # Empty labels
                "values": [
                    [str(timestamp), message]
                ]
            }
        ]
    }
    
    # Convert payload to JSON
    json_payload = json.dumps(payload)
    
    # Create connection
    conn = http.client.HTTPConnection("localhost", 3100)
    headers = {"Content-Type": "application/json"}
    
    try:
        conn.request("POST", "/loki/api/v1/push", json_payload, headers)
        response = conn.getresponse()
        print(f"Log sent - Status: {response.status}, Message: {message}")
        return response.status == 204
    except Exception as e:
        print(f"Error sending log: {str(e)}")
        return False
    finally:
        conn.close()

def main():
    while True:
        # Just send a simple message with timestamp
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        push_to_loki(f"Test log message at {current_time}")
        time.sleep(5)

if __name__ == "__main__":
    main()
