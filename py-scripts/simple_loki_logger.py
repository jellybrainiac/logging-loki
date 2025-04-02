import http.client
import json
import time
from datetime import datetime

def push_to_loki(message, labels=None):
    if labels is None:
        labels = {}
    
    # Current timestamp in nanoseconds
    timestamp = int(time.time() * 1e9)
    
    # Prepare the payload
    payload = {
        "streams": [
            {
                "stream": labels,
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
    
    # Set headers
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        # Send request
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
        # Example labels
        labels = {
            "app": "my-app",
            "environment": "dev"
        }
        
        # Send some test logs
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        push_to_loki(f"Info log at {current_time}", labels)
        push_to_loki(f"Error occurred at {current_time}", {**labels, "level": "error"})
        
        # Wait 5 seconds before next logs
        time.sleep(5)

if __name__ == "__main__":
    main()
