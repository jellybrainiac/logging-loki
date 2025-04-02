import greenstalk
import http.client
import json
import time
from datetime import datetime

def push_to_loki(message, tube=None, job_id=None):
    # Current timestamp in nanoseconds
    timestamp = int(time.time() * 1e9)
    
    # Add basic labels for Beanstalkd
    stream_labels = {
        "source": "beanstalkd",
        "tube": tube or "default",
        "job_id": str(job_id) if job_id else "unknown"
    }
    
    payload = {
        "streams": [
            {
                "stream": stream_labels,
                "values": [
                    [str(timestamp), message]
                ]
            }
        ]
    }
    
    # Convert payload to JSON
    json_payload = json.dumps(payload)
    
    # Create connection to Loki
    conn = http.client.HTTPConnection("localhost", 3100)
    headers = {"Content-Type": "application/json"}
    
    try:
        conn.request("POST", "/loki/api/v1/push", json_payload, headers)
        response = conn.getresponse()
        print(f"Log sent - Status: {response.status}, Tube: {tube}, Job ID: {job_id}, Message: {message}")
        return response.status == 204
    except Exception as e:
        print(f"Error sending log: {str(e)}")
        return False
    finally:
        conn.close()

def watch_beanstalkd(tubes=None, timeout=None):
    # Connect to Beanstalkd (update host and port if needed)
    client = greenstalk.Client(('127.0.0.1', 11300))
    
    # Watch specified tubes or use default
    if tubes:
        for tube in tubes:
            client.watch(tube)
    
    print(f"Watching Beanstalkd tubes: {tubes or ['default']}")
    
    try:
        while True:
            try:
                # Reserve a job (with optional timeout)
                job = client.reserve(timeout=timeout)
                
                # Get current tube
                current_tube = client.using()
                
                # Create message with job details
                try:
                    # Try to parse job body as JSON
                    job_body = json.loads(job.body)
                    message = f"New job in tube {current_tube}: {json.dumps(job_body)}"
                except json.JSONDecodeError:
                    # If not JSON, use raw body
                    message = f"New job in tube {current_tube}: {job.body}"
                
                # Push to Loki
                push_to_loki(message, current_tube, job.id)
                
                # Delete the job after processing
                client.delete(job)
                
            except greenstalk.TimedOutError:
                print("No jobs available, waiting...")
                continue
                
    except KeyboardInterrupt:
        print("\nStopping Beanstalkd watcher...")
    except Exception as e:
        print(f"Error watching Beanstalkd: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    # Example: watch specific tubes
    tubes_to_watch = ["default", "notifications", "emails"]  # customize this list
    watch_beanstalkd(tubes=tubes_to_watch)
