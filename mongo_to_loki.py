from pymongo import MongoClient
import http.client
import json
import time
from datetime import datetime
from bson import json_util

def push_to_loki(message, operation=None, collection=None):
    # Current timestamp in nanoseconds
    timestamp = int(time.time() * 1e9)
    
    # Add basic labels for MongoDB operations
    stream_labels = {
        "source": "mongodb",
        "operation": operation or "unknown",
        "collection": collection or "unknown"
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
        print(f"Log sent - Status: {response.status}, Operation: {operation}, Message: {message}")
        return response.status == 204
    except Exception as e:
        print(f"Error sending log: {str(e)}")
        return False
    finally:
        conn.close()

def watch_mongodb_changes():
    # Connect to MongoDB (update these parameters as needed)
    client = MongoClient('mongodb://localhost:27017/')
    
    # Select your database
    db = client['oryza_metadata_test']
    collection = db['Event']
    
    print(f"Watching for MongoDB changes in collection: {collection.name}...")
    
    # Keep track of last processed timestamp
    last_timestamp = None
    
    try:
        while True:
            # Query for new documents
            query = {}
            if last_timestamp:
                query['_id'] = {'$gt': last_timestamp}
            
            # Sort by _id to ensure we don't miss any documents
            cursor = collection.find(query).sort('_id', 1).limit(100)
            
            for doc in cursor:
                # Update last processed timestamp
                last_timestamp = doc['_id']
                
                # Format the document
                doc_str = json.loads(json_util.dumps(doc))
                message = f"New or updated document in Event collection: {doc_str}"
                
                # Push to Loki
                push_to_loki(message, "change", "Event")
            
            # Sleep briefly before next check
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping MongoDB watcher...")
    except Exception as e:
        print(f"Error watching MongoDB changes: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    watch_mongodb_changes()
