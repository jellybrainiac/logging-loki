from beanie import Document, init_beanie
from motor.motor_asyncio import AsyncIOMotorClient
import http.client
import json
import time
import asyncio
from datetime import datetime
from typing import Optional
from pydantic import Field

# Example Beanie document model
class User(Document):
    name: str
    email: str
    age: Optional[int] = None
    
    class Settings:
        name = "users"  # collection name

def push_to_loki(message, operation=None, collection=None):
    # Current timestamp in nanoseconds
    timestamp = int(time.time() * 1e9)
    
    # Add basic labels for MongoDB/Beanie operations
    stream_labels = {
        "source": "mongodb",
        "operation": operation or "unknown",
        "collection": collection or "unknown",
        "odm": "beanie"
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
        print(f"Log sent - Status: {response.status}, Operation: {operation}, Collection: {collection}, Message: {message}")
        return response.status == 204
    except Exception as e:
        print(f"Error sending log: {str(e)}")
        return False
    finally:
        conn.close()

async def watch_collection_changes():
    # MongoDB connection settings
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    
    # Initialize Beanie with the document models
    await init_beanie(database=client.your_database, document_models=[User])
    
    # Get change stream for the User collection
    change_stream = User.watch()
    
    print("Watching for MongoDB changes using Beanie...")
    
    try:
        async for change in change_stream:
            # Get operation type
            operation = change['operationType']
            collection = "users"  # or change['ns']['coll']
            
            # Format message based on operation type
            if operation == 'insert':
                document = change['fullDocument']
                message = f"New user inserted: {document}"
            elif operation == 'update':
                update_desc = change['updateDescription']
                message = f"User updated: {update_desc}"
            elif operation == 'delete':
                doc_key = change['documentKey']
                message = f"User deleted: {doc_key}"
            else:
                message = f"Operation {operation}: {change}"
            
            # Push to Loki
            push_to_loki(message, operation, collection)
            
    except Exception as e:
        print(f"Error watching changes: {str(e)}")

# Example of how to insert a test document
async def insert_test_user():
    user = User(name="Test User", email="test@example.com", age=25)
    await user.insert()
    print("Test user inserted")

async def main():
    # Start watching for changes
    watch_task = asyncio.create_task(watch_collection_changes())
    
    # Wait a bit for the watcher to start
    await asyncio.sleep(2)
    
    # Insert a test user (uncomment to test)
    # await insert_test_user()
    
    try:
        # Keep the script running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping Beanie watcher...")

if __name__ == "__main__":
    asyncio.run(main())
