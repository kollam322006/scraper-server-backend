import os
import sys
import redis
# Removed 'Connection' which caused the ImportError, as it is not needed here.
from rq import Worker 

# We attempt to import the necessary components (the worker task and the
# Redis connection object) from the now-fixed 'app.py'.
try:
    # Import the worker function (scrape_task), the Redis connection object, 
    # and the Flask app instance (app) to provide necessary context for DB operations.
    from app import scrape_task, redis_conn, app 
except ImportError as e:
    print(f"Error importing dependencies from app.py: {e}")
    # Exit gracefully if dependencies can't be loaded
    sys.exit(1)


# Define the queue(s) this worker will listen to
LISTEN = ['default'] 

if __name__ == '__main__':
    # We use the redis_conn object imported from app.py
    print("Starting RQ Worker...")
    
    # The 'with Connection(redis_conn):' block has been removed as it caused an ImportError.
    # The connection is passed directly to the Worker constructor below.
    worker = Worker(LISTEN, connection=redis_conn)
    
    # Start the worker process
    worker.work()
