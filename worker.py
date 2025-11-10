import os
import sys
import redis
from rq import Worker, Connection

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
    
    # The Connection context manager ensures the worker uses the correct Redis settings.
    with Connection(redis_conn):
        # We create the Worker instance, listening to the defined queue(s).
        # When tasks run, they will have access to the functions and models defined in app.py.
        worker = Worker(LISTEN, connection=redis_conn)
        
        # Start the worker process
        worker.work()
