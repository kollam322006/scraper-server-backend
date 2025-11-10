# worker.py (The RQ Worker process)

import os
import redis
from rq import Worker, Queue 
from rq.connection import Connection # <-- FIX: Import Connection from the correct submodule
# Import your database and task functions
from scraper_tasks import db, Email

listen = ['default']

redis_url = os.environ.get('REDIS_URL')

if not redis_url:
    # This should be fixed now that you set the environment variable
    raise RuntimeError("REDIS_URL not set in environment.")

# Use redis.from_url to create the connection object
conn = redis.from_url(redis_url)

if __name__ == '__main__':
    # Flask application context is needed for SQLAlchemy to work inside the worker
    from app import app
    with app.app_context():
        # Use the Connection context manager provided by RQ
        with Connection(conn):
            # Create a worker that listens to the default queue
            worker = Worker(list(map(Queue, listen)))
            print("Starting RQ Worker...")
            worker.work()
