# worker.py (The RQ Worker process)

import os
import redis
# FIX: Only import Worker and Queue from rq to avoid the ImportError
from rq import Worker, Queue 

# Import your database and task functions (needed to run tasks inside worker context)
from scraper_tasks import db, Email

listen = ['default']

# Get the REDIS_URL from the environment variables
redis_url = os.environ.get('REDIS_URL')

if not redis_url:
    raise RuntimeError("REDIS_URL not set in environment.")

# Use the redis library to create the connection object
conn = redis.from_url(redis_url)

if __name__ == '__main__':
    # Flask application context is required for SQLAlchemy (db.session) to work inside the worker
    from app import app
    with app.app_context():
        # Initialize the worker, passing the queue list and the connection object
        # This setup correctly handles the Redis connection without needing a specific 'Connection' import
        worker = Worker(list(map(Queue, listen)), connection=conn)
        print("Starting RQ Worker...")
        worker.work()
