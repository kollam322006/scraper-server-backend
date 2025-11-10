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
        # 1. Correctly create the Queue objects *with* the connection
        #    before passing them to the Worker.
        queues = [Queue(name, connection=conn) for name in listen]

        # 2. Initialize the worker with the list of initialized Queue objects
        worker = Worker(queues, connection=conn) 
        
        print("Starting RQ Worker...")
        worker.work()
