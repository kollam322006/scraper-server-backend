# worker.py

import os
import redis
from rq import Worker, Connection

# Get the internal Redis URL from the environment variable linked on Render
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')

if __name__ == '__main__':
    # Connect to Redis using the URL
    redis_conn = redis.from_url(REDIS_URL)
    
    with Connection(redis_conn):
        # The worker listens on the default queue
        worker = Worker(['default']) 
        worker.work()
