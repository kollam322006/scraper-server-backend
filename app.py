# app.py (The API Server)

from flask import Flask, request, jsonify, g
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
import os
import redis
from rq import Queue, Retry
from rq.job import Job

# Import task function and model from the tasks file
from scraper_tasks import Email, db, run_batch_scrape_task 

# --- FLASK AND DATABASE SETUP ---

app = Flask(__name__)
CORS(app)

# Database Configuration (Reads the DATABASE_URL environment variable)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Database initialization (done here for API functions like get_all_emails)
db.init_app(app)

# Create tables if they don't exist (needed for API to run)
with app.app_context():
    db.create_all()

# --- RQ SETUP ---

# Function to get Redis connection (safe way to handle connections across requests)
def get_redis_conn():
    if 'redis_conn' not in g:
        # Get the Redis URL from the environment (Render's internal URL)
        redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
        g.redis_conn = redis.from_url(redis_url)
    return g.redis_conn

# Function to get RQ Queue
def get_queue():
    if 'queue' not in g:
        g.queue = Queue(connection=get_redis_conn())
    return g.queue

# --- API ENDPOINTS ---

@app.route('/start_batch_scrape', methods=['POST'])
def start_batch_scrape():
    data = request.get_json()
    raw_input_urls = data.get('urls') 

    if not raw_input_urls:
        return jsonify({"error": "Missing 'urls' in request body"}), 400

    # Enqueue the job using the Redis Queue
    queue = get_queue()
    
    # Set up retry logic in case of temporary connection failure
    retry_policy = Retry(max=3, interval=[10, 30, 60]) 
    
    job = queue.enqueue(
        run_batch_scrape_task, # The function in scraper_tasks.py
        raw_input_urls,
        job_timeout='2h', # Max time for the job to run
        retry=retry_policy,
        # We can pass initial metadata to display progress later if we update the scraper task
        meta={'total_domains': len([link.strip() for link in raw_input_urls.split('\n') if link.strip()])} 
    )
    
    # Immediately return the Job ID to the frontend
    return jsonify({"job_id": job.id, "message": "Scraping job started."}), 202


@app.route('/status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    queue = get_queue()
    try:
        # Fetch the job object from Redis
        job = Job.fetch(job_id, connection=queue.connection)
    except Exception:
        return jsonify({"error": "Job ID not found or expired"}), 404
    
    # Map RQ Statuses for friendly frontend display
    status_map = {
        'queued': 'QUEUED',
        'started': 'RUNNING',
        'finished': 'COMPLETED',
        'failed': 'FAILED'
    }
    
    return jsonify({
        "job_id": job.id,
        "status": status_map.get(job.get_status(), 'UNKNOWN'),
        "result": job.result, # The final message from the task
        "meta": job.meta, # Metadata we passed on job start
        "percent_complete": 100 if job.get_status() == 'finished' else 0, # Simplified progress
        "queued_at": job.enqueued_at.strftime('%Y-%m-%d %H:%M:%S') if job.enqueued_at else None
    })


@app.route('/emails', methods=['GET'])
def get_all_emails():
    # Query the database for all saved emails
    emails = db.session.execute(db.select(Email).order_by(Email.id)).scalars()
    
    email_list = []
    for email in emails:
        email_list.append({
            'email_address': email.email_address,
            'source_url': email.source_url
        })

    return jsonify(email_list)


if __name__ == '__main__':
    # When running locally, you must run the worker separately
    app.run(debug=True)
