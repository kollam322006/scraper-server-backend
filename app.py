# app.py (The complete, robust API Server)

from flask import Flask, request, jsonify, g, Response
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy  # <-- FIX: Corrected typo
import os
import redis
from rq import Queue, Retry
from rq.job import Job
from sqlalchemy import or_
import io
import csv
from datetime import datetime

# Import task function and model from the tasks file
# NOTE: This assumes scraper_tasks.py is correct and contains Email, db, and run_batch_scrape_task
from scraper_tasks import Email, db, run_batch_scrape_task 

# --- FLASK AND DATABASE SETUP ---

app = Flask(__name__)
CORS(app)

# Database Configuration (Reads the DATABASE_URL environment variable)
# NOTE: This relies on the DATABASE_URL being set in Render Environment
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Database initialization
db.init_app(app)

# Create tables if they don't exist
with app.app_context():
    db.create_all()

# --- RQ SETUP ---

# Function to get Redis connection
def get_redis_conn():
    if 'redis_conn' not in g:
        # NOTE: This relies on the REDIS_URL being set in Render Environment
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
    """Enqueues a new scraping job to the Redis Queue."""
    data = request.get_json()
    raw_input_urls = data.get('urls') 

    if not raw_input_urls:
        return jsonify({"error": "Missing 'urls' in request body"}), 400

    queue = get_queue()
    retry_policy = Retry(max=3, interval=[10, 30, 60]) 
    
    job = queue.enqueue(
        run_batch_scrape_task, 
        raw_input_urls,
        job_timeout='2h', 
        retry=retry_policy,
        meta={'total_domains': len([link.strip() for link in raw_input_urls.split('\n') if link.strip()])} 
    )
    
    return jsonify({"job_id": job.id, "message": "Scraping job started."}), 202


@app.route('/status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Retrieves the current status and result of a queued job."""
    queue = get_queue()
    try:
        job = Job.fetch(job_id, connection=queue.connection)
    except Exception:
        return jsonify({"error": "Job ID not found or expired"}), 404
    
    status_map = {
        'queued': 'QUEUED',
        'started': 'RUNNING',
        'finished': 'COMPLETED',
        'failed': 'FAILED'
    }
    
    # Simple progress calculation, since detailed meta updates are complex on this setup
    progress = 100 if job.get_status() == 'finished' else (50 if job.get_status() == 'started' else 0)
    result_text = job.result if job.result else (
        "Job failed during processing." if job.get_status() == 'failed' else "Processing domains..."
    )
    
    return jsonify({
        "job_id": job.id,
        "status": status_map.get(job.get_status(), 'UNKNOWN'),
        "result": result_text, 
        "progress": progress,
        "queued_at": job.enqueued_at.strftime('%Y-%m-%d %H:%M:%S') if job.enqueued_at else None
    })

# --- DATABASE MANAGEMENT ENDPOINTS ---

@app.route('/emails', methods=['GET'])
def get_emails():
    """Retrieves all emails, supporting an optional 'q' search parameter."""
    search_query = request.args.get('q')
    
    query = db.select(Email).order_by(Email.id.desc())
    
    if search_query:
        search_term = f"%{search_query.lower()}%"
        query = query.filter(
            or_(
                Email.email_address.ilike(search_term),
                Email.source_url.ilike(search_term)
            )
        )
    
    emails = db.session.execute(query).scalars()
    
    email_list = []
    for email in emails:
        email_list.append({
            'id': email.id,
            'email_address': email.email_address,
            'source_url': email.source_url
        })

    return jsonify(email_list)


@app.route('/emails/delete', methods=['DELETE'])
def delete_emails():
    """Deletes one or more emails based on a list of IDs."""
    data = request.get_json()
    email_ids = data.get('ids')

    if not email_ids or not isinstance(email_ids, list):
        return jsonify({"error": "Missing or invalid list of 'ids' in request body"}), 400

    try:
        delete_count = db.session.query(Email).filter(Email.id.in_(email_ids)).delete(synchronize_session='fetch')
        db.session.commit()
        
        return jsonify({"message": f"Successfully deleted {delete_count} emails.", "deleted_ids": email_ids}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Failed to delete emails: {str(e)}"}), 500


@app.route('/emails/export', methods=['GET'])
def export_emails():
    """Exports all or filtered emails as a downloadable CSV file."""
    search_query = request.args.get('q') 
    
    query = db.select(Email).order_by(Email.id.desc())
    
    if search_query:
        search_term = f"%{search_query.lower()}%"
        query = query.filter(
            or_(
                Email.email_address.ilike(search_term),
                Email.source_url.ilike(search_term)
            )
        )
    
    emails = db.session.execute(query).scalars()
    
    # Create a CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)

    # Write header row
    writer.writerow(['ID', 'Email Address', 'Source URL'])
    
    # Write data rows
    for email in emails:
        writer.writerow([email.id, email.email_address, email.source_url])

    # Package it as a downloadable response
    response = Response(output.getvalue(), mimetype="text/csv")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    response.headers["Content-Disposition"] = f"attachment; filename=scraped_emails_{timestamp}.csv"
    
    return response

# This is only for local testing, the Render start command runs gunicorn
if __name__ == '__main__':
    app.run(debug=True)
