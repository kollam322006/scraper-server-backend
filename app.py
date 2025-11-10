# app.py

from flask import Flask, request, jsonify, g, Response
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy 
import os
import redis
from rq import Queue, Retry
from rq.job import Job
from sqlalchemy import or_, and_
import io
import csv
from datetime import datetime
from openpyxl import Workbook # NEW IMPORT
from openpyxl.writer.excel import save_virtual_workbook # NEW IMPORT

# Import task function and models from the tasks file
from scraper_tasks import Email, db, run_batch_scrape_task, Batch

# --- FLASK AND DATABASE SETUP ---

app = Flask(__name__)

# NOTE: Update 'origins' with your Netlify URL and local dev URL
CORS(app, origins=[
    "https://temmemailextractor.netlify.app", 
    "http://localhost:3000" 
])

# Database Configuration 
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Database initialization
db.init_app(app)

# Create tables (will update schema with new models)
with app.app_context():
    db.create_all()

# --- RQ SETUP ---

def get_redis_conn():
    if 'redis_conn' not in g:
        redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
        g.redis_conn = redis.from_url(redis_url)
        # Store connection in app config for worker task to use
        app.config['REDIS_CONNECTION'] = g.redis_conn 
    return g.redis_conn

def get_queue():
    if 'queue' not in g:
        g.queue = Queue(connection=get_redis_conn())
    return g.queue

# --- API ENDPOINTS ---

@app.route('/start_batch_scrape', methods=['POST'])
def start_batch_scrape():
    """Enqueues a new scraping job with a user-provided batch name."""
    data = request.get_json()
    raw_input_urls = data.get('urls')
    batch_name = data.get('batch_name', f"Batch {datetime.now().strftime('%Y%m%d%H%M%S')}") # Default name if none provided

    if not raw_input_urls:
        return jsonify({"error": "Missing 'urls' in request body"}), 400

    queue = get_queue()
    retry_policy = Retry(max=3, interval=[10, 30, 60]) 
    
    # Pass job_id, urls, and batch_name to the worker task
    job = queue.enqueue(
        run_batch_scrape_task, 
        job.id, # Pass job ID to worker to update its own status
        raw_input_urls,
        batch_name,
        job_timeout='2h', 
        retry=retry_policy,
        # Initialize meta data for progress tracking
        meta={'domains_processed': 0, 'total_domains': len([link.strip() for link in raw_input_urls.split('\n') if link.strip()])} 
    )
    
    return jsonify({"job_id": job.id, "batch_name": batch_name, "message": "Scraping job started."}), 202


@app.route('/status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Retrieves the current status and real-time progress of a queued job."""
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
    
    current_status = status_map.get(job.get_status(), 'UNKNOWN')
    
    # Check meta for real-time progress
    domains_processed = job.meta.get('domains_processed', 0)
    total_domains = job.meta.get('total_domains', 0)
    
    # Calculate simple percentage progress
    progress = 0
    if total_domains > 0 and current_status == 'RUNNING':
        progress = int((domains_processed / total_domains) * 100)
    elif current_status == 'COMPLETED':
        progress = 100

    # Look up the associated batch name and status
    batch_name = job.kwargs[2] # The 3rd argument passed to the task
    
    return jsonify({
        "job_id": job.id,
        "batch_name": batch_name,
        "status": current_status,
        "progress_percent": progress,
        "domains_processed": domains_processed,
        "total_domains": total_domains,
        "result": job.result if current_status in ['COMPLETED', 'FAILED'] else None
    })

# --- BATCH MANAGEMENT ENDPOINTS ---

@app.route('/batches', methods=['GET'])
def list_batches():
    """Lists all saved batches."""
    batches = db.session.execute(db.select(Batch).order_by(Batch.created_at.desc())).scalars()
    
    batch_list = []
    for batch in batches:
        # Count emails linked to the batch for display
        email_count = db.session.query(Email).filter_by(batch_id=batch.id).count()
        
        batch_list.append({
            'id': batch.id,
            'name': batch.batch_name,
            'created_at': batch.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'status': batch.status,
            'email_count': email_count
        })
    return jsonify(batch_list)

@app.route('/batches/<int:batch_id>/emails', methods=['GET'])
def get_emails_by_batch(batch_id):
    """Retrieves emails for a specific batch, supporting search."""
    search_query = request.args.get('q')
    
    query = db.select(Email).filter(Email.batch_id == batch_id).order_by(Email.id.desc())
    
    if search_query:
        search_term = f"%{search_query.lower()}%"
        query = query.filter(
            or_(
                Email.email_address.ilike(search_term),
                Email.source_url.ilike(search_term),
                Email.source_domain_name.ilike(search_term)
            )
        )
    
    emails = db.session.execute(query).scalars()
    
    email_list = []
    for email in emails:
        email_list.append({
            'id': email.id,
            'email_address': email.email_address,
            'source_url': email.source_url,
            'source_domain': email.source_domain_name
        })

    return jsonify(email_list)


@app.route('/batches/export/xlsx/<int:batch_id>', methods=['GET'])
def export_emails_xlsx(batch_id):
    """Exports emails for a specific batch to an XLSX file."""
    batch = db.session.get(Batch, batch_id)
    if not batch:
        return jsonify({"error": "Batch not found"}), 404

    emails = db.session.execute(db.select(Email).filter(Email.batch_id == batch_id).order_by(Email.id.desc())).scalars()

    # Create an in-memory workbook
    wb = Workbook()
    ws = wb.active
    ws.title = batch.batch_name

    # Write header row
    ws.append(['ID', 'Email Address', 'Source URL', 'Source Domain'])
    
    # Write data rows
    for email in emails:
        ws.append([email.id, email.email_address, email.source_url, email.source_domain_name])

    # Save to a virtual file
    response = Response(
        save_virtual_workbook(wb),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment;filename="{batch.batch_name}_emails.xlsx"'}
    )
    return response


@app.route('/batches/delete', methods=['DELETE'])
def delete_batches():
    """Deletes one or more batches (cascading to delete associated emails)."""
    data = request.get_json()
    batch_ids = data.get('ids')

    if not batch_ids or not isinstance(batch_ids, list):
        return jsonify({"error": "Missing or invalid list of 'ids' in request body"}), 400

    try:
        # Deleting a Batch automatically deletes associated Emails due to cascade="all, delete-orphan"
        delete_count = db.session.query(Batch).filter(Batch.id.in_(batch_ids)).delete(synchronize_session='fetch')
        db.session.commit()
        
        return jsonify({"message": f"Successfully deleted {delete_count} batches and associated emails.", "deleted_ids": batch_ids}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Failed to delete batches: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True)
