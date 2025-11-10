import io # New import needed for BytesIO handling
import os
import time
import json
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy

# RQ/Redis for background tasks
import redis
from rq import Queue

# XLSX library imports
from openpyxl import Workbook
from io import BytesIO # Also ensure BytesIO is imported from the standard library's io module

# --- FIX FOR OPENPYXL IMPORT ERROR ---
def save_virtual_workbook(workbook):
    """
    Replacement function for the removed openpyxl.writer.excel.save_virtual_workbook.
    It saves the workbook content to an in-memory BytesIO object.
    """
    virtual_file = io.BytesIO()
    # Use the standard save method, which accepts file-like objects
    workbook.save(virtual_file)
    # Return the byte content, just like the old function did
    return virtual_file.getvalue()
# --- END FIX ---


# --- Configuration ---
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}) # Allow all origins for the frontend

# Database Configuration (Assuming PostgreSQL on Render)
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://user:password@localhost/emails')
# SQLAlchemy recommends changing the postgres:// scheme to postgresql://
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Redis Configuration (Assuming Redis on Render)
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
redis_conn = redis.from_url(REDIS_URL)
queue = Queue(connection=redis_conn)

# --- Database Models ---

class Batch(db.Model):
    __tablename__ = 'batches'
    id = db.Column(db.Integer, primary_key=True)
    batch_name = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(50), default='PENDING') # PENDING, RUNNING, COMPLETED, FAILED
    job_id = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    domains_processed = db.Column(db.Integer, default=0)
    total_domains = db.Column(db.Integer, default=0)
    
    # Relationship to emails
    emails = db.relationship('Email', backref='batch', lazy='dynamic', cascade="all, delete-orphan")

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.batch_name,
            'status': self.status,
            'job_id': self.job_id,
            'created_at': self.created_at.isoformat(),
            'domains_processed': self.domains_processed,
            'total_domains': self.total_domains,
            'email_count': self.emails.count()
        }

class Email(db.Model):
    __tablename__ = 'emails'
    id = db.Column(db.Integer, primary_key=True)
    batch_id = db.Column(db.Integer, db.ForeignKey('batches.id'), nullable=False)
    email_address = db.Column(db.String(255), nullable=False, index=True)
    source_url = db.Column(db.String(512), nullable=False)
    source_domain_name = db.Column(db.String(255), nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'batch_id': self.batch_id,
            'email_address': self.email_address,
            'source_url': self.source_url,
            'source_domain': self.source_domain_name,
            'created_at': self.created_at.isoformat()
        }

# --- Database Initialization ---
with app.app_context():
    db.create_all()

# --- Scraper Logic (Worker Task) ---

def scrape_task(batch_id, urls):
    """
    Background task to scrape a list of URLs and save emails to the database.
    """
    # NOTE: This function needs to be in worker.py as well, or imported by it.
    # Ensure worker.py is also updated with the 'save_virtual_workbook' fix if it imports app.py!
    
    with app.app_context():
        batch = db.session.get(Batch, batch_id)
        if not batch:
            print(f"Batch {batch_id} not found, stopping task.")
            return

        total_urls = len(urls)
        batch.total_domains = total_urls
        db.session.commit()

        processed_count = 0
        all_emails = set()

        for url in urls:
            try:
                # 1. Fetch content
                response = requests.get(url, timeout=10)
                response.raise_for_status() # Raise exception for bad status codes (4xx or 5xx)
                
                # 2. Parse content
                soup = BeautifulSoup(response.text, 'lxml')
                text = soup.get_text()
                
                # 3. Email extraction (simple regex for demonstration, more robust needed in production)
                import re
                emails_found = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text))
                
                # 4. Filter and save unique emails to database
                domain = urlparse(url).netloc
                for email_address in emails_found:
                    # Check if email is already in the batch
                    exists = db.session.execute(
                        db.select(Email).filter_by(batch_id=batch_id, email_address=email_address)
                    ).scalar_one_or_none()
                    
                    if not exists:
                        new_email = Email(
                            batch_id=batch_id,
                            email_address=email_address,
                            source_url=url,
                            source_domain_name=domain
                        )
                        db.session.add(new_email)
                        all_emails.add(email_address)

                db.session.commit()

            except requests.exceptions.RequestException as e:
                print(f"Request failed for {url}: {e}")
            except Exception as e:
                print(f"An unexpected error occurred for {url}: {e}")
            
            finally:
                processed_count += 1
                # Update status in the database after processing each URL
                batch.domains_processed = processed_count
                batch.status = 'RUNNING'
                db.session.commit()

        # Final update
        batch.status = 'COMPLETED'
        batch.domains_processed = total_urls
        db.session.commit()
        
        return f"Batch '{batch.batch_name}' finished. Found {len(all_emails)} unique emails."

# --- API Endpoints ---

@app.route('/start_batch_scrape', methods=['POST'])
def start_batch_scrape():
    """Starts a new background scraping job."""
    data = request.get_json()
    urls_raw = data.get('urls', '')
    batch_name = data.get('batch_name', f"Batch {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    if not urls_raw:
        return jsonify({"error": "No URLs provided"}), 400

    # Clean and split URLs
    urls = [u.strip() for u in urls_raw.split('\n') if u.strip()]
    if not urls:
        return jsonify({"error": "No valid URLs provided"}), 400

    with app.app_context():
        # 1. Create a new Batch record
        new_batch = Batch(batch_name=batch_name, status='PENDING', total_domains=len(urls))
        db.session.add(new_batch)
        db.session.commit()
        batch_id = new_batch.id

        # 2. Enqueue the scraping task
        job = queue.enqueue(scrape_task, batch_id, urls, job_timeout='1h')
        
        # 3. Update Batch with RQ Job ID
        new_batch.job_id = job.id
        db.session.commit()

        return jsonify({
            "message": "Scraping job started",
            "batch_id": batch_id,
            "batch_name": batch_name,
            "job_id": job.id,
            "status": "QUEUED"
        }), 202


@app.route('/status/<job_id>', methods=['GET'])
def get_status(job_id):
    """Retrieves the live status of an RQ job."""
    job = queue.fetch_job(job_id)
    
    with app.app_context():
        batch = db.session.query(Batch).filter_by(job_id=job_id).first()
        
        if not batch:
            return jsonify({"status": "FAILED", "progress_percent": 0, "result": "Batch not found."}), 404

        status = batch.status
        domains_processed = batch.domains_processed
        total_domains = batch.total_domains
        progress_percent = round((domains_processed / total_domains) * 100) if total_domains > 0 else 0

        result = job.result if job and job.is_finished else None
        
        return jsonify({
            "status": status,
            "progress_percent": progress_percent,
            "domains_processed": domains_processed,
            "total_domains": total_domains,
            "result": result
        })


@app.route('/batches', methods=['GET'])
def list_batches():
    """Returns a list of all saved batches."""
    with app.app_context():
        batches = Batch.query.order_by(Batch.created_at.desc()).all()
        return jsonify([b.to_dict() for b in batches]), 200


@app.route('/batches/<int:batch_id>/emails', methods=['GET'])
def get_batch_emails(batch_id):
    """Returns emails for a specific batch, optionally filtered by query string."""
    query_string = request.args.get('q', '').lower()
    
    with app.app_context():
        # Base query
        email_query = Email.query.filter_by(batch_id=batch_id)
        
        # Apply search filter
        if query_string:
            search = f"%{query_string}%"
            email_query = email_query.filter(
                db.or_(
                    Email.email_address.ilike(search),
                    Email.source_domain_name.ilike(search),
                    Email.source_url.ilike(search)
                )
            )

        emails = email_query.order_by(Email.created_at.desc()).all()
        
        if not emails and not Batch.query.get(batch_id):
            return jsonify({"error": f"Batch with ID {batch_id} not found"}), 404
            
        return jsonify([e.to_dict() for e in emails]), 200

# --- NEW FEATURE: BATCH RENAMING (PATCH) ---
@app.route('/batches/<int:batch_id>', methods=['PATCH'])
def update_batch(batch_id):
    """Updates the name of a specific batch."""
    data = request.get_json()
    new_name = data.get('batch_name')

    if not new_name or not new_name.strip():
        return jsonify({"error": "Missing or empty 'batch_name' in request body"}), 400

    with app.app_context():
        batch = db.session.get(Batch, batch_id)
        if not batch:
            return jsonify({"error": f"Batch with ID {batch_id} not found"}), 404

        try:
            batch.batch_name = new_name.strip()
            db.session.commit()
            return jsonify({
                "message": f"Batch ID {batch_id} renamed to '{batch.batch_name}' successfully.",
                "id": batch.id,
                "new_name": batch.batch_name
            }), 200
        except Exception as e:
            db.session.rollback()
            return jsonify({"error": f"Failed to rename batch: {str(e)}"}), 500

# --- NEW FEATURE: EMAIL EDITING (PATCH) ---
@app.route('/emails/<int:email_id>', methods=['PATCH'])
def update_email(email_id):
    """Updates the address and/or source domain of a specific email."""
    data = request.get_json()
    new_address = data.get('email_address')
    new_domain = data.get('source_domain')

    if not new_address and not new_domain:
        return jsonify({"error": "No fields provided for update"}), 400

    with app.app_context():
        email = db.session.get(Email, email_id)
        if not email:
            return jsonify({"error": f"Email with ID {email_id} not found"}), 404

        try:
            if new_address and new_address.strip():
                email.email_address = new_address.strip()
            if new_domain and new_domain.strip():
                email.source_domain_name = new_domain.strip()

            db.session.commit()
            return jsonify({
                "message": f"Email ID {email_id} updated successfully.",
                "email_address": email.email_address,
                "source_domain": email.source_domain_name
            }), 200
        except Exception as e:
            db.session.rollback()
            return jsonify({"error": f"Failed to update email: {str(e)}"}), 500

# --- NEW FEATURE: MASS EMAIL DELETION (DELETE) ---
@app.route('/emails/delete', methods=['DELETE'])
def delete_emails():
    """Deletes selected individual emails from the database."""
    data = request.get_json()
    email_ids = data.get('ids')

    if not email_ids or not isinstance(email_ids, list):
        return jsonify({"error": "Missing or invalid list of 'ids' in request body"}), 400

    try:
        # Delete emails where the ID is in the provided list
        delete_count = db.session.query(Email).filter(Email.id.in_(email_ids)).delete(synchronize_session='fetch')
        db.session.commit()
        
        if delete_count == 0:
            return jsonify({"message": "No emails found with the provided IDs.", "deleted_ids": []}), 404

        return jsonify({"message": f"Successfully deleted {delete_count} email(s).", "deleted_ids": email_ids}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Failed to delete emails: {str(e)}"}), 500

# --- EXISTING FEATURE: BATCH DELETION ---
@app.route('/batches/delete', methods=['DELETE'])
def delete_batches():
    """Deletes batches and all associated emails."""
    data = request.get_json()
    batch_ids = data.get('ids')

    if not batch_ids or not isinstance(batch_ids, list):
        return jsonify({"error": "Missing or invalid list of 'ids' in request body"}), 400

    try:
        # SQLAlchemy cascade="all, delete-orphan" handles Email deletion automatically
        delete_count = db.session.query(Batch).filter(Batch.id.in_(batch_ids)).delete(synchronize_session='fetch')
        db.session.commit()

        if delete_count == 0:
            return jsonify({"message": "No batches found with the provided IDs.", "deleted_ids": []}), 404
        
        return jsonify({"message": f"Successfully deleted {delete_count} batch(es) and associated emails.", "deleted_ids": batch_ids}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Failed to delete batches: {str(e)}"}), 500


# --- EXISTING FEATURE: XLSX EXPORT ---
@app.route('/batches/export/xlsx/<int:batch_id>', methods=['GET'])
def export_batch_xlsx(batch_id):
    """Generates and returns an XLSX file for a specific batch."""
    with app.app_context():
        batch = db.session.get(Batch, batch_id)
        if not batch:
            return jsonify({"error": f"Batch with ID {batch_id} not found"}), 404

        emails = Email.query.filter_by(batch_id=batch_id).order_by(Email.created_at.desc()).all()

        if not emails:
            return jsonify({"error": "No emails found in this batch to export"}), 404

        # Create Workbook and Worksheet
        wb = Workbook(write_only=True)
        ws = wb.create_sheet(title=batch.batch_name)

        # Write header row
        header = ["Email Address", "Source Domain", "Source URL", "Date Scraped"]
        ws.append(header)

        # Write data rows
        for email in emails:
            row = [
                email.email_address,
                email.source_domain_name,
                email.source_url,
                email.created_at.strftime("%Y-%m-%d %H:%M:%S")
            ]
            ws.append(row)

        # Save the workbook to an in-memory file using the fixed helper function
        virtual_file = save_virtual_workbook(wb)

        # Prepare the response
        file_data = BytesIO(virtual_file)
        filename = f"{batch.batch_name.replace(' ', '_')}_emails.xlsx"

        return send_file(
            file_data,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )


if __name__ == '__main__':
    # This is typically only used for local development
    # On Render, Gunicorn runs the app using `gunicorn app:app`
    with app.app_context():
        db.create_all()
    app.run(host='0.0.0.0', port=os.environ.get('PORT', 5000))
