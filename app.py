import os
import redis
import json
import time
import re
import io
from flask import Flask, jsonify, request, send_file
from flask_sqlalchemy import SQLAlchemy
from rq import Queue
from rq.job import Job
from typing import List, Optional
from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, DateTime, Text, Boolean, func, distinct
from openpyxl import Workbook
from urllib.parse import urlparse, urlunparse
from flask_cors import CORS # Needed for front-end communication
from io import BytesIO
import requests 
from bs4 import BeautifulSoup

# --- FIX FOR OPENPYXL IMPORT ERROR ---
# This function replaces the removed 'save_virtual_workbook'
def save_virtual_workbook(workbook):
    """
    Saves an openpyxl Workbook to an in-memory BytesIO object.
    """
    virtual_file = io.BytesIO()
    workbook.save(virtual_file)
    virtual_file.seek(0) # Rewind the file to the beginning
    return virtual_file.getvalue()
# --- END FIX ---


# --- Database and Queue Configuration ---

# 1. Database Configuration
DATABASE_URL = os.environ.get('DATABASE_URL')

# Standard URL correction (postgres -> postgresql)
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# CRITICAL FIX: Add sslmode=require for Render PostgreSQL connections 
# This fixes the "SSL connection has been closed unexpectedly" error.
if DATABASE_URL and 'sslmode=' not in DATABASE_URL:
    if '?' in DATABASE_URL:
        DATABASE_URL += '&sslmode=require'
    else:
        DATABASE_URL += '?sslmode=require'


# 2. Redis Configuration
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
redis_conn = redis.from_url(REDIS_URL)

# 3. Flask App Initialization
app = Flask(__name__)
# Allow all origins for simplicity. For production, limit to your Netlify URL.
CORS(app, resources={r"/*": {"origins": "*"}}) 

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL or 'sqlite:///scraper_data.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
q = Queue(connection=redis_conn)

# --- Database Models ---

class Batch(db.Model):
    __tablename__ = 'batches'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    status: Mapped[str] = mapped_column(String(50), default='PENDING')
    job_id: Mapped[str] = mapped_column(String(50), nullable=True)

    # CRITICAL FIX: cascade="all, delete-orphan" to fix Foreign Key Violation
    emails: Mapped[List["Email"]] = relationship("Email", back_populates="batch", cascade="all, delete-orphan")
    
    # Tracking for progress
    domains_to_scrape: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    total_domains: Mapped[int] = mapped_column(Integer, default=0)
    domains_processed: Mapped[int] = mapped_column(Integer, default=0)
    
    def __repr__(self):
        return f"<Batch {self.id}: {self.name}>"

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'status': self.status,
            'job_id': self.job_id,
            'created_at': self.created_at.isoformat(),
            'domains_processed': self.domains_processed,
            'total_domains': self.total_domains,
            'email_count': db.session.scalar(db.select(func.count(Email.id)).filter_by(batch_id=self.id))
        }

class Email(db.Model):
    __tablename__ = 'emails'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[int] = mapped_column(Integer, db.ForeignKey('batches.id'), nullable=False)
    email_address: Mapped[str] = mapped_column(String(255), nullable=False)
    source_url: Mapped[str] = mapped_column(String(500), nullable=False) 
    source_domain: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    batch: Mapped["Batch"] = relationship("Batch", back_populates="emails")

    __table_args__ = (
        # Unique constraint per batch: ensures one email is saved only once *per batch*
        db.UniqueConstraint('batch_id', 'email_address', 'source_url', name='uix_email_batch_source'), 
    )

    def to_dict(self):
        return {
            'id': self.id,
            'batch_id': self.batch_id,
            'email_address': self.email_address,
            'source_url': self.source_url,
            'source_domain': self.source_domain,
            'created_at': self.created_at.isoformat()
        }

# --- Scraper Task Helper Functions ---

def sanitize_and_expand_urls(raw_urls_string):
    """
    Takes a raw string of domains/URLs, sanitizes them, and expands each domain 
    into a list of target URLs for scraping.
    Returns a list of tuples: (url_to_scrape, base_domain_url_to_save, domain_to_save)
    """
    # Define the 3 paths to check for each domain
    TARGET_SUFFIXES = [
        "", # For the original link / homepage
        "/policies/privacy-policy",
        "/policies/contact-information"
    ]
    
    urls_to_scrape_and_save_as = [] 
    raw_links = [u.strip() for u in raw_urls_string.split('\n') if u.strip()]

    for link in raw_links:
        try:
            if '://' not in link:
                link = 'https://' + link

            parsed_url = urlparse(link)
            
            # Sanitize the network location (netloc) to remove 'www.'
            netloc = parsed_url.netloc.replace('www.', '')
            scheme = 'https' # Force HTTPS
            
            # This is the Base URL we will save emails against
            base_url_parts = (scheme, netloc, '', '', '', '')
            base_domain_url = urlunparse(base_url_parts)
            domain_to_save = netloc
            
            # Expand into target URLs (3 total)
            for suffix in TARGET_SUFFIXES:
                target_url_parts = (scheme, netloc, suffix, '', '', '')
                url_to_scrape = urlunparse(target_url_parts)
                
                # Append the tuple: (URL to fetch, Base URL for saving, Base Domain for saving)
                urls_to_scrape_and_save_as.append((url_to_scrape, base_domain_url, domain_to_save))

        except Exception as e:
            print(f"[ERROR] Skipping invalid URL: {link}. Error: {e}")
            continue

    final_urls = list(set(urls_to_scrape_and_save_as))
    print(f"[DEBUG] Generated {len(final_urls)} target URLs from {len(raw_links)} raw inputs.")
    return final_urls

# --- Scraper Task ---

def scrape_task(batch_id: int, urls_to_process: list):
    """
    Background task to scrape a list of URLs and save emails to the database.
    'urls_to_process' is a list of tuples: (url_to_scrape, base_domain_url_to_save, domain_to_save)
    """
    print(f"[DEBUG] Worker started for Batch ID: {batch_id}")
    
    with app.app_context():
        batch = db.session.get(Batch, batch_id)
        if not batch:
            print(f"[ERROR] Batch {batch_id} not found, stopping task.")
            return

        batch.status = 'RUNNING'
        db.session.commit()
        
        try:
            total_urls = len(urls_to_process)
            batch.total_domains = total_urls
            db.session.commit()

            total_emails_saved_in_batch = 0
            processed_count = 0
            
            for url_to_scrape, base_domain_url_to_save, domain_to_save in urls_to_process:
                
                try:
                    print(f"[DEBUG] Processing URL: {url_to_scrape} (Grouping under: {base_domain_url_to_save})")
                    
                    response = requests.get(url_to_scrape, timeout=10)
                    response.raise_for_status() 
                    
                    soup = BeautifulSoup(response.text, 'lxml')
                    text = soup.get_text()
                    
                    emails_found = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text))
                    
                    for email_address in emails_found:
                        email_address = email_address.lower()
                        
                        exists = db.session.execute(
                            db.select(Email).filter_by(batch_id=batch_id, 
                                                      email_address=email_address,
                                                      source_url=base_domain_url_to_save)
                        ).scalar_one_or_none()
                        
                        if not exists:
                            new_email = Email(
                                batch_id=batch_id,
                                email_address=email_address,
                                source_url=base_domain_url_to_save,
                                source_domain=domain_to_save
                            )
                            db.session.add(new_email)
                            total_emails_saved_in_batch += 1
                            print(f"[DEBUG] SAVED new email: {email_address} (Source: {base_domain_url_to_save})")

                    db.session.commit()

                except requests.exceptions.RequestException as e:
                    print(f"[ERROR] Request failed for {url_to_scrape}: {e}")
                    db.session.rollback()
                except Exception as e:
                    print(f"[FATAL] An unexpected error occurred while processing {url_to_scrape}: {e}")
                    db.session.rollback()
                
                finally:
                    processed_count += 1
                    batch.domains_processed = processed_count
                    db.session.commit() # Commit progress after each URL

            batch.status = 'COMPLETED'
            db.session.commit()
            return f"Batch scrape finished. Saved {total_emails_saved_in_batch} unique email(s)."

        except Exception as e:
            db.session.rollback()
            print(f"[FATAL] Error during scraping for Batch {batch_id}: {e}")
            batch.status = 'FAILED'
            db.session.commit()
            raise

# --- API Endpoints ---

@app.route('/create_tables', methods=['POST'])
def create_tables_endpoint():
    """Utility endpoint to manually create database tables."""
    with app.app_context():
        db.create_all()
    return jsonify({"message": "Tables created or already exist."}), 201

@app.route('/start_batch_scrape', methods=['POST'])
def start_batch_scrape():
    """Starts a new background scraping job."""
    data = request.json
    urls_input = data.get('urls', '')
    batch_name = data.get('batch_name', f'Batch {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}')

    if not urls_input:
        return jsonify({"error": "No URLs provided"}), 400

    # 1. Sanitize and expand URLs
    urls_to_process = sanitize_and_expand_urls(urls_input)
    if not urls_to_process:
        return jsonify({"error": "No valid URLs could be processed"}), 400
    
    with app.app_context():
        # 2. Create a new Batch record
        new_batch = Batch(
            name=batch_name, 
            status='PENDING',
            domains_to_scrape=json.dumps([url[1] for url in urls_to_process]), # Store base domains
            total_domains=len(urls_to_process) # Total URLs to scrape
        )
        db.session.add(new_batch)
        db.session.commit()
        batch_id = new_batch.id
        
        # 3. Enqueue the scraping task
        job = q.enqueue(scrape_task, batch_id, urls_to_process, job_timeout='1h') 
        
        new_batch.job_id = job.id
        db.session.commit()

        return jsonify({
            "message": "Scraping job queued",
            "batch_id": batch_id,
            "job_id": job.id,
            "status": "QUEUED"
        }), 202

@app.route('/status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Retrieves the live status of an RQ job from the database."""
    with app.app_context():
        batch = db.session.execute(db.select(Batch).filter_by(job_id=job_id)).scalar_one_or_none()
        
        if not batch:
            # Check RQ job status as a fallback
            try:
                job = Job.fetch(job_id, connection=redis_conn)
                rq_status = job.get_status()
                return jsonify({"status": rq_status.upper(), "progress_percent": 0, "result": "Job found in queue but not in DB."}), 200
            except:
                 return jsonify({"status": "NOT_FOUND", "progress_percent": 0, "result": "Job not found."}), 404

        # Job found in DB, report its status
        total_domains = batch.total_domains
        domains_processed = batch.domains_processed
        progress_percent = int((domains_processed / total_domains) * 100) if total_domains > 0 else 0
        db_status = batch.status.upper()
        
        result_text = "Processing..."
        if db_status == 'COMPLETED':
            job = Job.fetch(job_id, connection=redis_conn)
            result_text = job.result if job.result else "Batch scrape finished."
        elif db_status == 'FAILED':
            result_text = "Job failed during processing."

        return jsonify({
            "status": db_status,
            "progress_percent": progress_percent,
            "domains_processed": domains_processed,
            "total_domains": total_domains,
            "result": result_text
        }), 200

@app.route('/batches', methods=['GET'])
def get_batches():
    """Returns a list of all saved batches."""
    with app.app_context():
        batches = db.session.execute(
            db.select(Batch).order_by(Batch.created_at.desc())
        ).scalars().all()
        
        return jsonify([b.to_dict() for b in batches]), 200

@app.route('/batches/<int:batch_id>/emails', methods=['GET'])
def get_batch_emails(batch_id):
    """Returns emails for a specific batch, optionally filtered by query string."""
    search_query = request.args.get('q')
    
    with app.app_context():
        batch = db.session.get(Batch, batch_id)
        if not batch:
            return jsonify({"error": "Batch not found"}), 404

        query = db.select(Email).filter_by(batch_id=batch_id).order_by(Email.email_address)

        if search_query:
            search_pattern = f"%{search_query}%"
            query = query.filter(
                db.or_(
                    Email.email_address.ilike(search_pattern), 
                    Email.source_domain.ilike(search_pattern)
                )
            )

        emails = db.session.execute(query).scalars().all()
        
        return jsonify([e.to_dict() for e in emails]), 200

@app.route('/batches/<int:batch_id>', methods=['PATCH'])
def rename_batch(batch_id):
    """[RENAMING BATCHES] Updates the name of a specific batch."""
    data = request.json
    new_name = data.get('batch_name')

    if not new_name:
        return jsonify({"error": "Batch name is required"}), 400

    with app.app_context():
        batch = db.session.get(Batch, batch_id)
        if not batch:
            return jsonify({"error": "Batch not found"}), 404
        
        batch.name = new_name
        db.session.commit()
        
        return jsonify({"message": f"Batch {batch_id} renamed successfully", "new_name": new_name}), 200

@app.route('/emails/<int:email_id>', methods=['PATCH'])
def update_email(email_id):
    """[EDITING EMAILS] Updates the address and/or source domain of a specific email."""
    data = request.json
    new_address = data.get('email_address')
    new_domain = data.get('source_domain')

    if not new_address and not new_domain:
        return jsonify({"error": "No fields provided for update (email_address or source_domain)"}), 400

    with app.app_context():
        email = db.session.get(Email, email_id)
        if not email:
            return jsonify({"error": "Email not found"}), 404
        
        if new_address:
            email.email_address = new_address
        if new_domain:
            email.source_domain = new_domain
        
        db.session.commit()
        return jsonify(email.to_dict()), 200


@app.route('/emails/delete', methods=['DELETE'])
def delete_emails():
    """[DELETING SELECTED EMAILS] Deletes selected individual emails by a list of IDs."""
    data = request.json
    ids_to_delete = data.get('ids', [])

    if not ids_to_delete or not isinstance(ids_to_delete, list):
        return jsonify({"error": "A list of email IDs is required"}), 400

    try:
        with app.app_context():
            deleted_count = db.session.query(Email).filter(Email.id.in_(ids_to_delete)).delete(synchronize_session='fetch')
            db.session.commit()
            
            return jsonify({
                "message": f"Successfully deleted {deleted_count} email(s)",
                "deleted_count": deleted_count
            }), 200

    except Exception as e:
        db.session.rollback()
        print(f"Error deleting emails: {e}")
        return jsonify({"error": f"Failed to delete emails: {e}"}), 500

@app.route('/batches/delete', methods=['DELETE'])
def delete_batches():
    """[DELETING BATCHES] Deletes batches by a list of IDs and all associated emails."""
    data = request.json
    ids_to_delete = data.get('ids', [])

    if not ids_to_delete or not isinstance(ids_to_delete, list):
        return jsonify({"error": "A list of batch IDs is required"}), 400
    
    try:
        with app.app_context():
            batches_to_delete = db.session.query(Batch).filter(Batch.id.in_(ids_to_delete)).all()
            
            deleted_ids = []
            for batch in batches_to_delete:
                deleted_ids.append(batch.id)
                db.session.delete(batch)
            
            # The 'cascade="all, delete-orphan"' rule handles deleting the 'Email' records.
            db.session.commit()

            return jsonify({
                "message": f"Successfully deleted {len(deleted_ids)} batch(es).",
                "deleted_ids": deleted_ids
            }), 200

    except Exception as e:
        db.session.rollback()
        print(f"Error deleting batches: {e}")
        return jsonify({"error": f"Failed to delete batches: {e.__class__.__name__}."}), 500

@app.route('/batches/export/xlsx/<int:batch_id>', methods=['GET'])
def export_batch_xlsx(batch_id):
    """[EXPORTING XLSX] Generates and returns an XLSX file for a specific batch."""
    from io import BytesIO 
    
    with app.app_context():
        batch = db.session.get(Batch, batch_id)
        if not batch:
            return jsonify({"error": "Batch not found"}), 404

        emails = db.session.execute(
            db.select(Email).filter_by(batch_id=batch_id).order_by(Email.email_address)
        ).scalars().all()

        wb = Workbook()
        ws = wb.active
        ws.title = f"Batch_{batch_id}_Emails"

        headers = ["Email Address", "Source URL", "Source Domain"]
        ws.append(headers)

        seen_emails = set()
        for email in emails:
            # Simple de-duplication for the export
            if email.email_address not in seen_emails:
                ws.append([
                    email.email_address,
                    email.source_url,
                    email.source_domain
                ])
                seen_emails.add(email.email_address)

        output = BytesIO()
        wb.save(output)
        output.seek(0)

        download_name = f"{batch.name.replace(' ', '_').replace('/', '')}_Export_{datetime.utcnow().strftime('%Y%m%d')}.xlsx"
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=download_name
        )


if __name__ == '__main__':
    # This block is for local development only.
    # On Render, Gunicorn runs 'app:app'
    with app.app_context():
        db.create_all() 
    app.run(debug=True, port=5000)

