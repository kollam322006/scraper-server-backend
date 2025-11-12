import os
import redis
import json
import time
from flask import Flask, jsonify, request, send_file
from flask_sqlalchemy import SQLAlchemy
from rq import Queue
from rq.job import Job
from typing import List, Optional
from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, DateTime, Text, Boolean, func, distinct
from openpyxl import Workbook
from urllib.parse import urlparse
from flask_cors import CORS # Needed for front-end communication

# --- Database and Queue Configuration ---

# 1. Database Configuration
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# 2. Redis Configuration
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
redis_conn = redis.from_url(REDIS_URL)

# 3. Flask App Initialization
app = Flask(__name__)
CORS(app) # Enable CORS for frontend

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

    # CRITICAL FIX: cascade="all, delete-orphan"
    # This automatically deletes associated emails when the batch is deleted.
    emails: Mapped[List["Email"]] = relationship("Email", back_populates="batch", cascade="all, delete-orphan")
    
    # Tracking for progress
    domains_to_scrape: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    total_domains: Mapped[int] = mapped_column(Integer, default=0)
    domains_processed: Mapped[int] = mapped_column(Integer, default=0)
    
    def __repr__(self):
        return f"<Batch {self.id}: {self.name}>"

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
        db.UniqueConstraint('batch_id', 'email_address', name='uix_email_batch'), 
    )

# --- Scraper Task Helper Functions ---

def generate_common_contact_urls(url: str) -> List[str]:
    if not urlparse(url).scheme:
        url = 'https://' + url
    
    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    
    suffixes = [
        "/contact", "/contact-us", "/about", "/policies/privacy-policy", 
        "/info", "/support", "/privacy", "/contact-information", 
        "/help", "/cdn/shop/pages/contact"
    ]
    
    urls = [base_url] + [base_url + suffix for suffix in suffixes]
    return list(set(urls))

def simulate_scrape_emails(url: str) -> List[str]:
    parsed = urlparse(url)
    domain = parsed.netloc.replace('www.', '')
    
    emails = []
    
    if 'example' in domain:
        emails.extend(['info@example.com', 'sales@example.com'])
    if 'store' in domain:
        emails.extend(['support@somestore.com', f'help@{domain}'])

    if 'privacy' in url:
        emails.append(f'privacy@{domain.split(".")[0]}.com')
    if 'contact' in url:
        emails.append(f'support@{domain}')
    
    if domain:
        emails.append(f"contact_sim_{abs(hash(domain)) % 1000}@{domain}")

    return list(set(emails))

# --- Scraper Task ---

def scrape_task(batch_id: int):
    with app.app_context():
        batch = db.session.get(Batch, batch_id)
        if not batch:
            return

        batch.status = 'RUNNING'
        db.session.commit()
        
        try:
            urls_to_scrape_list = json.loads(batch.domains_to_scrape)
            batch.total_domains = len(urls_to_scrape_list)
            db.session.commit()

            total_emails_saved = 0
            
            for i, base_url_input in enumerate(urls_to_scrape_list):
                base_url_input = base_url_input.strip()
                if not base_url_input:
                    continue

                expanded_urls = generate_common_contact_urls(base_url_input)
                base_domain_url_to_save = expanded_urls[0] 
                
                for url_to_scrape in expanded_urls:
                    found_emails = simulate_scrape_emails(url_to_scrape)
                    
                    for email_address in found_emails:
                        email_address = email_address.lower()
                        
                        exists = db.session.execute(
                            db.select(Email).filter_by(batch_id=batch_id, 
                                                      email_address=email_address) 
                        ).scalar_one_or_none()
                        
                        if not exists:
                            parsed_url = urlparse(base_domain_url_to_save)
                            source_domain = parsed_url.netloc.replace('www.', '')
                            
                            new_email = Email(
                                batch_id=batch_id,
                                email_address=email_address,
                                source_url=base_domain_url_to_save,
                                source_domain=source_domain
                            )
                            db.session.add(new_email)
                            total_emails_saved += 1

                    db.session.commit()
                
                batch.domains_processed += 1
                db.session.commit()

            batch.status = 'COMPLETED'
            db.session.commit()
            return f"Batch scrape finished. Saved {total_emails_saved} unique email(s)."

        except Exception as e:
            db.session.rollback()
            print(f"Error during scraping for Batch {batch_id}: {e}")
            batch.status = 'FAILED'
            db.session.commit()
            raise

# --- API Endpoints ---

@app.route('/create_tables', methods=['POST'])
def create_tables_endpoint():
    with app.app_context():
        db.create_all()
    return jsonify({"message": "Tables created or already exist."}), 201

@app.route('/start_batch_scrape', methods=['POST'])
def start_batch_scrape():
    data = request.json
    urls_input = data.get('urls', '')
    batch_name = data.get('batch_name', f'Batch {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}')

    if not urls_input:
        return jsonify({"error": "No URLs provided"}), 400

    urls_list = [url.strip() for url in urls_input.split('\n') if url.strip()]

    with app.app_context():
        new_batch = Batch(
            name=batch_name, 
            status='PENDING',
            domains_to_scrape=json.dumps(urls_list),
            total_domains=len(urls_list)
        )
        db.session.add(new_batch)
        db.session.commit()
        batch_id = new_batch.id
        
        job = q.enqueue(scrape_task, batch_id, job_timeout='1h') 
        
        new_batch.job_id = job.id
        db.session.commit()

        return jsonify({
            "message": "Scraping job queued",
            "batch_id": batch_id,
            "job_id": job.id
        }), 202

@app.route('/status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    db_status = 'QUEUED'
    total_domains = 0
    domains_processed = 0
    progress_percent = 0
    
    with app.app_context():
        batch = db.session.execute(db.select(Batch).filter_by(job_id=job_id)).scalar_one_or_none()
        
        if batch:
            total_domains = batch.total_domains
            domains_processed = batch.domains_processed
            progress_percent = int((domains_processed / total_domains) * 100) if total_domains > 0 else 0
            db_status = batch.status
        
        if db_status not in ['COMPLETED', 'FAILED']:
            try:
                job = Job.fetch(job_id, connection=redis_conn)
                rq_status = job.get_status()
                # Use RQ status if DB is still showing PENDING/RUNNING, otherwise trust DB for final states
                db_status = rq_status 
            except:
                # Job not found in Redis, if DB status wasn't final, assume failure
                if db_status not in ['COMPLETED', 'FAILED']:
                    db_status = 'LOST'

    return jsonify({
        "status": db_status,
        "progress_percent": progress_percent,
        "domains_processed": domains_processed,
        "total_domains": total_domains,
        "result": "Processing..." if db_status not in ['COMPLETED', 'FAILED'] else "Job finished."
    }), 200

@app.route('/batches', methods=['GET'])
def get_batches():
    with app.app_context():
        batches = db.session.execute(
            db.select(Batch).order_by(Batch.created_at.desc())
        ).scalars().all()

        batch_list = []
        for batch in batches:
            email_count = db.session.scalar(
                db.select(func.count(Email.id)).filter_by(batch_id=batch.id)
            )
            
            batch_list.append({
                "id": batch.id,
                "name": batch.name,
                "status": batch.status,
                "created_at": batch.created_at.isoformat(),
                "email_count": email_count,
                "job_id": batch.job_id,
                "domains_processed": batch.domains_processed,
                "total_domains": batch.total_domains,
            })

        return jsonify(batch_list), 200

@app.route('/batches/<int:batch_id>/emails', methods=['GET'])
def get_batch_emails(batch_id):
    search_query = request.args.get('q')
    
    with app.app_context():
        batch = db.session.get(Batch, batch_id)
        if not batch:
            return jsonify({"error": "Batch not found"}), 404

        query = db.select(Email).filter_by(batch_id=batch_id).order_by(Email.email_address)

        if search_query:
            search_pattern = f"%{search_query}%"
            query = query.filter(
                (Email.email_address.ilike(search_pattern)) | 
                (Email.source_domain.ilike(search_pattern))
            )

        emails = db.session.execute(query).scalars().all()
        
        email_list = [{
            "id": email.id,
            "email_address": email.email_address,
            "source_url": email.source_url,
            "source_domain": email.source_domain,
            "created_at": email.created_at.isoformat()
        } for email in emails]

        return jsonify(email_list), 200

@app.route('/batches/<int:batch_id>', methods=['PATCH'])
def rename_batch(batch_id):
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

@app.route('/emails/delete', methods=['DELETE'])
def delete_emails():
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
            
            db.session.commit()

            return jsonify({
                "message": f"Successfully deleted {len(deleted_ids)} batch(es). Associated emails were also removed.",
                "deleted_ids": deleted_ids
            }), 200

    except Exception as e:
        db.session.rollback()
        print(f"Error deleting batches: {e}")
        return jsonify({"error": f"Failed to delete batches: {e.__class__.__name__}. This may require a database reset if the table structure is outdated."}), 500

@app.route('/batches/export/xlsx/<int:batch_id>', methods=['GET'])
def export_batch_xlsx(batch_id):
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
    with app.app_context():
        db.create_all() 
    pass
