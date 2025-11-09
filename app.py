# backend/app.py

from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
import requests
from bs4 import BeautifulSoup
import re
import os
from urllib.parse import urlparse, urljoin # Added urljoin for safety
import threading # For running the scraper in the background
import time # Used for small delays, if needed, but primarily for async structure

# --- Global Job Tracking ---
# Global dictionary to hold job status: {job_id: status_object}
SCRAPING_JOBS = {} 
JOB_COUNTER = 0

# --- UTILITY FUNCTIONS ---

def sanitize_url(raw_url):
    """Removes common prefixes and ensures a clean https:// prefix."""
    url = raw_url.strip().lower()
    
    # Strip common prefixes
    if url.startswith('https://'):
        url = url[8:]
    elif url.startswith('http://'):
        url = url[7:]
    if url.startswith('www.'):
        url = url[4:]
    
    # Re-add the clean HTTPS prefix
    return f"https://{url}"

def generate_crawl_list(sanitized_url):
    """Generates the base URL and two policy links from a sanitized domain."""
    # Use urlparse to safely get the root domain structure
    parsed = urlparse(sanitized_url)
    root_domain = f"{parsed.scheme}://{parsed.netloc}"
    
    # The three URLs to visit for this single domain
    urls_to_visit = [
        root_domain, 
        urljoin(root_domain, '/policies/contact-information'), # Safely joins paths
        urljoin(root_domain, '/policies/privacy-policy')
    ]
    return urls_to_visit

# --- FLASK AND DATABASE SETUP ---

app = Flask(__name__)
CORS(app)

# Database Configuration
basedir = os.path.abspath(os.path.dirname(__file__))
# Change DB configuration to use the production variable
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
# Your other configurations remain the same
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Database Model
class Email(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email_address = db.Column(db.String(120), unique=True, nullable=False)
    source_url = db.Column(db.String(500), nullable=False)

    def __repr__(self):
        return f'<Email {self.email_address}>'

# Initialize the Database
with app.app_context():
    db.create_all()

# --- SCRAPER THREAD WORKER ---

def _run_batch_scrape(job_id, raw_input_urls):
    global SCRAPING_JOBS
    
    # Initialize job status
    SCRAPING_JOBS[job_id]['status'] = 'RUNNING'
    
    raw_links = [link.strip() for link in raw_input_urls.split('\n') if link.strip()]
    total_domains = len(raw_links)
    
    for domain_index, raw_link in enumerate(raw_links):
        
        # Calculate progress based on domains processed
        SCRAPING_JOBS[job_id]['progress'] = int(((domain_index + 1) / total_domains) * 100)
        
        sanitized_domain = sanitize_url(raw_link)
        links_to_scrape = generate_crawl_list(sanitized_domain)
        
        # Scrape the three generated links
        for url_to_scrape in links_to_scrape:
            newly_saved_on_link = 0
            
            try:
                headers = {'User-Agent': 'Mozilla/5.0'}
                # Using a 10-second timeout
                response = requests.get(url_to_scrape, headers=headers, timeout=10)
                response.raise_for_status() 
                html_content = response.text

                email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                found_emails = set(re.findall(email_pattern, html_content))
                
                # IMPORTANT: Use app.app_context() for database ops in a separate thread
                with app.app_context():
                    for email_address in found_emails:
                        exists = db.session.query(Email.id).filter_by(email_address=email_address).first()
                        if not exists:
                            new_email = Email(email_address=email_address, source_url=url_to_scrape)
                            db.session.add(new_email)
                            SCRAPING_JOBS[job_id]['total_saved'] += 1 # Update global count
                            newly_saved_on_link += 1
                    
                    db.session.commit()
                
                # Update details (optional for frontend, but useful for debugging)
                SCRAPING_JOBS[job_id]['details'].append({
                    "url": url_to_scrape,
                    "status": "Success",
                    "emails_saved": newly_saved_on_link
                })

            except requests.exceptions.RequestException as e:
                # Log the failure but continue to the next link
                SCRAPING_JOBS[job_id]['details'].append({
                    "url": url_to_scrape,
                    "status": "Failed",
                    "error": "Timeout or connection error"
                })
                continue
            
    # Mark job as complete
    SCRAPING_JOBS[job_id]['status'] = 'COMPLETED'
    SCRAPING_JOBS[job_id]['progress'] = 100

# --- API ENDPOINTS ---

@app.route('/start_batch_scrape', methods=['POST'])
def start_batch_scrape():
    global JOB_COUNTER
    data = request.get_json()
    raw_input_urls = data.get('urls') 

    if not raw_input_urls:
        return jsonify({"error": "Missing 'urls' in request body"}), 400

    # 1. Assign a unique Job ID
    JOB_COUNTER += 1
    job_id = f"job-{JOB_COUNTER}"
    
    # 2. Initialize job status
    SCRAPING_JOBS[job_id] = {
        'status': 'QUEUED', 
        'progress': 0, 
        'total_saved': 0, 
        'details': [], 
        'domains_to_process': len([link.strip() for link in raw_input_urls.split('\n') if link.strip()])
    }

    # 3. Start the scraping in a separate thread
    thread = threading.Thread(target=_run_batch_scrape, args=(job_id, raw_input_urls))
    thread.start()
    
    # 4. Immediately return the Job ID to the frontend
    return jsonify({"job_id": job_id, "message": "Scraping job started."}), 202


@app.route('/status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    job_status = SCRAPING_JOBS.get(job_id)
    if not job_status:
        return jsonify({"error": "Job not found"}), 404
        
    return jsonify(job_status)


@app.route('/emails', methods=['GET'])
def get_all_emails():
    # Query the database for all saved emails
    emails = db.session.execute(db.select(Email).order_by(Email.id)).scalars()
    
    # Convert the SQLAlchemy objects into a list of dictionaries for JSON serialization
    email_list = []
    for email in emails:
        email_list.append({
            'email_address': email.email_address,
            'source_url': email.source_url
        })

    return jsonify(email_list)


if __name__ == '__main__':

    app.run(debug=True)

