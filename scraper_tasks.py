# scraper_tasks.py

import os
import re
import requests
from urllib.parse import urlparse, urljoin 

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

# --- FLASK AND DATABASE SETUP ---
# The worker needs a minimal Flask app context and the DB URL
app = Flask(__name__)
# Get the DB URL from the environment (Render's linked PostgreSQL)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Database Model (Must be defined here for the worker)
class Email(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email_address = db.Column(db.String(120), unique=True, nullable=False)
    source_url = db.Column(db.String(500), nullable=False)
    
# --- UTILITY FUNCTIONS ---

def sanitize_url(raw_url):
    """Removes common prefixes and ensures a clean https:// prefix."""
    url = raw_url.strip().lower()
    
    if url.startswith('https://'):
        url = url[8:]
    elif url.startswith('http://'):
        url = url[7:]
    if url.startswith('www.'):
        url = url[4:]
    
    return f"https://{url}"

def generate_crawl_list(sanitized_url):
    """Generates the base URL and two policy links from a sanitized domain."""
    parsed = urlparse(sanitized_url)
    root_domain = f"{parsed.scheme}://{parsed.netloc}"
    
    urls_to_visit = [
        root_domain, 
        urljoin(root_domain, '/policies/contact-information'),
        urljoin(root_domain, '/policies/privacy-policy')
    ]
    return urls_to_visit

# --- SCRAPER TASK WORKER ---

def run_batch_scrape_task(raw_input_urls):
    """The main scraping function, now runnable by RQ."""
    
    # All database operations MUST be within the app_context() when run by RQ
    
    raw_links = [link.strip() for link in raw_input_urls.split('\n') if link.strip()]
    
    emails_saved_count = 0
    
    # Initialize DB tables if they don't exist (important for the first run)
    with app.app_context():
        db.create_all()

    for raw_link in raw_links:
        sanitized_domain = sanitize_url(raw_link)
        links_to_scrape = generate_crawl_list(sanitized_domain)
        
        for url_to_scrape in links_to_scrape:
            try:
                headers = {'User-Agent': 'Mozilla/5.0'}
                response = requests.get(url_to_scrape, headers=headers, timeout=15) # Increased timeout
                response.raise_for_status() 
                html_content = response.text

                email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                found_emails = set(re.findall(email_pattern, html_content))
                
                with app.app_context():
                    for email_address in found_emails:
                        # Use select statement for robust checking
                        exists = db.session.execute(
                            db.select(Email).filter_by(email_address=email_address)
                        ).scalar_one_or_none()
                        
                        if not exists:
                            new_email = Email(email_address=email_address, source_url=url_to_scrape)
                            db.session.add(new_email)
                            emails_saved_count += 1
                    
                    db.session.commit()
            
            except requests.exceptions.RequestException as e:
                # Use print() or logging in RQ task to see errors in worker logs
                print(f"ERROR: Failed to scrape {url_to_scrape}. Error: {e}")
            except Exception as e:
                print(f"CRITICAL ERROR: Unexpected error while processing {url_to_scrape}. Error: {e}")
                
    # Return a final message that can be retrieved by the API's /status endpoint
    return f"Scraping complete. Total unique emails saved: {emails_saved_count}"
