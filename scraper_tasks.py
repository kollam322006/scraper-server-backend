# scraper_tasks.py

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship 
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, urljoin
import time
from rq.job import Job # NEW IMPORT

# Initialize SQLAlchemy
db = SQLAlchemy()

# --- NEW BATCH MODEL ---
class Batch(db.Model):
    id = Column(Integer, primary_key=True)
    batch_name = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(20), default='PENDING') # PENDING, RUNNING, COMPLETED, FAILED
    total_domains = Column(Integer, default=0)
    
    # Relationship to link Emails to this Batch
    emails = relationship("Email", back_populates="batch", cascade="all, delete-orphan")

# --- UPDATED EMAIL MODEL ---
class Email(db.Model):
    id = Column(Integer, primary_key=True)
    email_address = Column(String(120), nullable=False) # Removed unique constraint for simplicity
    source_url = Column(String(255), nullable=False)
    source_domain_name = Column(String(150), nullable=True) # NEW: For UI
    
    # Foreign key link to the Batch model
    batch_id = Column(Integer, ForeignKey('batch.id'), nullable=False) 
    batch = relationship("Batch", back_populates="emails") 

    def __repr__(self):
        return f'<Email {self.email_address}>'

# --- Scraping Logic ---

EMAIL_REGEX = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

def extract_emails_from_text(text):
    """Finds all unique emails in a given block of text."""
    return set(re.findall(EMAIL_REGEX, text))

def scrape_url_for_emails(url, session, batch_id):
    """Scrapes a single URL and saves any unique emails found to the database."""
    print(f"Scraping: {url}")
    found_emails_count = 0
    parsed_url = urlparse(url)
    source_domain = parsed_url.netloc
    
    try:
        time.sleep(0.5) 
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
            'Accept-Language': 'en-US,en;q=0.5'
        }
        
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status() 
        
        soup = BeautifulSoup(response.text, 'lxml')
        page_text = soup.get_text()
        unique_emails = extract_emails_from_text(page_text)
        
        for email_address in unique_emails:
            new_email = Email(
                email_address=email_address, 
                source_url=url, 
                source_domain_name=source_domain,
                batch_id=batch_id # Attach to the current batch
            )
            db.session.add(new_email)
            found_emails_count += 1
                
        db.session.commit()
        
    except requests.exceptions.RequestException as e:
        print(f"Error accessing {url}: {e}")
        db.session.rollback()
    except Exception as e:
        print(f"General error on {url}: {e}")
        db.session.rollback()
        
    return found_emails_count

def run_batch_scrape_task(job_id, raw_urls_string, batch_name):
    """Main task function executed by the RQ Worker."""
    
    from app import app
    
    domains = [u.strip() for u in raw_urls_string.split('\n') if u.strip()]
    total_domains_to_check = len(domains) * 3 # 3 paths per domain
    
    # Target paths relative to the domain root
    paths_to_check = ['/', '/policies/contact-information', '/policies/privacy-policy']
    
    total_saved_emails = 0
    domains_processed = 0
    job = Job.fetch(job_id, connection=app.config.get('REDIS_CONNECTION')) # Fetch job instance
    
    with app.app_context():
        # --- 1. Create and Initialize Batch ---
        new_batch = Batch(batch_name=batch_name, status='RUNNING', total_domains=len(domains))
        db.session.add(new_batch)
        db.session.commit()
        batch_id = new_batch.id
        
        with requests.Session() as session:
            for domain in domains:
                
                if not domain.startswith(('http://', 'https://')):
                    domain = f'https://{domain}'

                parsed_domain = urlparse(domain)
                base_url = f"{parsed_domain.scheme}://{parsed_domain.netloc}"

                print(f"Starting check for domain: {base_url}")
                
                for path in paths_to_check:
                    target_url = urljoin(base_url, path)
                    
                    if target_url.startswith(base_url):
                        saved_count = scrape_url_for_emails(target_url, session, batch_id)
                        total_saved_emails += saved_count
                
                domains_processed += 1
                
                # --- Update Progress (Polling) ---
                job.meta['domains_processed'] = domains_processed
                job.meta['total_domains'] = len(domains)
                job.save_meta()

        # --- 2. Finalize Batch Status ---
        Batch.query.filter_by(id=batch_id).update({'status': 'COMPLETED'})
        db.session.commit()
            
    return f"Batch '{batch_name}' finished. Saved {total_saved_emails} unique email(s)."
