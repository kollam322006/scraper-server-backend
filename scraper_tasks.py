# scraper_tasks.py (Database Model and Scraping Logic)

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, urljoin
import time

# Initialize SQLAlchemy
db = SQLAlchemy()

# --- Database Model ---
class Email(db.Model):
    id = Column(Integer, primary_key=True)
    email_address = Column(String(120), unique=True, nullable=False)
    source_url = Column(String(255), nullable=False)

    def __repr__(self):
        return f'<Email {self.email_address}>'

# --- Scraping Logic ---

# Regex pattern to find emails
EMAIL_REGEX = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

def extract_emails_from_text(text):
    """Finds all unique emails in a given block of text."""
    return set(re.findall(EMAIL_REGEX, text))

def scrape_url_for_emails(url, session):
    """Scrapes a single URL and saves any unique emails found to the database."""
    print(f"Scraping: {url}")
    found_emails_count = 0
    try:
        # Simple backoff to avoid hammering sites
        time.sleep(0.5) 
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
            'Accept-Language': 'en-US,en;q=0.5'
        }
        
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status() # Raise exception for 4xx/5xx errors
        
        soup = BeautifulSoup(response.text, 'lxml')
        page_text = soup.get_text()
        
        unique_emails = extract_emails_from_text(page_text)
        
        # Save unique emails to the database
        for email_address in unique_emails:
            # Check if email already exists before inserting
            existing_email = db.session.execute(
                db.select(Email).filter_by(email_address=email_address)
            ).scalar_one_or_none()
            
            if not existing_email:
                new_email = Email(email_address=email_address, source_url=url)
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

def run_batch_scrape_task(raw_urls_string):
    """Main task function executed by the RQ Worker."""
    
    # Needs to be imported inside the worker function to avoid circular import issues
    from app import app
    
    # Process input: clean and normalize domains/URLs
    domains = [u.strip() for u in raw_urls_string.split('\n') if u.strip()]
    
    # Target paths relative to the domain root
    paths_to_check = [
        '/',
        '/policies/contact-information',
        '/policies/privacy-policy'
    ]
    
    total_saved_emails = 0
    
    with app.app_context():
        with requests.Session() as session:
            
            for domain in domains:
                
                # Normalize domain/link to start with http or https if needed
                if not domain.startswith(('http://', 'https://')):
                    domain = f'https://{domain}'

                parsed_domain = urlparse(domain)
                base_url = f"{parsed_domain.scheme}://{parsed_domain.netloc}"

                print(f"Starting check for domain: {base_url}")
                
                for path in paths_to_check:
                    target_url = urljoin(base_url, path)
                    
                    # Only scrape the base domain or the specific paths under it
                    if target_url.startswith(base_url):
                        saved_count = scrape_url_for_emails(target_url, session)
                        total_saved_emails += saved_count
            
    return f"Batch scrape finished. Saved {total_saved_emails} unique email(s)."
