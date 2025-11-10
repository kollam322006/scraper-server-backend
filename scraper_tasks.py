# scraper_tasks.py

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship # <-- NEW IMPORT
from datetime import datetime
# ... (rest of imports) ...

db = SQLAlchemy()

# --- NEW BATCH MODEL ---
class Batch(db.Model):
    id = Column(Integer, primary_key=True)
    batch_name = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(20), default='PENDING') # PENDING, RUNNING, COMPLETED, FAILED
    
    # Relationship to link Emails to this Batch
    emails = relationship("Email", back_populates="batch", cascade="all, delete-orphan")

# --- UPDATED EMAIL MODEL ---
class Email(db.Model):
    id = Column(Integer, primary_key=True)
    email_address = Column(String(120), unique=True, nullable=False)
    source_url = Column(String(255), nullable=False)
    
    # Foreign key link to the Batch model
    batch_id = Column(Integer, ForeignKey('batch.id'), nullable=False) # <-- NEW FIELD
    batch = relationship("Batch", back_populates="emails") # <-- NEW RELATIONSHIP

    def __repr__(self):
        return f'<Email {self.email_address}>'

# NOTE: You MUST commit these schema changes and run db.create_all() again (or use migrations if your app is larger)
