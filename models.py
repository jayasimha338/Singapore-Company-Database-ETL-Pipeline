# models.py
# Data models for the Singapore Company ETL Pipeline

from dataclasses import dataclass
from typing import Optional
from datetime import datetime

@dataclass
class CompanyData:
    """Data class for company information"""
    # Core identifiers
    uen: Optional[str] = None
    company_name: Optional[str] = None
    website: Optional[str] = None
    
    # Location information
    hq_country: Optional[str] = None
    no_of_locations_in_singapore: Optional[int] = None
    
    # Social media
    linkedin: Optional[str] = None
    facebook: Optional[str] = None
    instagram: Optional[str] = None
    
    # Business information
    industry: Optional[str] = None
    number_of_employees: Optional[int] = None
    company_size: Optional[str] = None
    
    # Financial information
    is_it_delisted: Optional[bool] = None
    stock_exchange_code: Optional[str] = None
    revenue: Optional[str] = None
    founding_year: Optional[int] = None
    
    # Contact information
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    
    # Business details
    products_offered: Optional[str] = None
    services_offered: Optional[str] = None
    keywords: Optional[str] = None
    
    # Metadata
    source_of_data: Optional[str] = None
    extraction_timestamp: Optional[str] = None
    
    def __post_init__(self):
        """Set extraction timestamp if not provided"""
        if self.extraction_timestamp is None:
            self.extraction_timestamp = datetime.now().isoformat()
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
    def calculate_completeness_score(self) -> float:
        """Calculate data completeness score (0-100)"""
        total_fields = len(self.__dataclass_fields__)
        populated_fields = sum(1 for v in self.__dict__.values() if v is not None)
        return (populated_fields / total_fields) * 100

@dataclass
class DataSource:
    """Data source tracking"""
    source_name: str
    source_url: Optional[str] = None
    extraction_method: str = "unknown"
    confidence_score: float = 1.0
    extraction_timestamp: Optional[str] = None
    
    def __post_init__(self):
        if self.extraction_timestamp is None:
            self.extraction_timestamp = datetime.now().isoformat()

@dataclass
class ValidationResult:
    """Validation result for data quality"""
    is_valid: bool
    field_name: str
    error_message: Optional[str] = None
    confidence_score: float = 1.0