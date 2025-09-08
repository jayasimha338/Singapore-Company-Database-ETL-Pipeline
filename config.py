# config.py
# Configuration management for the ETL pipeline

import os
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class DatabaseConfig:
    """Database configuration"""
    path: str = "singapore_companies.db"
    backup_path: str = "backups/"
    timeout: int = 30

@dataclass
class ScrapingConfig:
    """Web scraping configuration"""
    request_timeout: int = 30
    retry_attempts: int = 3
    delay_between_requests: float = 0.5
    max_concurrent_requests: int = 5
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    headless_browser: bool = True
    
@dataclass
class LLMConfig:
    """LLM configuration"""
    model_name: str = "microsoft/DialoGPT-medium"
    max_tokens: int = 150
    temperature: float = 0.7
    device: str = "auto"  # auto, cpu, cuda
    batch_size: int = 4

@dataclass
class DataQualityConfig:
    """Data quality thresholds"""
    min_completeness_score: float = 50.0
    min_accuracy_score: float = 70.0
    fuzzy_match_threshold: int = 85

@dataclass
class ETLConfig:
    """ETL pipeline configuration"""
    target_company_count: int = 10000
    batch_size: int = 100
    max_workers: int = 4
    enable_llm_enrichment: bool = True
    enable_website_scraping: bool = True
    data_sources: List[str] = None
    
    def __post_init__(self):
        if self.data_sources is None:
            self.data_sources = [
                "acra",
                "bizfile", 
                "data_gov_sg",
                "company_websites"
            ]

class Config:
    """Main configuration class"""
    
    def __init__(self):
        self.database = DatabaseConfig()
        self.scraping = ScrapingConfig()
        self.llm = LLMConfig()
        self.data_quality = DataQualityConfig()
        self.etl = ETLConfig()
        
        # Load from environment variables
        self._load_from_env()
    
    def _load_from_env(self):
        """Load configuration from environment variables"""
        # Database config
        self.database.path = os.getenv("DB_PATH", self.database.path)
        
        # LLM config
        self.llm.model_name = os.getenv("LLM_MODEL", self.llm.model_name)
        self.llm.device = os.getenv("LLM_DEVICE", self.llm.device)
        
        # ETL config
        self.etl.target_company_count = int(
            os.getenv("TARGET_COMPANY_COUNT", self.etl.target_company_count)
        )
        self.etl.batch_size = int(os.getenv("BATCH_SIZE", self.etl.batch_size))

# Global configuration instance
config = Config()