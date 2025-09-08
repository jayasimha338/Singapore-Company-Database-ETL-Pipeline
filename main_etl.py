# main_etl.py
# Main ETL pipeline orchestrator for Singapore Company Database

import logging
import time
from datetime import datetime
from typing import List, Dict
import concurrent.futures
from pathlib import Path

# Import our modules
from config import config
from models import CompanyData
from database import DatabaseManager
from dataextractor import SGDataExtractor
from web_scraper import WebScraper
from llm_processor import LLMProcessor
from entity_matcher import EntityMatcher

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SGCompanyETL:
    """Main ETL pipeline orchestrator"""
    
    def __init__(self):
        logger.info("Initializing Singapore Company ETL Pipeline...")
        
        # Initialize components
        self.db_manager = DatabaseManager(config.database.path)
        self.data_extractor = SGDataExtractor()
        self.web_scraper = WebScraper(
            headless=config.scraping.headless_browser,
            use_selenium=config.etl.enable_website_scraping
        )
        self.llm_processor = LLMProcessor(config.llm.model_name) if config.etl.enable_llm_enrichment else None
        self.entity_matcher = EntityMatcher(config.data_quality.fuzzy_match_threshold)
        
        # Performance tracking
        self.stats = {
            'start_time': None,
            'end_time': None,
            'companies_extracted': 0,
            'companies_processed': 0,
            'companies_loaded': 0,
            'websites_scraped': 0,
            'llm_enrichments': 0,
            'duplicates_removed': 0
        }
        
        logger.info("ETL Pipeline initialized successfully")
    
    def run_pipeline(self, target_count: int = None) -> Dict:
        """Run the complete ETL pipeline"""
        if target_count is None:
            target_count = config.etl.target_company_count
        
        self.stats['start_time'] = datetime.now()
        logger.info(f"Starting ETL Pipeline with target count: {target_count}")
        
        try:
            # Phase 1: Extract
            logger.info("=" * 50)
            logger.info("PHASE 1: DATA EXTRACTION")
            logger.info("=" * 50)
            raw_companies = self.extract_phase(target_count)
            self.stats['companies_extracted'] = len(raw_companies)
            
            # Phase 2: Transform
            logger.info("=" * 50)
            logger.info("PHASE 2: DATA TRANSFORMATION")
            logger.info("=" * 50)
            processed_companies = self.transform_phase(raw_companies)
            self.stats['companies_processed'] = len(processed_companies)
            
            # Phase 3: Load
            logger.info("=" * 50)
            logger.info("PHASE 3: DATA LOADING")
            logger.info("=" * 50)
            loaded_count = self.load_phase(processed_companies)
            self.stats['companies_loaded'] = loaded_count
            
            # Phase 4: Report
            self.stats['end_time'] = datetime.now()
            self.generate_final_report()
            
            logger.info("ETL Pipeline completed successfully!")
            return self.stats
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
            raise
        
        finally:
            self.cleanup()
    
    def extract_phase(self, target_count: int) -> List[Dict]:
        """Phase 1: Extract company data from various sources"""
        logger.info("Starting data extraction phase...")
        
        all_companies = []
        
        # Extract from government sources
        logger.info("Extracting from Singapore government sources...")
        gov_companies = self.data_extractor.extract_companies_from_data_gov(
            limit=target_count // 2
        )
        all_companies.extend(gov_companies)
        
        # Extract from CSV sources
        logger.info("Extracting from CSV sources...")
        csv_companies = self.data_extractor.extract_companies_from_csv_sources()
        all_companies.extend(csv_companies)
        
        # Limit to target count
        if len(all_companies) > target_count:
            all_companies = all_companies[:target_count]
        
        logger.info(f"Extracted {len(all_companies)} companies from government sources")
        
        # Enrich with website data if enabled
        if config.etl.enable_website_scraping:
            all_companies = self.enrich_with_website_data(all_companies)
        
        return all_companies
    
    def enrich_with_website_data(self, companies: List[Dict]) -> List[Dict]:
        """Enrich companies with website data using concurrent processing"""
        logger.info("Enriching companies with website data...")
        
        # Limit concurrent website scraping to avoid being blocked
        max_websites_to_scrape = min(len(companies), 200)  # Reasonable limit for demo
        companies_to_scrape = companies[:max_websites_to_scrape]
        
        def scrape_single_company(company_data):
            try:
                company_name = company_data.get('company_name', '')
                
                # Find or generate website URL
                website = company_data.get('website')
                if not website:
                    website = self.data_extractor.search_company_websites(company_name)
                
                if website:
                    company_data['website'] = website
                    
                    # Scrape website for additional data
                    scraped_data = self.web_scraper.scrape_company_website(website)
                    
                    # Merge scraped data
                    for key, value in scraped_data.items():
                        if value and not company_data.get(key):
                            company_data[key] = value
                    
                    self.stats['websites_scraped'] += 1
                    
                    # Add delay to be respectful
                    time.sleep(config.scraping.delay_between_requests)
                
                return company_data
                
            except Exception as e:
                logger.error(f"Error scraping website for {company_name}: {e}")
                return company_data
        
        # Process companies with limited concurrency
        with concurrent.futures.ThreadPoolExecutor(max_workers=config.etl.max_workers) as executor:
            # Submit jobs in batches
            batch_size = config.etl.batch_size
            enriched_companies = []
            
            for i in range(0, len(companies_to_scrape), batch_size):
                batch = companies_to_scrape[i:i + batch_size]
                
                logger.info(f"Processing website batch {i//batch_size + 1} ({len(batch)} companies)")
                
                # Submit batch jobs
                future_to_company = {
                    executor.submit(scrape_single_company, company): company 
                    for company in batch
                }
                
                # Collect results
                for future in concurrent.futures.as_completed(future_to_company):
                    try:
                        enriched_company = future.result(timeout=60)
                        enriched_companies.append(enriched_company)
                    except Exception as e:
                        original_company = future_to_company[future]
                        logger.error(f"Failed to process company {original_company.get('company_name')}: {e}")
                        enriched_companies.append(original_company)
        
        # Add remaining companies that weren't scraped
        enriched_companies.extend(companies[max_websites_to_scrape:])
        
        logger.info(f"Website enrichment completed. Scraped {self.stats['websites_scraped']} websites")
        return enriched_companies
    
    def transform_phase(self, companies: List[Dict]) -> List[CompanyData]:
        """Phase 2: Transform and enrich company data"""
        logger.info("Starting data transformation phase...")
        
        # Step 1: Entity matching and deduplication
        logger.info("Performing entity matching and deduplication...")
        original_count = len(companies)
        deduplicated_companies = self.entity_matcher.fuzzy_match_companies(companies)
        self.stats['duplicates_removed'] = original_count - len(deduplicated_companies)
        
        logger.info(f"Removed {self.stats['duplicates_removed']} duplicates")
        
        # Step 2: LLM enrichment
        if self.llm_processor and config.etl.enable_llm_enrichment:
            logger.info("Performing LLM enrichment...")
            deduplicated_companies = self.llm_enrichment(deduplicated_companies)
        
        # Step 3: Convert to CompanyData objects
        logger.info("Converting to standardized format...")
        company_objects = []
        
        for company_dict in deduplicated_companies:
            try:
                # Clean and standardize data
                cleaned_data = self.clean_company_data(company_dict)
                
                # Create CompanyData object
                company_obj = CompanyData(**cleaned_data)
                company_objects.append(company_obj)
                
            except Exception as e:
                logger.error(f"Error converting company {company_dict.get('company_name', 'Unknown')}: {e}")
        
        logger.info(f"Transformation completed. Processed {len(company_objects)} companies")
        return company_objects
    
    def llm_enrichment(self, companies: List[Dict]) -> List[Dict]:
        """Enrich companies using LLM"""
        enriched_companies = []
        
        for i, company in enumerate(companies):
            try:
                if i % 50 == 0:
                    logger.info(f"LLM enrichment progress: {i}/{len(companies)}")
                
                enriched_company = self.llm_processor.enhance_company_data(company)
                enriched_companies.append(enriched_company)
                self.stats['llm_enrichments'] += 1
                
            except Exception as e:
                logger.error(f"LLM enrichment failed for {company.get('company_name')}: {e}")
                enriched_companies.append(company)
        
        logger.info(f"LLM enrichment completed for {self.stats['llm_enrichments']} companies")
        return enriched_companies
    
    def clean_company_data(self, company_dict: Dict) -> Dict:
        """Clean and standardize company data"""
        cleaned = {}
        
        # Map and clean fields
        field_mapping = {
            'uen': 'uen',
            'entity_name': 'company_name',
            'company_name': 'company_name',
            'reg_street_name': None,  # Skip for now
            'reg_postal_code': None,  # Skip for now
            'primary_ssic_description': 'industry',
            'website': 'website',
            'linkedin': 'linkedin',
            'facebook': 'facebook',
            'instagram': 'instagram',
            'contact_email': 'contact_email',
            'contact_phone': 'contact_phone',
            'founding_year': 'founding_year',
            'number_of_employees': 'number_of_employees',
            'company_size': 'company_size',
            'services_offered': 'services_offered',
            'products_offered': 'products_offered',
            'keywords': 'keywords',
            'source': 'source_of_data'
        }
        
        for source_field, target_field in field_mapping.items():
            if target_field and source_field in company_dict:
                value = company_dict[source_field]
                if value:
                    cleaned[target_field] = self.clean_field_value(target_field, value)
        
        # Set defaults
        cleaned['hq_country'] = 'Singapore'
        
        return cleaned
    
    def clean_field_value(self, field_name: str, value) -> any:
        """Clean individual field values"""
        if value is None:
            return None
        
        # Convert to string for text processing
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
        
        # Field-specific cleaning
        if field_name in ['founding_year', 'number_of_employees']:
            try:
                return int(value)
            except (ValueError, TypeError):
                return None
        
        elif field_name in ['website', 'linkedin', 'facebook', 'instagram']:
            # Ensure URLs have proper protocol
            if isinstance(value, str) and value:
                if not value.startswith(('http://', 'https://')):
                    value = 'https://' + value
                return value
        
        elif field_name == 'uen':
            # Clean UEN format
            if isinstance(value, str):
                return value.upper().strip()
        
        return value
    
    def load_phase(self, companies: List[CompanyData]) -> int:
        """Phase 3: Load data into database"""
        logger.info("Starting data loading phase...")
        
        try:
            # Batch insert for better performance
            batch_size = config.etl.batch_size
            total_loaded = 0
            
            for i in range(0, len(companies), batch_size):
                batch = companies[i:i + batch_size]
                loaded_count = self.db_manager.insert_companies_batch(batch)
                total_loaded += loaded_count
                
                logger.info(f"Loaded batch {i//batch_size + 1}: {loaded_count} companies")
            
            logger.info(f"Data loading completed. Loaded {total_loaded} companies")
            return total_loaded
            
        except Exception as e:
            logger.error(f"Error in load phase: {e}")
            raise
    
    def generate_final_report(self):
        """Generate final ETL report"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        # Get database coverage report
        coverage_report = self.db_manager.get_data_coverage_report()
        
        print("\n" + "=" * 60)
        print("SINGAPORE COMPANY ETL PIPELINE - FINAL REPORT")
        print("=" * 60)
        print(f"Total Runtime: {duration:.2f} seconds")
        print(f"Companies Extracted: {self.stats['companies_extracted']}")
        print(f"Companies Processed: {self.stats['companies_processed']}")
        print(f"Companies Loaded: {self.stats['companies_loaded']}")
        print(f"Websites Scraped: {self.stats['websites_scraped']}")
        print(f"LLM Enrichments: {self.stats['llm_enrichments']}")
        print(f"Duplicates Removed: {self.stats['duplicates_removed']}")
        
        if coverage_report:
            print(f"\nDATA COVERAGE:")
            coverage = coverage_report.get('coverage', {})
            print(f"Website Coverage: {coverage.get('website_coverage', 0):.1f}%")
            print(f"LinkedIn Coverage: {coverage.get('linkedin_coverage', 0):.1f}%")
            print(f"Email Coverage: {coverage.get('email_coverage', 0):.1f}%")
            print(f"Industry Coverage: {coverage.get('industry_coverage', 0):.1f}%")
            print(f"Average Quality Score: {coverage.get('avg_quality_score', 0):.1f}%")
            
            print(f"\nTOP 5 INDUSTRIES:")
            for industry in coverage_report.get('top_industries', []):
                print(f"  {industry['industry']}: {industry['company_count']} companies")
        
        print("=" * 60)
    
    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up resources...")
        
        try:
            if self.web_scraper:
                self.web_scraper.close()
            
            if self.llm_processor:
                self.llm_processor.cleanup()
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def run_pipeline_dry_run(self, target_count: int = 100):
        """Run pipeline in dry-run mode (no database writes)"""
        logger.info("Running ETL Pipeline in DRY-RUN mode...")
        
        # Limit to small number for testing
        target_count = min(target_count, 100)
        
        # Extract
        companies = self.extract_phase(target_count)
        logger.info(f"Would extract {len(companies)} companies")
        
        # Transform
        processed_companies = self.transform_phase(companies[:10])  # Limit for dry run
        logger.info(f"Would process {len(processed_companies)} companies")
        
        # Show sample data
        if processed_companies:
            sample = processed_companies[0]
            print(f"\nSample processed company:")
            print(f"  Name: {sample.company_name}")
            print(f"  UEN: {sample.uen}")
            print(f"  Industry: {sample.industry}")
            print(f"  Website: {sample.website}")
            print(f"  Completeness: {sample.calculate_completeness_score():.1f}%")
        
        logger.info("Dry-run completed successfully!")