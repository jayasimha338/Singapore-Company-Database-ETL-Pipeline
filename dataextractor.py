# data_extractor.py
# Data extraction from various Singapore government sources

import requests
import json
import logging
import time
from typing import Dict, List, Optional
from datetime import datetime
import csv
import io

logger = logging.getLogger(__name__)

class SGDataExtractor:
    """Handles data extraction from various Singapore government sources"""
    
    def __init__(self):
        self.base_urls = {
            'data_gov_sg': 'https://data.gov.sg/api',
            'acra_api': 'https://api.acra.gov.sg',  # Hypothetical API endpoint
        }
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Singapore-Company-ETL/1.0 (Educational Purpose)',
            'Accept': 'application/json'
        })
    
    def extract_companies_from_data_gov(self, limit: int = 1000) -> List[Dict]:
        """
        Extract company data from Singapore's Data.gov.sg
        
        Note: This demonstrates the structure. In practice, you would:
        1. Register for API access at data.gov.sg
        2. Use actual dataset IDs for ACRA business profiles
        3. Handle authentication and rate limiting
        """
        companies = []
        
        try:
            logger.info("Extracting companies from Data.gov.sg...")
            
            # Example API structure for ACRA data (you need to replace with actual endpoints)
            # Real implementation would use: https://data.gov.sg/dataset/acra-information-on-corporate-entities
            
            # For demonstration, we'll simulate the data structure
            # In reality, you would make actual API calls like:
            # response = self.session.get(f"{self.base_urls['data_gov_sg']}/action/datastore_search", 
            #                           params={'resource_id': 'actual_resource_id', 'limit': limit})
            
            # Simulated data structure based on actual ACRA format
            for i in range(min(limit, 1000)):
                company = {
                    'uen': f'20{1000000000 + i:010d}A',  # Simulated UEN format
                    'company_name': f'Singapore Company {i+1} Pte Ltd',
                    'reg_street_name': f'Street {i+1}',
                    'reg_postal_code': f'{100000 + i:06d}',
                    'company_type': 'PRIVATE COMPANY LIMITED BY SHARES',
                    'primary_ssic_code': '62010',  # Computer programming activities
                    'primary_ssic_description': 'Computer programming activities',
                    'secondary_ssic_code': None,
                    'incorporation_date': '2020-01-15',
                    'company_status': 'Live Company',
                    'source': 'data_gov_sg',
                    'extraction_timestamp': datetime.now().isoformat()
                }
                companies.append(company)
                
                # Add some variety to the data
                if i % 10 == 0:
                    company['company_name'] = f'Tech Solutions {i+1} Pte Ltd'
                    company['primary_ssic_description'] = 'Information technology consultancy activities'
                elif i % 15 == 0:
                    company['company_name'] = f'Food & Beverage {i+1} Pte Ltd'
                    company['primary_ssic_description'] = 'Restaurants'
                    company['primary_ssic_code'] = '56101'
                
        except Exception as e:
            logger.error(f"Error extracting from Data.gov.sg: {e}")
        
        logger.info(f"Extracted {len(companies)} companies from Data.gov.sg")
        return companies
    
    def extract_companies_from_csv_sources(self, csv_url: str = None) -> List[Dict]:
        """
        Extract company data from CSV sources
        
        Note: Replace with actual CSV download URLs from Singapore government
        """
        companies = []
        
        try:
            if not csv_url:
                # This would be replaced with actual government CSV URLs
                logger.info("Using simulated CSV data source...")
                return self._generate_sample_csv_data(500)
            
            logger.info(f"Downloading CSV from: {csv_url}")
            response = self.session.get(csv_url, timeout=30)
            response.raise_for_status()
            
            # Parse CSV content
            csv_content = io.StringIO(response.text)
            reader = csv.DictReader(csv_content)
            
            for row in reader:
                company = {
                    'uen': row.get('uen'),
                    'company_name': row.get('entity_name'),
                    'company_type': row.get('entity_type'),
                    'primary_ssic_code': row.get('primary_ssic_code'),
                    'primary_ssic_description': row.get('primary_ssic_description'),
                    'reg_street_name': row.get('reg_street_name'),
                    'reg_postal_code': row.get('reg_postal_code'),
                    'incorporation_date': row.get('incorporation_date'),
                    'company_status': row.get('entity_status'),
                    'source': 'csv_download',
                    'extraction_timestamp': datetime.now().isoformat()
                }
                companies.append(company)
                
        except Exception as e:
            logger.error(f"Error extracting from CSV source: {e}")
        
        logger.info(f"Extracted {len(companies)} companies from CSV sources")
        return companies
    
    def _generate_sample_csv_data(self, count: int) -> List[Dict]:
        """Generate sample data for demonstration purposes"""
        industries = [
            ('62010', 'Computer programming activities'),
            ('56101', 'Restaurants'),
            ('64110', 'Central banking'),
            ('85100', 'Pre-primary education'),
            ('68100', 'Buying and selling of own real estate'),
            ('49100', 'Land transport of passengers'),
            ('47110', 'Retail sale in non-specialised stores with food predominating'),
            ('70100', 'Activities of head offices'),
            ('86901', 'General medical practice'),
            ('25110', 'Manufacture of structural metal products')
        ]
        
        company_types = [
            'PRIVATE COMPANY LIMITED BY SHARES',
            'PUBLIC COMPANY LIMITED BY SHARES', 
            'BUSINESS',
            'LIMITED LIABILITY PARTNERSHIP',
            'SOCIETY'
        ]
        
        companies = []
        for i in range(count):
            industry = industries[i % len(industries)]
            company_type = company_types[i % len(company_types)]
            
            company = {
                'uen': f'20{1000000000 + i:010d}A',
                'company_name': f'{self._generate_company_name(i, industry[1])}',
                'company_type': company_type,
                'primary_ssic_code': industry[0],
                'primary_ssic_description': industry[1],
                'reg_street_name': f'Street {i+1}',
                'reg_postal_code': f'{100000 + (i % 900000):06d}',
                'incorporation_date': f'20{10 + (i % 15)}-{1 + (i % 12):02d}-{1 + (i % 28):02d}',
                'company_status': 'Live Company' if i % 20 != 0 else 'Struck Off',
                'source': 'sample_data',
                'extraction_timestamp': datetime.now().isoformat()
            }
            companies.append(company)
        
        return companies
    
    def _generate_company_name(self, index: int, industry_desc: str) -> str:
        """Generate realistic company names based on industry"""
        name_patterns = {
            'Computer programming': ['Tech Solutions', 'Digital Systems', 'Software Innovations', 'Cyber Solutions'],
            'Restaurants': ['Food Paradise', 'Culinary Delights', 'Taste Buds', 'Flavour House'],
            'banking': ['Financial Services', 'Capital Management', 'Investment Holdings'],
            'education': ['Learning Center', 'Education Hub', 'Knowledge Institute'],
            'real estate': ['Property Holdings', 'Real Estate Development', 'Land Holdings'],
            'transport': ['Logistics Solutions', 'Transport Services', 'Mobility Solutions'],
            'Retail': ['Trading Company', 'Commerce Solutions', 'Retail Holdings'],
            'head offices': ['Holdings', 'Group', 'Investments', 'Management'],
            'medical': ['Healthcare Services', 'Medical Center', 'Wellness Clinic'],
            'Manufacture': ['Manufacturing', 'Industrial Solutions', 'Production Systems']
        }
        
        # Find matching pattern
        pattern_key = next((key for key in name_patterns.keys() if key.lower() in industry_desc.lower()), 'Holdings')
        patterns = name_patterns.get(pattern_key, ['Solutions', 'Services', 'Holdings'])
        
        pattern = patterns[index % len(patterns)]
        number = index + 1
        
        return f'{pattern} {number} Pte Ltd'
    
    def search_company_websites(self, company_name: str) -> Optional[str]:
        """
        Search for company website using search engines
        
        Note: In production, you would use search APIs like:
        - Google Custom Search API
        - Bing Search API
        - DuckDuckGo API
        """
        try:
            # Simulate website search - in reality use actual search APIs
            clean_name = company_name.lower().replace(' pte ltd', '').replace(' ', '')
            
            # Common Singapore domain patterns
            possible_domains = [
                f"https://www.{clean_name}.com.sg",
                f"https://www.{clean_name}.sg", 
                f"https://{clean_name}.com",
                f"https://www.{clean_name}.com"
            ]
            
            # In production, you would actually check if these domains exist
            # For now, return the first pattern for demo
            return possible_domains[0]
            
        except Exception as e:
            logger.error(f"Error searching website for {company_name}: {e}")
            return None
    
    def get_extraction_stats(self) -> Dict:
        """Get statistics about the extraction process"""
        # This would track actual extraction metrics
        return {
            'total_sources_accessed': 2,
            'successful_extractions': 1500,
            'failed_extractions': 0,
            'data_coverage': {
                'uen': 100.0,
                'company_name': 100.0,
                'industry': 100.0,
                'address': 90.0
            }
        }