# database.py
# Database management for Singapore Company ETL Pipeline

import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
from models import CompanyData

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handles all database operations"""
    
    def __init__(self, db_path: str = "singapore_companies.db"):
        self.db_path = db_path
        self.create_tables()
    
    def create_tables(self):
        """Create database tables with proper schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main companies table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uen TEXT UNIQUE,
            company_name TEXT NOT NULL,
            website TEXT,
            hq_country TEXT DEFAULT 'Singapore',
            no_of_locations_in_singapore INTEGER,
            linkedin TEXT,
            facebook TEXT,
            instagram TEXT,
            industry TEXT,
            number_of_employees INTEGER,
            company_size TEXT CHECK (company_size IN ('Small', 'Medium', 'Large', 'Unknown')),
            is_it_delisted BOOLEAN DEFAULT FALSE,
            stock_exchange_code TEXT,
            revenue TEXT,
            founding_year INTEGER,
            contact_email TEXT,
            contact_phone TEXT,
            products_offered TEXT,
            services_offered TEXT,
            keywords TEXT,
            data_quality_score REAL DEFAULT 0.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Data sources tracking table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER,
            field_name TEXT,
            source_name TEXT,
            extraction_timestamp TIMESTAMP,
            confidence_score REAL DEFAULT 1.0,
            FOREIGN KEY (company_id) REFERENCES companies (id)
        )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_uen ON companies(uen)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_name ON companies(company_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_website ON companies(website)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_industry ON companies(industry)')
        
        # Create trigger for data quality score calculation
        cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS calculate_data_quality_score
            AFTER INSERT OR UPDATE ON companies
            FOR EACH ROW
        BEGIN
            UPDATE companies SET data_quality_score = (
                CASE WHEN NEW.uen IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN NEW.company_name IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN NEW.website IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN NEW.linkedin IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN NEW.industry IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN NEW.contact_email IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN NEW.contact_phone IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN NEW.founding_year IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN NEW.number_of_employees IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN NEW.services_offered IS NOT NULL THEN 1 ELSE 0 END
            ) / 10.0 * 100.0
            WHERE id = NEW.id;
        END;
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Database tables created successfully")
    
    def insert_company(self, company: CompanyData) -> int:
        """Insert or update a company record"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
            INSERT OR REPLACE INTO companies (
                uen, company_name, website, hq_country, no_of_locations_in_singapore,
                linkedin, facebook, instagram, industry, number_of_employees,
                company_size, is_it_delisted, stock_exchange_code, revenue,
                founding_year, contact_email, contact_phone, products_offered,
                services_offered, keywords
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                company.uen, company.company_name, company.website,
                company.hq_country, company.no_of_locations_in_singapore,
                company.linkedin, company.facebook, company.instagram,
                company.industry, company.number_of_employees,
                company.company_size, company.is_it_delisted,
                company.stock_exchange_code, company.revenue,
                company.founding_year, company.contact_email,
                company.contact_phone, company.products_offered,
                company.services_offered, company.keywords
            ))
            
            company_id = cursor.lastrowid
            conn.commit()
            return company_id
            
        except Exception as e:
            logger.error(f"Error inserting company {company.company_name}: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()
    
    def insert_companies_batch(self, companies: List[CompanyData]) -> int:
        """Insert multiple companies in batch"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        inserted_count = 0
        
        try:
            for company in companies:
                cursor.execute('''
                INSERT OR REPLACE INTO companies (
                    uen, company_name, website, hq_country, no_of_locations_in_singapore,
                    linkedin, facebook, instagram, industry, number_of_employees,
                    company_size, is_it_delisted, stock_exchange_code, revenue,
                    founding_year, contact_email, contact_phone, products_offered,
                    services_offered, keywords
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    company.uen, company.company_name, company.website,
                    company.hq_country, company.no_of_locations_in_singapore,
                    company.linkedin, company.facebook, company.instagram,
                    company.industry, company.number_of_employees,
                    company.company_size, company.is_it_delisted,
                    company.stock_exchange_code, company.revenue,
                    company.founding_year, company.contact_email,
                    company.contact_phone, company.products_offered,
                    company.services_offered, company.keywords
                ))
                inserted_count += 1
            
            conn.commit()
            logger.info(f"Successfully inserted {inserted_count} companies")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error in batch insert: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
    
    def get_company_count(self) -> int:
        """Get total number of companies in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT COUNT(*) FROM companies")
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            logger.error(f"Error getting company count: {e}")
            return 0
        finally:
            conn.close()
    
    def get_data_coverage_report(self) -> Dict:
        """Generate data coverage report"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Basic statistics
            total_companies = pd.read_sql("SELECT COUNT(*) as count FROM companies", conn).iloc[0]['count']
            
            # Coverage analysis
            coverage_query = '''
            SELECT 
                SUM(CASE WHEN website IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as website_coverage,
                SUM(CASE WHEN linkedin IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as linkedin_coverage,
                SUM(CASE WHEN contact_email IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as email_coverage,
                SUM(CASE WHEN industry IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as industry_coverage,
                AVG(data_quality_score) as avg_quality_score
            FROM companies
            '''
            
            coverage_data = pd.read_sql(coverage_query, conn)
            
            # Top industries
            top_industries = pd.read_sql('''
            SELECT industry, COUNT(*) as company_count 
            FROM companies 
            WHERE industry IS NOT NULL 
            GROUP BY industry 
            ORDER BY company_count DESC 
            LIMIT 5
            ''', conn)
            
            return {
                'total_companies': total_companies,
                'coverage': coverage_data.iloc[0].to_dict(),
                'top_industries': top_industries.to_dict('records')
            }
            
        except Exception as e:
            logger.error(f"Error generating coverage report: {e}")
            return {}
        finally:
            conn.close()
    
    def close(self):
        """Close database connection"""
        pass  # Using context managers, so explicit close not needed