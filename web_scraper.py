# web_scraper.py
# Website scraping for company information

import re
import time
import logging
from typing import Dict, Optional
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

# Selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logging.warning("Selenium not available. Web scraping will be limited.")

logger = logging.getLogger(__name__)

class WebScraper:
    """Handles website scraping for company information"""
    
    def __init__(self, headless: bool = True, use_selenium: bool = True):
        self.use_selenium = use_selenium and SELENIUM_AVAILABLE
        self.driver = None
        
        # Regular expressions for data extraction
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        self.phone_pattern = re.compile(r'(\+65\s?)?[689]\d{7}')
        
        # Setup requests session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        if self.use_selenium:
            self.setup_driver(headless)
    
    def setup_driver(self, headless: bool):
        """Setup Selenium WebDriver"""
        if not SELENIUM_AVAILABLE:
            logger.warning("Selenium not available, falling back to requests")
            return
            
        try:
            options = Options()
            if headless:
                options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(30)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("Selenium WebDriver setup successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup WebDriver: {e}")
            self.driver = None
            self.use_selenium = False
    
    def scrape_company_website(self, url: str) -> Dict:
        """Scrape company information from website"""
        data = {
            'website': url,
            'source_of_data': 'website_scraping'
        }
        
        try:
            if self.use_selenium and self.driver:
                data.update(self._scrape_with_selenium(url))
            else:
                data.update(self._scrape_with_requests(url))
                
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
        
        return data
    
    def _scrape_with_selenium(self, url: str) -> Dict:
        """Scrape using Selenium for JavaScript-heavy sites"""
        data = {}
        
        try:
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Get page content
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            data.update(self._extract_data_from_soup(soup))
            
        except TimeoutException:
            logger.warning(f"Timeout loading {url}")
        except WebDriverException as e:
            logger.error(f"WebDriver error for {url}: {e}")
            
        return data
    
    def _scrape_with_requests(self, url: str) -> Dict:
        """Scrape using requests for simple sites"""
        data = {}
        
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            data.update(self._extract_data_from_soup(soup))
            
        except requests.RequestException as e:
            logger.warning(f"Request error for {url}: {e}")
            
        return data
    
    def _extract_data_from_soup(self, soup: BeautifulSoup) -> Dict:
        """Extract data from BeautifulSoup object"""
        data = {}
        
        # Extract company name from title or header
        title = soup.find('title')
        if title:
            title_text = title.get_text().strip()
            # Clean title text
            title_text = re.sub(r'\s*-\s*.*', '', title_text)  
            data['company_name'] = title_text
        
        # Try to find company name in h1 tags
        if not data.get('company_name'):
            h1_tags = soup.find_all('h1')
            for h1 in h1_tags:
                if len(h1.get_text().strip()) < 100:  # Reasonable company name length
                    data['company_name'] = h1.get_text().strip()
                    break
        
        # Get all text content for pattern matching
        page_text = soup.get_text()
        
        # Extract contact information
        emails = self.email_pattern.findall(page_text)
        if emails:
            # Filter out common non-contact emails
            filtered_emails = [email for email in emails 
                             if not any(skip in email.lower() 
                                       for skip in ['noreply', 'no-reply', 'support@example', 'admin@example'])]
            if filtered_emails:
                data['contact_email'] = filtered_emails[0]
        
        # Extract phone numbers
        phones = self.phone_pattern.findall(page_text)
        if phones:
            data['contact_phone'] = phones[0]
        
        # Extract social media links
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            if 'linkedin.com/company' in href or 'linkedin.com/in' in href:
                data['linkedin'] = link['href']
            elif 'facebook.com' in href and '/pages/' not in href:
                data['facebook'] = link['href']
            elif 'instagram.com' in href:
                data['instagram'] = link['href']
        
        # Extract meta description for services/products
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            content = meta_desc.get('content', '').strip()
            if len(content) > 20:  # Meaningful description
                data['services_offered'] = content
        
        # Look for about us section
        about_section = soup.find(['div', 'section'], 
                                 class_=re.compile(r'about', re.I))
        if about_section:
            about_text = about_section.get_text().strip()
            
            # Extract founding year
            year_matches = re.findall(r'(19|20)\d{2}', about_text)
            if year_matches:
                # Get the most reasonable founding year (not too recent, not too old)
                years = [int(year) for year in year_matches]
                reasonable_years = [year for year in years if 1950 <= year <= 2024]
                if reasonable_years:
                    data['founding_year'] = min(reasonable_years)  # Usually the founding year is the earliest
        
        # Try to extract employee count
        employee_patterns = [
            r'(\d+)\s*employees',
            r'team\s+of\s+(\d+)',
            r'(\d+)\s*staff',
            r'(\d+)\s*people'
        ]
        
        for pattern in employee_patterns:
            matches = re.findall(pattern, page_text, re.I)
            if matches:
                try:
                    employee_count = int(matches[0])
                    if 1 <= employee_count <= 100000:  # Reasonable range
                        data['number_of_employees'] = employee_count
                        break
                except ValueError:
                    continue
        
        # Extract keywords from page content
        # This is a simple version - LLM will do more sophisticated extraction
        important_sections = soup.find_all(['h1', 'h2', 'h3', 'title', 'meta'])
        keywords_text = ' '.join([elem.get_text() if elem.name != 'meta' 
                                 else elem.get('content', '') 
                                 for elem in important_sections])
        
        if keywords_text:
            # Extract meaningful words (3+ characters, not common words)
            words = re.findall(r'\b[a-zA-Z]{3,}\b', keywords_text.lower())
            # Filter common words
            common_words = {'the', 'and', 'for', 'are', 'with', 'our', 'you', 'your', 
                           'company', 'business', 'service', 'services', 'solutions'}
            meaningful_words = [word for word in words if word not in common_words]
            
            # Get unique words and limit to reasonable number
            unique_words = list(dict.fromkeys(meaningful_words))[:10]
            data['keywords'] = ', '.join(unique_words)
        
        return data
    
    def close(self):
        """Clean up resources"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logger.error(f"Error closing WebDriver: {e}")
        
        if hasattr(self.session, 'close'):
            self.session.close()
    
    def __del__(self):
        """Destructor to ensure cleanup"""
        self.close()