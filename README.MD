# Singapore Company Database ETL Pipeline

A comprehensive data engineering solution for building a Singapore company intelligence database through automated extraction, transformation, and loading of publicly available company information.

## Project Overview

This project demonstrates advanced data engineering techniques by creating a robust ETL pipeline that:
- Extracts company data from Singapore government sources (ACRA, Data.gov.sg)
- Scrapes company websites for additional information
- Uses Large Language Models (LLMs) for data enrichment and classification
- Implements entity matching and deduplication
- Maintains high data quality standards
- Provides comprehensive monitoring and reporting

##  Key Features

- **Multi-source Data Extraction**: ACRA, BizFile, Data.gov.sg, company websites
- **AI-Powered Enrichment**: Industry classification, keyword extraction, data normalization
- **Advanced Web Scraping**: Selenium-based website data extraction
- **Entity Matching**: Fuzzy matching and deduplication algorithms
- **Data Quality Monitoring**: Automated validation and quality scoring
- **Scalable Architecture**: Configurable batch processing and parallel execution
- **Comprehensive Logging**: Structured logging with performance monitoring

##  Quick Start

### Prerequisites

- Python 3.8+
- Chrome/Chromium browser (for web scraping)
- 8GB+ RAM recommended
- 5GB+ disk space

### Installation

1. **Clone and setup environment**
```bash
git clone <repository-url>
cd singapore-company-etl
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure environment**
```bash
cp .env.example .env
# Edit .env with your settings if needed
```

### Basic Usage

**Quick test run:**
```bash
python main.py --target-count 100 --dry-run
```

**Production run:**
```bash
python main.py --target-count 1000
```

**Fast run (skip time-intensive features):**
```bash
python main.py --target-count 5000 --skip-scraping --skip-llm
```

## Project Structure

```
singapore-company-etl/
├── main.py                 # Main entry point
├── config.py              # Configuration management
├── models.py              # Data models
├── database.py            # Database operations
├── data_extractor.py      # Government data extraction
├── web_scraper.py         # Website scraping
├── llm_processor.py       # LLM integration
├── entity_matcher.py     # Entity matching & deduplication
├── main_etl.py           # ETL pipeline orchestrator
├── requirements.txt       # Python dependencies
├── .env.example          # Environment variables template
├── README.md             # This file
├── logs/                 # Log files (created automatically)
└── singapore_companies.db # SQLite database (created automatically)
```

## Configuration

### Command Line Options

```bash
python main.py [OPTIONS]

Options:
  --target-count INTEGER     Number of companies to extract (default: 10000)
  --batch-size INTEGER       Batch size for processing (default: 100)
  --skip-scraping           Skip website scraping (faster)
  --skip-llm               Skip LLM enrichment (faster)
  --dry-run                Test run without database writes
  --backup                 Create backup before running
  --log-level LEVEL        Logging level (DEBUG, INFO, WARNING, ERROR)
  --help                   Show help message
```

### Environment Variables

Key environment variables (see `.env.example`):

- `TARGET_COMPANY_COUNT`: Default number of companies to extract
- `BATCH_SIZE`: Processing batch size
- `LLM_MODEL`: LLM model to use for enrichment
- `ENABLE_WEBSITE_SCRAPING`: Enable/disable web scraping
- `ENABLE_LLM_ENRICHMENT`: Enable/disable LLM processing

##  Data Sources

### Primary Sources
1. **Singapore Data.gov.sg**
   - Official government open data portal
   - ACRA business profiles
   - Industry classifications

2. **Company Websites**
   - Contact information
   - Services and products
   - Social media links
   - Company descriptions

### Data Fields Extracted

The pipeline extracts and standardizes the following fields:

**Core Information:**
- `uen` - Unique Entity Number
- `company_name` - Official company name
- `website` - Primary website URL
- `industry` - Industry classification
- `hq_country` - Headquarters country

**Contact Information:**
- `contact_email` - Primary email
- `contact_phone` - Primary phone
- `linkedin` - LinkedIn profile URL
- `facebook` - Facebook page URL
- `instagram` - Instagram profile URL

**Business Details:**
- `number_of_employees` - Employee count
- `company_size` - Size classification (Small/Medium/Large)
- `founding_year` - Year established
- `revenue` - Annual revenue
- `products_offered` - Key products
- `services_offered` - Key services
- `keywords` - Relevant search terms

##  LLM Integration

The pipeline uses open-source Large Language Models for:

### Industry Classification
```python
# Example prompt used for classification
prompt = """
Based on the company description below, classify it into ONE of these industries:
Technology, Finance, Healthcare, Manufacturing, Retail, Education, 
Real Estate, Transportation, Food & Beverage, Professional Services

Company description: {description}

The industry is:"""
```

### Keyword Extraction
- Extracts relevant terms from company descriptions
- Filters common business terms and stop words
- Provides searchable keywords for each company

### Data Normalization
- Standardizes company size classifications
- Cleans and formats contact information
- Resolves industry naming inconsistencies

##  Data Quality Framework

### Quality Metrics

1. **Completeness Score** (0-100%)
   - Percentage of required fields populated
   - Weighted by field importance

2. **Entity Matching**
   - UEN-based exact matching (highest priority)
   - Website domain matching
   - Fuzzy name matching (85% threshold)

3. **Data Validation**
   - Email format validation
   - Phone number format checking
   - URL format verification
   - Business rule compliance

### Quality Report Example

```
SINGAPORE COMPANY DATABASE REPORT
==================================================
Total Companies: 5,247
Website Coverage: 67.3%
LinkedIn Coverage: 34.2%
Email Coverage: 45.8%
Industry Coverage: 89.1%
Average Quality Score: 73.2%

Top 5 Industries:
  Technology: 1,247 companies
  Professional Services: 892 companies
  Manufacturing: 678 companies
  Retail: 534 companies
  Finance: 423 companies
==================================================
```

##  Entity Matching Algorithm

The pipeline implements sophisticated entity matching:

```python
def match_companies(company1, company2):
    # 1. Perfect UEN match (100% confidence)
    if company1.uen == company2.uen:
        return True
    
    # 2. Website domain match (95% confidence)
    if normalize_domain(company1.website) == normalize_domain(company2.website):
        return True
    
    # 3. Fuzzy name match (85% threshold)
    similarity = fuzzy_ratio(
        normalize_name(company1.name), 
        normalize_name(company2.name)
    )
    return similarity >= 85
```

## Performance & Scalability

### Performance Optimizations

- **Concurrent Processing**: Multi-threaded web scraping
- **Batch Processing**: Database operations in batches
- **Connection Pooling**: Efficient database connections
- **Rate Limiting**: Respectful web scraping delays
- **Memory Management**: Streaming data processing

### Scaling Recommendations

For larger datasets (100K+ companies):

1. **Use PostgreSQL** instead of SQLite
2. **Implement distributed processing** with Celery/Redis
3. **Add caching layer** with Redis
4. **Use cloud services** for web scraping (ScrapingBee, etc.)
5. **Implement data partitioning** by industry or region

## Logging & Monitoring

### Log Files

- `logs/etl_YYYYMMDD_HHMMSS.log` - Main pipeline log
- Console output with progress indicators
- Structured JSON logs for production monitoring

### Performance Monitoring

The pipeline tracks:
- Processing speed (companies/minute)
- Success/failure rates by source
- Data quality metrics over time
- Resource utilization (CPU, memory)

##  Troubleshooting

### Common Issues

**1. Chrome/ChromeDriver Issues**
```bash
# Install Chrome and ChromeDriver
sudo apt-get install google-chrome-stable
pip install webdriver-manager
```

**2. Memory Issues**
```bash
# Reduce batch size
python main.py --batch-size 50 --target-count 1000
```

**3. Network/Scraping Issues**
```bash
# Skip scraping if websites are blocking
python main.py --skip-scraping
```

**4. LLM Issues**
```bash
# Skip LLM if having GPU/memory issues
python main.py --skip-llm
```

### Debug Mode

```bash
# Run with debug logging
python main.py --log-level DEBUG --target-count 10 --dry-run
```
