# main.py
# Main entry point for the Singapore Company ETL Pipeline

import argparse
import sys
import logging
import signal
from pathlib import Path
from datetime import datetime

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Import our modules
from main_etl import SGCompanyETL
from config import config

def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration"""
    # Create logs directory
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Setup logging
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # File handler
    file_handler = logging.FileHandler(
        logs_dir / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger

# main.py
# Main entry point for the Singapore Company ETL Pipeline

import argparse
import sys
import logging
import signal
from pathlib import Path
from datetime import datetime

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Import our modules
from main_etl import SGCompanyETL
from config import config

def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration"""
    # Create logs directory
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Setup logging
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # File handler
    file_handler = logging.FileHandler(
        logs_dir / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger

def signal_handler(signum, frame):
    """Handle graceful shutdown"""
    print("\n\nReceived shutdown signal. Gracefully stopping...")
    logging.info("ETL Pipeline interrupted by user")
    sys.exit(0)

def create_backup():
    """Create database backup"""
    from database import DatabaseManager
    
    try:
        db_manager = DatabaseManager()
        if Path(db_manager.db_path).exists():
            # Simple backup by copying file
            backup_path = f"{db_manager.db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            import shutil
            shutil.copy2(db_manager.db_path, backup_path)
            print(f"Database backed up to: {backup_path}")
        else:
            print("No existing database found to backup")
    except Exception as e:
        print(f"Backup failed: {e}")

def main():
    """Main entry point for the ETL pipeline"""
    
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Singapore Company ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --target-count 1000                    # Extract 1000 companies
  python main.py --target-count 5000 --skip-scraping    # Skip website scraping
  python main.py --target-count 100 --dry-run           # Test run without DB writes
  python main.py --backup --target-count 10000          # Create backup first
        """
    )
    
    parser.add_argument(
        "--target-count", 
        type=int, 
        default=config.etl.target_company_count,
        help=f"Target number of companies to extract (default: {config.etl.target_company_count})"
    )
    
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=config.etl.batch_size,
        help=f"Batch size for processing (default: {config.etl.batch_size})"
    )
    
    parser.add_argument(
        "--skip-scraping", 
        action="store_true",
        help="Skip website scraping (faster execution)"
    )
    
    parser.add_argument(
        "--skip-llm", 
        action="store_true",
        help="Skip LLM enrichment (faster execution)"
    )
    
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Run without writing to database (testing)"
    )
    
    parser.add_argument(
        "--log-level", 
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--backup", 
        action="store_true",
        help="Create database backup before running"
    )
    
    parser.add_argument(
        "--version", 
        action="version", 
        version="Singapore Company ETL Pipeline v1.0.0"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_level)
    
    # Print startup banner
    print("=" * 60)
    print("SINGAPORE COMPANY DATABASE ETL PIPELINE")
    print("=" * 60)
    print(f"Target Companies: {args.target_count:,}")
    print(f"Batch Size: {args.batch_size}")
    print(f"Website Scraping: {'Disabled' if args.skip_scraping else 'Enabled'}")
    print(f"LLM Enrichment: {'Disabled' if args.skip_llm else 'Enabled'}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'PRODUCTION'}")
    print(f"Log Level: {args.log_level}")
    print("=" * 60)
    
    # Create backup if requested
    if args.backup:
        logger.info("Creating database backup...")
        create_backup()
    
    # Update configuration based on arguments
    config.etl.target_company_count = args.target_count
    config.etl.batch_size = args.batch_size
    config.etl.enable_website_scraping = not args.skip_scraping
    config.etl.enable_llm_enrichment = not args.skip_llm
    
    try:
        # Initialize and run ETL pipeline
        logger.info("Initializing ETL pipeline...")
        etl = SGCompanyETL()
        
        if args.dry_run:
            logger.info("Running in DRY-RUN mode")
            etl.run_pipeline_dry_run(args.target_count)
        else:
            logger.info("Running in PRODUCTION mode")
            stats = etl.run_pipeline(args.target_count)
            
            # Print success summary
            print(f"\n✅ ETL Pipeline completed successfully!")
            print(f"📊 {stats.get('companies_loaded', 0)} companies loaded into database")
            
        logger.info("ETL Pipeline finished successfully!")
        return 0
        
    except KeyboardInterrupt:
        logger.info("ETL Pipeline interrupted by user")
        print("\n❌ Pipeline interrupted by user")
        return 1
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
        print(f"\n❌ Pipeline failed: {e}")
        print("Check the log files for detailed error information")
        return 1

def check_dependencies():
    """Check if all required dependencies are available"""
    missing_deps = []
    
    try:
        import requests
    except ImportError:
        missing_deps.append("requests")
    
    try:
        import pandas
    except ImportError:
        missing_deps.append("pandas")
    
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        missing_deps.append("beautifulsoup4")
    
    # Optional dependencies
    optional_missing = []
    
    try:
        from selenium import webdriver
    except ImportError:
        optional_missing.append("selenium (for advanced web scraping)")
    
    try:
        from transformers import pipeline
    except ImportError:
        optional_missing.append("transformers (for LLM features)")
    
    try:
        from fuzzywuzzy import fuzz
    except ImportError:
        optional_missing.append("fuzzywuzzy (for better entity matching)")
    
    if missing_deps:
        print("❌ Missing required dependencies:")
        for dep in missing_deps:
            print(f"  - {dep}")
        print("\nPlease install missing dependencies with:")
        print(f"pip install {' '.join(missing_deps)}")
        return False
    
    if optional_missing:
        print("⚠️  Missing optional dependencies (features will be limited):")
        for dep in optional_missing:
            print(f"  - {dep}")
        print()
    
    return True

def show_help():
    """Show additional help information"""
    print("""
SINGAPORE COMPANY ETL PIPELINE - HELP

This pipeline extracts company data from Singapore government sources,
enriches it with web scraping and LLM processing, and loads it into a database.

QUICK START:
1. Install dependencies: pip install -r requirements.txt
2. Run with default settings: python main.py
3. Run a small test: python main.py --target-count 100 --dry-run

CONFIGURATION:
- Edit config.py to change default settings
- Set environment variables (see .env.example)
- Use command line arguments for runtime options

DATA SOURCES:
- Singapore Data.gov.sg API
- ACRA company registry data
- Company websites (via web scraping)
- Social media profiles

OUTPUT:
- SQLite database: singapore_companies.db
- Log files: logs/etl_YYYYMMDD_HHMMSS.log
- Coverage and quality reports

For more information, see the README.md file.
    """)

if __name__ == "__main__":
    # Check dependencies first
    if not check_dependencies():
        sys.exit(1)
    
    # Handle help request
    if len(sys.argv) > 1 and sys.argv[1] in ["--help-extended", "-h-ext"]:
        show_help()
        sys.exit(0)
    
    # Run main function
    exit_code = main()
    sys.exit(exit_code)