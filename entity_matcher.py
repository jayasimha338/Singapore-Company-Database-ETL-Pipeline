# entity_matcher.py
# Entity matching and deduplication for company records

import logging
from typing import List, Dict, Tuple, Optional
from urllib.parse import urlparse
import re

# Fuzzy matching
try:
    from fuzzywuzzy import fuzz, process
    FUZZYWUZZY_AVAILABLE = True
except ImportError:
    FUZZYWUZZY_AVAILABLE = False
    logging.warning("FuzzyWuzzy not available. Entity matching will be limited.")

logger = logging.getLogger(__name__)

class EntityMatcher:
    """Handles entity matching and deduplication"""
    
    def __init__(self, threshold: int = 85):
        self.threshold = threshold
        self.fuzzy_available = FUZZYWUZZY_AVAILABLE
    
    def fuzzy_match_companies(self, companies: List[Dict]) -> List[Dict]:
        """Match and merge duplicate companies"""
        if not companies:
            return []
        
        logger.info(f"Starting entity matching for {len(companies)} companies")
        
        matched_companies = []
        processed_companies = []
        
        for company in companies:
            # Skip companies without basic information
            if not company.get('company_name'):
                continue
            
            # Find potential matches
            best_match_idx = self._find_best_match(company, processed_companies)
            
            if best_match_idx is not None:
                # Merge with existing company
                merged_company = self._merge_companies(
                    processed_companies[best_match_idx], 
                    company
                )
                processed_companies[best_match_idx] = merged_company
                matched_companies[best_match_idx] = merged_company
            else:
                # Add as new company
                processed_companies.append(company.copy())
                matched_companies.append(company.copy())
        
        logger.info(f"Entity matching completed. Reduced from {len(companies)} to {len(matched_companies)} companies")
        return matched_companies
    
    def _find_best_match(self, company: Dict, existing_companies: List[Dict]) -> Optional[int]:
        """Find the best matching company in existing list"""
        if not existing_companies:
            return None
        
        company_name = company.get('company_name', '').strip()
        company_uen = company.get('uen', '').strip()
        company_website = company.get('website', '').strip()
        
        for idx, existing in enumerate(existing_companies):
            existing_name = existing.get('company_name', '').strip()
            existing_uen = existing.get('uen', '').strip()
            existing_website = existing.get('website', '').strip()
            
            # Perfect UEN match (highest priority)
            if company_uen and existing_uen and company_uen == existing_uen:
                return idx
            
            # Perfect website match (high priority)
            if (company_website and existing_website and 
                self._normalize_website(company_website) == self._normalize_website(existing_website)):
                return idx
            
            # Fuzzy name matching
            if company_name and existing_name:
                similarity = self._calculate_name_similarity(company_name, existing_name)
                if similarity >= self.threshold:
                    return idx
        
        return None
    
# entity_matcher.py
# Entity matching and deduplication for company records

import logging
from typing import List, Dict, Tuple, Optional
from urllib.parse import urlparse
import re

# Fuzzy matching
try:
    from fuzzywuzzy import fuzz, process
    FUZZYWUZZY_AVAILABLE = True
except ImportError:
    FUZZYWUZZY_AVAILABLE = False
    logging.warning("FuzzyWuzzy not available. Entity matching will be limited.")

logger = logging.getLogger(__name__)

class EntityMatcher:
    """Handles entity matching and deduplication"""
    
    def __init__(self, threshold: int = 85):
        self.threshold = threshold
        self.fuzzy_available = FUZZYWUZZY_AVAILABLE
    
    def fuzzy_match_companies(self, companies: List[Dict]) -> List[Dict]:
        """Match and merge duplicate companies"""
        if not companies:
            return []
        
        logger.info(f"Starting entity matching for {len(companies)} companies")
        
        matched_companies = []
        processed_companies = []
        
        for company in companies:
            # Skip companies without basic information
            if not company.get('company_name'):
                continue
            
            # Find potential matches
            best_match_idx = self._find_best_match(company, processed_companies)
            
            if best_match_idx is not None:
                # Merge with existing company
                merged_company = self._merge_companies(
                    processed_companies[best_match_idx], 
                    company
                )
                processed_companies[best_match_idx] = merged_company
                matched_companies[best_match_idx] = merged_company
            else:
                # Add as new company
                processed_companies.append(company.copy())
                matched_companies.append(company.copy())
        
        logger.info(f"Entity matching completed. Reduced from {len(companies)} to {len(matched_companies)} companies")
        return matched_companies
    
    def _find_best_match(self, company: Dict, existing_companies: List[Dict]) -> Optional[int]:
        """Find the best matching company in existing list"""
        if not existing_companies:
            return None
        
        company_name = company.get('company_name', '').strip()
        company_uen = company.get('uen', '').strip()
        company_website = company.get('website', '').strip()
        
        for idx, existing in enumerate(existing_companies):
            existing_name = existing.get('company_name', '').strip()
            existing_uen = existing.get('uen', '').strip()
            existing_website = existing.get('website', '').strip()
            
            # Perfect UEN match (highest priority)
            if company_uen and existing_uen and company_uen == existing_uen:
                return idx
            
            # Perfect website match (high priority)
            if (company_website and existing_website and 
                self._normalize_website(company_website) == self._normalize_website(existing_website)):
                return idx
            
            # Fuzzy name matching
            if company_name and existing_name:
                similarity = self._calculate_name_similarity(company_name, existing_name)
                if similarity >= self.threshold:
                    return idx
        
        return None
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two company names"""
        if not name1 or not name2:
            return 0.0
        
        # Normalize names for comparison
        norm_name1 = self._normalize_company_name(name1)
        norm_name2 = self._normalize_company_name(name2)
        
        if self.fuzzy_available:
            # Use multiple fuzzy matching methods
            ratio = fuzz.ratio(norm_name1, norm_name2)
            partial_ratio = fuzz.partial_ratio(norm_name1, norm_name2)
            token_sort_ratio = fuzz.token_sort_ratio(norm_name1, norm_name2)
            token_set_ratio = fuzz.token_set_ratio(norm_name1, norm_name2)
            
            # Weighted average favoring token-based methods for company names
            similarity = (ratio * 0.2 + partial_ratio * 0.2 + 
                         token_sort_ratio * 0.3 + token_set_ratio * 0.3)
            
            return similarity
        else:
            # Simple string comparison fallback
            if norm_name1 == norm_name2:
                return 100.0
            elif norm_name1 in norm_name2 or norm_name2 in norm_name1:
                return 80.0
            else:
                return 0.0
    
    def _normalize_company_name(self, name: str) -> str:
        """Normalize company name for comparison"""
        if not name:
            return ""
        
        # Convert to lowercase
        normalized = name.lower().strip()
        
        # Remove common company suffixes
        suffixes = [
            'pte ltd', 'pte. ltd.', 'private limited',
            'ltd', 'ltd.', 'limited',
            'inc', 'inc.', 'incorporated',
            'corp', 'corp.', 'corporation',
            'llc', 'l.l.c.',
            'sdn bhd', 'sdn. bhd.',
            'co', 'co.', 'company'
        ]
        
        for suffix in suffixes:
            if normalized.endswith(' ' + suffix):
                normalized = normalized[:-len(suffix)-1].strip()
        
        # Remove extra whitespace and special characters
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def _normalize_website(self, website: str) -> str:
        """Normalize website URL for comparison"""
        if not website:
            return ""
        
        try:
            # Parse URL
            parsed = urlparse(website.lower())
            
            # Extract domain without www
            domain = parsed.netloc or parsed.path
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # Remove trailing slashes and paths for domain comparison
            domain = domain.split('/')[0]
            
            return domain
            
        except Exception:
            return website.lower().strip()
    
    def _merge_companies(self, existing: Dict, new: Dict) -> Dict:
        """Merge two company records, prioritizing non-null values"""
        merged = existing.copy()
        
        # Fields where we prefer the most complete/recent data
        priority_fields = [
            'company_name', 'uen', 'website', 'industry', 
            'number_of_employees', 'contact_email', 'contact_phone'
        ]
        
        # Fields where we can concatenate or combine
        combinable_fields = [
            'products_offered', 'services_offered', 'keywords'
        ]
        
        # Merge priority fields (new overwrites existing if new is not null)
        for field in priority_fields:
            new_value = new.get(field)
            if new_value and (not merged.get(field) or len(str(new_value)) > len(str(merged.get(field, '')))):
                merged[field] = new_value
        
        # Merge combinable fields
        for field in combinable_fields:
            existing_value = merged.get(field, '')
            new_value = new.get(field, '')
            
            if existing_value and new_value and existing_value != new_value:
                # Combine values, avoiding duplicates
                combined = self._combine_text_fields(existing_value, new_value)
                merged[field] = combined
            elif new_value and not existing_value:
                merged[field] = new_value
        
        # Special handling for social media links
        social_fields = ['linkedin', 'facebook', 'instagram']
        for field in social_fields:
            if new.get(field) and not merged.get(field):
                merged[field] = new[field]
        
        # Update metadata
        merged['source_of_data'] = self._combine_sources(
            merged.get('source_of_data', ''),
            new.get('source_of_data', '')
        )
        
        # Keep the most recent extraction timestamp
        if new.get('extraction_timestamp'):
            merged['extraction_timestamp'] = new['extraction_timestamp']
        
        return merged
    
    def _combine_text_fields(self, text1: str, text2: str) -> str:
        """Combine two text fields, removing duplicates"""
        if not text1:
            return text2
        if not text2:
            return text1
        
        # Split by common delimiters
        items1 = re.split(r'[,;|]', text1)
        items2 = re.split(r'[,;|]', text2)
        
        # Clean and deduplicate
        all_items = []
        seen = set()
        
        for item in items1 + items2:
            clean_item = item.strip()
            if clean_item and clean_item.lower() not in seen:
                all_items.append(clean_item)
                seen.add(clean_item.lower())
        
        return ', '.join(all_items)
    
    def _combine_sources(self, source1: str, source2: str) -> str:
        """Combine source information"""
        sources = []
        
        if source1:
            sources.extend(source1.split(','))
        if source2:
            sources.extend(source2.split(','))
        
        # Remove duplicates while preserving order
        unique_sources = []
        seen = set()
        
        for source in sources:
            clean_source = source.strip()
            if clean_source and clean_source not in seen:
                unique_sources.append(clean_source)
                seen.add(clean_source)
        
        return ', '.join(unique_sources)
    
    def generate_matching_report(self, original_count: int, matched_count: int) -> Dict:
        """Generate a report on the matching process"""
        duplicates_found = original_count - matched_count
        deduplication_rate = (duplicates_found / original_count * 100) if original_count > 0 else 0
        
        return {
            'original_count': original_count,
            'matched_count': matched_count,
            'duplicates_found': duplicates_found,
            'deduplication_rate': round(deduplication_rate, 2),
            'threshold_used': self.threshold,
            'fuzzy_matching_available': self.fuzzy_available
        }