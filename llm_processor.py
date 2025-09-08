# llm_processor.py
# LLM integration for data enrichment and classification

import logging
import re
from typing import Optional, List, Dict
from collections import Counter

# LLM imports
try:
    from transformers import pipeline, AutoTokenizer, AutoModelForCausalLM
    import torch
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False
    logging.warning("Transformers not available. LLM features will be limited.")

# NLTK for text processing
try:
    import nltk
    from nltk.corpus import stopwords
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False
    logging.warning("NLTK not available. Text processing will be limited.")

logger = logging.getLogger(__name__)

class LLMProcessor:
    """Handles LLM integration for data enrichment"""
    
    def __init__(self, model_name: str = "microsoft/DialoGPT-medium"):
        self.model_name = model_name
        self.generator = None
        self.tokenizer = None
        self.model = None
        
        # Industry categories for classification
        self.industry_categories = [
            'Technology', 'Finance', 'Healthcare', 'Manufacturing', 
            'Retail', 'Education', 'Real Estate', 'Transportation', 
            'Food & Beverage', 'Professional Services', 'Construction',
            'Media & Entertainment', 'Energy', 'Telecommunications',
            'Agriculture', 'Automotive', 'Aerospace', 'Tourism'
        ]
        
        if TRANSFORMERS_AVAILABLE:
            self.setup_model()
        else:
            logger.warning("Transformers not available. Using fallback methods.")
    
    def setup_model(self):
        """Setup the LLM model"""
        try:
            logger.info(f"Loading LLM model: {self.model_name}")
            
            # For lightweight deployment, we'll use a simple approach
            # In production, consider using larger models like Llama-2 or Mistral
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            
            # Add padding token if not present
            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token
            
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_name,
                torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
                device_map="auto" if torch.cuda.is_available() else None
            )
            
            # Setup text generation pipeline
            self.generator = pipeline(
                'text-generation',
                model=self.model,
                tokenizer=self.tokenizer,
                max_length=150,
                temperature=0.7,
                do_sample=True,
                pad_token_id=self.tokenizer.eos_token_id
            )
            
            logger.info(f"LLM model {self.model_name} loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load LLM model: {e}")
            self.generator = None
            logger.info("Falling back to rule-based classification")
    
    def classify_industry(self, company_description: str, company_name: str = "") -> str:
        """Classify company industry using LLM or fallback methods"""
        
        # Combine company name and description for better context
        full_description = f"{company_name} {company_description}".strip()
        
        if self.generator and len(full_description) > 10:
            return self._classify_with_llm(full_description)
        else:
            return self._classify_with_rules(full_description)
    
    def _classify_with_llm(self, description: str) -> str:
        """Classify industry using LLM"""
        try:
            # Create a focused prompt for industry classification
            prompt = f"""Based on the company description below, classify it into ONE of these industries:
{', '.join(self.industry_categories)}

Company description: {description[:200]}

The industry is:"""
            
            # Generate response
            result = self.generator(
                prompt, 
                max_length=len(prompt.split()) + 15,
                num_return_sequences=1,
                temperature=0.3  # Lower temperature for more focused results
            )
            
            response = result[0]['generated_text'][len(prompt):].strip()
            
            # Extract industry from response
            for industry in self.industry_categories:
                if industry.lower() in response.lower():
                    return industry
            
            # Fallback to rule-based if LLM doesn't return valid industry
            return self._classify_with_rules(description)
            
        except Exception as e:
            logger.error(f"LLM classification failed: {e}")
            return self._classify_with_rules(description)
    
    def _classify_with_rules(self, description: str) -> str:
        """Rule-based industry classification as fallback"""
        description_lower = description.lower()
        
        # Define keyword patterns for each industry
        industry_keywords = {
            'Technology': ['software', 'tech', 'digital', 'computer', 'programming', 'app', 'web', 'it', 'cyber', 'data', 'ai', 'artificial intelligence'],
            'Finance': ['bank', 'finance', 'investment', 'insurance', 'fund', 'capital', 'trading', 'financial', 'loan', 'credit'],
            'Healthcare': ['health', 'medical', 'hospital', 'clinic', 'healthcare', 'pharmaceutical', 'drug', 'medicine', 'therapy'],
            'Manufacturing': ['manufacturing', 'factory', 'production', 'industrial', 'machinery', 'equipment', 'assembly'],
            'Retail': ['retail', 'shop', 'store', 'sales', 'commerce', 'trading', 'merchandise', 'goods'],
            'Education': ['education', 'school', 'university', 'training', 'learning', 'academic', 'teaching', 'course'],
            'Real Estate': ['real estate', 'property', 'housing', 'construction', 'building', 'development', 'land'],
            'Transportation': ['transport', 'logistics', 'shipping', 'delivery', 'freight', 'cargo', 'aviation', 'maritime'],
            'Food & Beverage': ['food', 'restaurant', 'catering', 'beverage', 'dining', 'culinary', 'cafe', 'bar'],
            'Professional Services': ['consulting', 'advisory', 'legal', 'accounting', 'audit', 'professional', 'services'],
            'Construction': ['construction', 'contractor', 'building', 'civil', 'engineering', 'infrastructure'],
            'Media & Entertainment': ['media', 'entertainment', 'advertising', 'marketing', 'creative', 'design', 'agency'],
            'Energy': ['energy', 'oil', 'gas', 'renewable', 'solar', 'power', 'utility', 'electricity'],
            'Telecommunications': ['telecom', 'telecommunications', 'mobile', 'network', 'communication', 'broadband']
        }
        
        # Score each industry based on keyword matches
        industry_scores = {}
        for industry, keywords in industry_keywords.items():
            score = sum(1 for keyword in keywords if keyword in description_lower)
            if score > 0:
                industry_scores[industry] = score
        
        # Return industry with highest score
        if industry_scores:
            return max(industry_scores, key=industry_scores.get)
        
        return "Professional Services"  # Default fallback
    
    def extract_keywords(self, text: str, max_keywords: int = 10) -> str:
        """Extract relevant keywords from company text"""
        if not text or len(text.strip()) < 10:
            return ""
        
        try:
            if NLTK_AVAILABLE:
                return self._extract_keywords_nltk(text, max_keywords)
            else:
                return self._extract_keywords_simple(text, max_keywords)
                
        except Exception as e:
            logger.error(f"Keyword extraction failed: {e}")
            return ""
    
    def _extract_keywords_nltk(self, text: str, max_keywords: int) -> str:
        """Extract keywords using NLTK"""
        try:
            # Download required NLTK data if not present
            try:
                nltk.data.find('corpora/stopwords')
            except LookupError:
                nltk.download('stopwords', quiet=True)
            
            try:
                nltk.data.find('tokenizers/punkt')
            except LookupError:
                nltk.download('punkt', quiet=True)
            
            stop_words = set(stopwords.words('english'))
            
            # Add domain-specific stop words
            stop_words.update({
                'company', 'ltd', 'pte', 'singapore', 'services', 'solutions',
                'group', 'holdings', 'international', 'private', 'limited'
            })
            
            # Extract words (3+ characters, alphabetic)
            words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
            
            # Filter stop words and common business terms
            filtered_words = [word for word in words if word not in stop_words]
            
            # Count frequency and get most common
            word_freq = Counter(filtered_words)
            keywords = [word for word, freq in word_freq.most_common(max_keywords)]
            
            return ', '.join(keywords)
            
        except Exception as e:
            logger.error(f"NLTK keyword extraction failed: {e}")
            return self._extract_keywords_simple(text, max_keywords)
    
    def _extract_keywords_simple(self, text: str, max_keywords: int) -> str:
        """Simple keyword extraction without NLTK"""
        # Basic stop words
        stop_words = {
            'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
            'by', 'from', 'up', 'about', 'into', 'through', 'during', 'before',
            'after', 'above', 'below', 'between', 'among', 'company', 'ltd', 'pte',
            'singapore', 'services', 'solutions', 'group', 'holdings', 'international',
            'private', 'limited', 'our', 'we', 'us', 'you', 'your', 'they', 'their'
        }
        
        # Extract meaningful words
        words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
        filtered_words = [word for word in words if word not in stop_words]
        
        # Get unique words (preserve order of first occurrence)
        unique_words = list(dict.fromkeys(filtered_words))
        
        # Return first max_keywords words
        return ', '.join(unique_words[:max_keywords])
    
    def determine_company_size(self, employees: Optional[int], revenue: Optional[str], 
                              description: str = "") -> str:
        """Determine company size based on available information"""
        
        # Primary classification by employee count
        if employees is not None:
            if employees < 50:
                return "Small"
            elif employees < 250:
                return "Medium" 
            else:
                return "Large"
        
        # Secondary classification by revenue if available
        if revenue:
            revenue_lower = revenue.lower()
            # Look for revenue indicators
            if any(indicator in revenue_lower for indicator in ['million', 'billion']):
                if 'billion' in revenue_lower:
                    return "Large"
                elif 'million' in revenue_lower:
                    # Extract number before million
                    numbers = re.findall(r'(\d+(?:\.\d+)?)', revenue_lower)
                    if numbers:
                        try:
                            amount = float(numbers[0])
                            if amount >= 100:  # 100+ million
                                return "Large"
                            elif amount >= 10:  # 10-100 million
                                return "Medium"
                            else:
                                return "Small"
                        except ValueError:
                            pass
        
        # Tertiary classification by description keywords
        if description:
            description_lower = description.lower()
            large_indicators = ['multinational', 'global', 'international', 'enterprise', 'corporation']
            small_indicators = ['startup', 'boutique', 'local', 'family', 'small']
            
            if any(indicator in description_lower for indicator in large_indicators):
                return "Large"
            elif any(indicator in description_lower for indicator in small_indicators):
                return "Small"
        
        return "Unknown"
    
    def enhance_company_data(self, company_data: Dict) -> Dict:
        """Enhance company data using LLM processing"""
        enhanced_data = company_data.copy()
        
        # Prepare description for LLM processing
        description_parts = []
        if company_data.get('services_offered'):
            description_parts.append(company_data['services_offered'])
        if company_data.get('products_offered'):
            description_parts.append(company_data['products_offered'])
        
        description = ' '.join(description_parts)
        company_name = company_data.get('company_name', '')
        
        # Industry classification
        if not enhanced_data.get('industry') and description:
            enhanced_data['industry'] = self.classify_industry(description, company_name)
        
        # Keyword extraction
        if not enhanced_data.get('keywords') and description:
            enhanced_data['keywords'] = self.extract_keywords(description)
        
        # Company size determination
        if not enhanced_data.get('company_size'):
            enhanced_data['company_size'] = self.determine_company_size(
                enhanced_data.get('number_of_employees'),
                enhanced_data.get('revenue'),
                description
            )
        
        return enhanced_data
    
    def cleanup(self):
        """Clean up model resources"""
        if self.model:
            del self.model
        if self.tokenizer:
            del self.tokenizer
        if self.generator:
            del self.generator
        
        # Clear CUDA cache if using GPU
        if torch.cuda.is_available():
            torch.cuda.empty_cache()