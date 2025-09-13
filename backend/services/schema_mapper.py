import pandas as pd
from typing import List, Dict, Tuple
from fuzzywuzzy import fuzz, process
import re

class SchemaMapper:
    def __init__(self):
        self.synonym_mappings = {
            'customer_id': ['cust_id', 'client_id', 'id', 'customer_number', 'client_number'],
            'customer_name': ['customer name', 'full_name', 'full name', 'name', 'client_name', 'client name'],
            'email': ['email address', 'email_addr', 'e_mail', 'email_addr'],
            'phone': ['phone number', 'contact_phone', 'contact phone', 'telephone', 'tel'],
            'address': ['street address', 'street_addr', 'address_line', 'street'],
            'city': ['city name', 'town', 'municipality'],
            'state': ['state code', 'region', 'province', 'state_province'],
            'postal_code': ['zip code', 'zip', 'postal', 'postcode'],
            'country': ['country code', 'country_name', 'nation'],
            'registration_date': ['reg date', 'reg_date', 'date_registered', 'signup_date'],
            'tax_id': ['tax identification', 'vat_number', 'vat number', 'tax_number', 'reg no'],
            'annual_revenue': ['revenue', 'annual_revenue', 'yearly_revenue', 'income'],
            'industry': ['industry type', 'industry_sector', 'sector', 'business_type'],
            'employee_count': ['emp count', 'emp_count', 'staff_count', 'staff count', 'employees'],
            'status': ['customer status', 'client_status', 'client status', 'active_status']
        }
        
        self.confidence_thresholds = {
            'exact': 1.0,
            'synonym': 0.9,
            'fuzzy_high': 0.8,
            'fuzzy_medium': 0.6,
            'fuzzy_low': 0.4
        }

    def map_columns(self, source_columns: List[str], canonical_columns: List[str], 
                   custom_mappings: Dict[str, str] = None) -> Dict:
        """
        Map source columns to canonical schema with confidence scores
        """
        custom_mappings = custom_mappings or {}
        mapping = {}
        confidence_scores = {}
        unmapped_columns = []
        extra_columns = []
        
        # Apply custom mappings first
        for source_col, canonical_col in custom_mappings.items():
            if source_col in source_columns and canonical_col in canonical_columns:
                mapping[source_col] = canonical_col
                confidence_scores[source_col] = 1.0
                source_columns = [col for col in source_columns if col != source_col]
        
        # Map remaining columns
        for source_col in source_columns:
            best_match, confidence = self._find_best_match(source_col, canonical_columns)
            
            if confidence >= self.confidence_thresholds['fuzzy_low']:
                mapping[source_col] = best_match
                confidence_scores[source_col] = confidence
            else:
                unmapped_columns.append(source_col)
        
        # Identify extra columns (canonical columns not mapped)
        mapped_canonical = set(mapping.values())
        extra_columns = [col for col in canonical_columns if col not in mapped_canonical]
        
        return {
            'mapping': mapping,
            'confidence_scores': confidence_scores,
            'unmapped_columns': unmapped_columns,
            'extra_columns': extra_columns
        }
    
    def _find_best_match(self, source_col: str, canonical_columns: List[str]) -> Tuple[str, float]:
        """
        Find the best match for a source column in canonical columns
        """
        source_lower = source_col.lower().strip()
        
        # 1. Exact match
        for canonical_col in canonical_columns:
            if source_lower == canonical_col.lower():
                return canonical_col, self.confidence_thresholds['exact']
        
        # 2. Synonym match
        for canonical_col in canonical_columns:
            if canonical_col in self.synonym_mappings:
                synonyms = self.synonym_mappings[canonical_col]
                for synonym in synonyms:
                    if source_lower == synonym.lower():
                        return canonical_col, self.confidence_thresholds['synonym']
        
        # 3. Fuzzy matching
        best_match = None
        best_score = 0
        
        for canonical_col in canonical_columns:
            # Direct fuzzy match
            score = fuzz.ratio(source_lower, canonical_col.lower())
            
            # Check synonyms with fuzzy matching
            if canonical_col in self.synonym_mappings:
                for synonym in self.synonym_mappings[canonical_col]:
                    synonym_score = fuzz.ratio(source_lower, synonym.lower())
                    score = max(score, synonym_score)
            
            if score > best_score:
                best_score = score
                best_match = canonical_col
        
        # Normalize score to 0-1 range
        confidence = best_score / 100.0
        
        return best_match, confidence
    
    def get_mapping_explanation(self, source_col: str, canonical_col: str, confidence: float) -> str:
        """
        Generate explanation for why a mapping was suggested
        """
        if confidence == 1.0:
            return f"Exact match: '{source_col}' → '{canonical_col}'"
        elif confidence >= 0.9:
            return f"Synonym match: '{source_col}' → '{canonical_col}'"
        elif confidence >= 0.8:
            return f"High confidence fuzzy match: '{source_col}' → '{canonical_col}'"
        elif confidence >= 0.6:
            return f"Medium confidence fuzzy match: '{source_col}' → '{canonical_col}'"
        else:
            return f"Low confidence fuzzy match: '{source_col}' → '{canonical_col}'"