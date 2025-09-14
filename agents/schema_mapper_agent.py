"""
Schema Mapper Agent - Handles column mapping and confidence scoring
"""
from typing import Dict, List, Tuple, Any
import pandas as pd
import re
from dataclasses import dataclass
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage


@dataclass
class MappingResult:
    source_column: str
    target_column: str
    confidence: float
    mapping_type: str  # 'exact', 'fuzzy', 'semantic', 'manual'


class SchemaMapperAgent:
    def __init__(self, canonical_schema: Dict[str, str]):
        self.canonical_schema = canonical_schema
        self.llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)
        
        # Rule-based mapping patterns
        self.mapping_patterns = {
            'order_id': ['order', 'id', 'reference', 'ord', 'ref'],
            'order_date': ['date', 'ordered', 'created', 'timestamp'],
            'customer_id': ['customer', 'client', 'cust', 'user'],
            'customer_name': ['name', 'customer', 'client', 'fullname'],
            'email': ['email', 'e-mail', 'mail', 'contact'],
            'phone': ['phone', 'mobile', 'tel', 'contact'],
            'billing_address': ['bill', 'billing', 'address'],
            'shipping_address': ['ship', 'shipping', 'delivery'],
            'city': ['city', 'town', 'municipality'],
            'state': ['state', 'province', 'region'],
            'postal_code': ['postal', 'zip', 'pin', 'code'],
            'country': ['country', 'nation', 'region'],
            'product_sku': ['sku', 'code', 'product', 'item'],
            'product_name': ['product', 'item', 'name', 'desc'],
            'category': ['cat', 'category', 'type'],
            'subcategory': ['sub', 'subcat', 'subcategory'],
            'quantity': ['qty', 'quantity', 'units', 'amount'],
            'unit_price': ['price', 'unit', 'cost', 'rate'],
            'currency': ['currency', 'curr', 'symbol'],
            'discount_pct': ['discount', 'disc', 'off'],
            'tax_pct': ['tax', 'gst', 'vat', 'rate'],
            'shipping_fee': ['shipping', 'ship', 'logistics', 'fee'],
            'total_amount': ['total', 'grand', 'sum', 'amount'],
            'tax_id': ['tax', 'gstin', 'vat', 'reg', 'registration']
        }
    
    def map_columns(self, source_columns: List[str]) -> List[MappingResult]:
        """Map source columns to canonical schema with confidence scores"""
        mappings = []
        
        for source_col in source_columns:
            best_match = self._find_best_match(source_col)
            if best_match:
                mappings.append(best_match)
        
        return mappings
    
    def _find_best_match(self, source_column: str) -> MappingResult:
        """Find the best match for a source column"""
        source_lower = source_column.lower().strip()
        
        # Exact match
        for canonical_col, patterns in self.mapping_patterns.items():
            if source_lower in patterns or source_lower == canonical_col:
                return MappingResult(
                    source_column=source_column,
                    target_column=canonical_col,
                    confidence=1.0,
                    mapping_type='exact'
                )
        
        # Fuzzy match
        best_fuzzy = self._fuzzy_match(source_lower)
        if best_fuzzy and best_fuzzy[1] > 0.7:
            return MappingResult(
                source_column=source_column,
                target_column=best_fuzzy[0],
                confidence=best_fuzzy[1],
                mapping_type='fuzzy'
            )
        
        # Semantic match using LLM
        semantic_match = self._semantic_match(source_column)
        if semantic_match and semantic_match[1] > 0.6:
            return MappingResult(
                source_column=source_column,
                target_column=semantic_match[0],
                confidence=semantic_match[1],
                mapping_type='semantic'
            )
        
        return None
    
    def _fuzzy_match(self, source_column: str) -> Tuple[str, float]:
        """Perform fuzzy matching based on common patterns"""
        best_score = 0
        best_match = None
        
        for canonical_col, patterns in self.mapping_patterns.items():
            for pattern in patterns:
                # Check for substring matches
                if pattern in source_column or source_column in pattern:
                    score = len(pattern) / max(len(source_column), len(pattern))
                    if score > best_score:
                        best_score = score
                        best_match = canonical_col
                
                # Check for word overlap
                source_words = set(source_column.split())
                pattern_words = set(pattern.split())
                overlap = len(source_words.intersection(pattern_words))
                if overlap > 0:
                    score = overlap / max(len(source_words), len(pattern_words))
                    if score > best_score:
                        best_score = score
                        best_match = canonical_col
        
        return (best_match, best_score) if best_match else (None, 0)
    
    def _semantic_match(self, source_column: str) -> Tuple[str, float]:
        """Use LLM for semantic matching"""
        try:
            canonical_cols = list(self.canonical_schema.keys())
            canonical_descriptions = [self.canonical_schema[col] for col in canonical_cols]
            
            prompt = f"""
            Given the source column name "{source_column}" and these canonical columns with descriptions:
            
            {chr(10).join([f"- {col}: {desc}" for col, desc in zip(canonical_cols, canonical_descriptions)])}
            
            Which canonical column best matches the source column? Return only the column name and a confidence score (0-1).
            Format: COLUMN_NAME|CONFIDENCE_SCORE
            """
            
            response = self.llm.invoke([HumanMessage(content=prompt)])
            result = response.content.strip()
            
            if '|' in result:
                col_name, confidence = result.split('|')
                col_name = col_name.strip()
                confidence = float(confidence.strip())
                
                if col_name in canonical_cols and 0 <= confidence <= 1:
                    return (col_name, confidence)
        
        except Exception as e:
            print(f"LLM semantic matching failed: {e}")
        
        return (None, 0)
    
    def get_unmapped_columns(self, source_columns: List[str], mappings: List[MappingResult]) -> List[str]:
        """Get columns that couldn't be mapped"""
        mapped_source_cols = {m.source_column for m in mappings}
        return [col for col in source_columns if col not in mapped_source_cols]