"""
LLM Fix Agent - Provides targeted fix suggestions for remaining issues
"""
from typing import Dict, List, Any, Tuple
import pandas as pd
from dataclasses import dataclass
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage


@dataclass
class FixSuggestion:
    column: str
    row_index: int
    original_value: Any
    suggested_value: Any
    reason: str
    confidence: float
    fix_type: str  # 'format', 'semantic', 'validation'


class LLMFixAgent:
    def __init__(self, canonical_schema: Dict[str, str]):
        self.canonical_schema = canonical_schema
        self.llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.1)
        self.learned_fixes = self._load_learned_fixes()
    
    def _load_learned_fixes(self) -> Dict[str, List[Dict]]:
        """Load previously learned fixes from storage"""
        # In a real implementation, this would load from a database or file
        return {}
    
    def generate_fix_suggestions(self, df: pd.DataFrame, validation_errors: List[Dict]) -> List[FixSuggestion]:
        """Generate targeted fix suggestions for validation errors"""
        suggestions = []
        
        for error in validation_errors:
            column = error.get('column')
            row_index = error.get('row_index')
            value = error.get('value')
            error_type = error.get('error_type')
            
            if column in self.canonical_schema:
                suggestion = self._generate_single_fix(column, value, error_type, row_index)
                if suggestion:
                    suggestions.append(suggestion)
        
        return suggestions
    
    def _generate_single_fix(self, column: str, value: Any, error_type: str, row_index: int) -> FixSuggestion:
        """Generate a fix suggestion for a single value"""
        try:
            # Check if we have a learned fix for this pattern
            learned_fix = self._check_learned_fixes(column, value, error_type)
            if learned_fix:
                return FixSuggestion(
                    column=column,
                    row_index=row_index,
                    original_value=value,
                    suggested_value=learned_fix['suggested_value'],
                    reason=f"Learned fix: {learned_fix['reason']}",
                    confidence=learned_fix['confidence'],
                    fix_type='learned'
                )
            
            # Generate new fix using LLM
            llm_fix = self._generate_llm_fix(column, value, error_type)
            if llm_fix:
                return FixSuggestion(
                    column=column,
                    row_index=row_index,
                    original_value=value,
                    suggested_value=llm_fix['suggested_value'],
                    reason=llm_fix['reason'],
                    confidence=llm_fix['confidence'],
                    fix_type='llm'
                )
        
        except Exception as e:
            print(f"Error generating fix for {column}: {e}")
        
        return None
    
    def _check_learned_fixes(self, column: str, value: Any, error_type: str) -> Dict:
        """Check if we have a learned fix for this pattern"""
        key = f"{column}_{error_type}_{str(value)}"
        return self.learned_fixes.get(key)
    
    def _generate_llm_fix(self, column: str, value: Any, error_type: str) -> Dict:
        """Generate fix using LLM"""
        column_description = self.canonical_schema.get(column, "")
        
        prompt = f"""
        You are a data quality expert. I need help fixing a data validation error.
        
        Column: {column}
        Description: {column_description}
        Current Value: {value}
        Error Type: {error_type}
        
        Please suggest a corrected value and explain why. Consider:
        1. The expected format/pattern for this column
        2. Common data quality issues
        3. Business logic constraints
        
        Respond in this exact format:
        SUGGESTED_VALUE: [corrected value]
        REASON: [explanation]
        CONFIDENCE: [0.0-1.0]
        """
        
        try:
            response = self.llm.invoke([HumanMessage(content=prompt)])
            result = response.content.strip()
            
            # Parse the response
            lines = result.split('\n')
            suggested_value = None
            reason = ""
            confidence = 0.0
            
            for line in lines:
                if line.startswith('SUGGESTED_VALUE:'):
                    suggested_value = line.split(':', 1)[1].strip()
                elif line.startswith('REASON:'):
                    reason = line.split(':', 1)[1].strip()
                elif line.startswith('CONFIDENCE:'):
                    try:
                        confidence = float(line.split(':', 1)[1].strip())
                    except:
                        confidence = 0.5
            
            if suggested_value and reason:
                return {
                    'suggested_value': suggested_value,
                    'reason': reason,
                    'confidence': confidence
                }
        
        except Exception as e:
            print(f"LLM fix generation failed: {e}")
        
        return None
    
    def promote_fix(self, fix_suggestion: FixSuggestion) -> None:
        """Promote a fix to the learned fixes database"""
        key = f"{fix_suggestion.column}_{fix_suggestion.original_value}"
        
        self.learned_fixes[key] = {
            'suggested_value': fix_suggestion.suggested_value,
            'reason': fix_suggestion.reason,
            'confidence': fix_suggestion.confidence,
            'usage_count': self.learned_fixes.get(key, {}).get('usage_count', 0) + 1
        }
        
        # Save to persistent storage
        self._save_learned_fixes()
    
    def _save_learned_fixes(self) -> None:
        """Save learned fixes to persistent storage"""
        # In a real implementation, this would save to a database or file
        pass
    
    def validate_data_quality(self, df: pd.DataFrame) -> List[Dict]:
        """Validate data quality and return errors"""
        errors = []
        
        for column in df.columns:
            if column in self.canonical_schema:
                column_errors = self._validate_column(df[column], column)
                errors.extend(column_errors)
        
        return errors
    
    def _validate_column(self, series: pd.Series, column_name: str) -> List[Dict]:
        """Validate a specific column"""
        errors = []
        
        for idx, value in series.items():
            if pd.isna(value):
                continue
            
            validation_result = self._validate_value(value, column_name)
            if not validation_result['valid']:
                errors.append({
                    'column': column_name,
                    'row_index': idx,
                    'value': value,
                    'error_type': validation_result['error_type'],
                    'message': validation_result['message']
                })
        
        return errors
    
    def _validate_value(self, value: Any, column_name: str) -> Dict:
        """Validate a single value against column requirements"""
        value_str = str(value)
        
        # Define validation rules for each column type
        validation_rules = {
            'order_id': self._validate_order_id,
            'order_date': self._validate_date,
            'customer_id': self._validate_customer_id,
            'email': self._validate_email,
            'phone': self._validate_phone,
            'postal_code': self._validate_postal_code,
            'quantity': self._validate_quantity,
            'unit_price': self._validate_price,
            'currency': self._validate_currency,
            'discount_pct': self._validate_percentage,
            'tax_pct': self._validate_percentage,
            'shipping_fee': self._validate_price,
            'total_amount': self._validate_price,
            'tax_id': self._validate_gstin
        }
        
        validator = validation_rules.get(column_name)
        if validator:
            return validator(value_str)
        
        return {'valid': True, 'error_type': 'unknown', 'message': ''}
    
    # Validation functions
    def _validate_order_id(self, value: str) -> Dict:
        import re
        if not re.match(r'^ORD-\d+$', value):
            return {'valid': False, 'error_type': 'format', 'message': 'Order ID must be in format ORD-XXXX'}
        return {'valid': True, 'error_type': '', 'message': ''}
    
    def _validate_date(self, value: str) -> Dict:
        from datetime import datetime
        try:
            datetime.strptime(value, '%Y-%m-%d')
            return {'valid': True, 'error_type': '', 'message': ''}
        except ValueError:
            return {'valid': False, 'error_type': 'format', 'message': 'Date must be in YYYY-MM-DD format'}
    
    def _validate_customer_id(self, value: str) -> Dict:
        import re
        if not re.match(r'^CUST-\d+$', value):
            return {'valid': False, 'error_type': 'format', 'message': 'Customer ID must be in format CUST-XXXX'}
        return {'valid': True, 'error_type': '', 'message': ''}
    
    def _validate_email(self, value: str) -> Dict:
        import re
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', value):
            return {'valid': False, 'error_type': 'format', 'message': 'Invalid email format'}
        return {'valid': True, 'error_type': '', 'message': ''}
    
    def _validate_phone(self, value: str) -> Dict:
        import re
        if not re.match(r'^\+91-\d{10}$', value):
            return {'valid': False, 'error_type': 'format', 'message': 'Phone must be in format +91-XXXXXXXXXX'}
        return {'valid': True, 'error_type': '', 'message': ''}
    
    def _validate_postal_code(self, value: str) -> Dict:
        import re
        if not re.match(r'^\d{6}$', value):
            return {'valid': False, 'error_type': 'format', 'message': 'Postal code must be 6 digits'}
        return {'valid': True, 'error_type': '', 'message': ''}
    
    def _validate_quantity(self, value: str) -> Dict:
        try:
            qty = int(value)
            if qty <= 0:
                return {'valid': False, 'error_type': 'range', 'message': 'Quantity must be positive'}
            return {'valid': True, 'error_type': '', 'message': ''}
        except ValueError:
            return {'valid': False, 'error_type': 'format', 'message': 'Quantity must be a number'}
    
    def _validate_price(self, value: str) -> Dict:
        try:
            price = float(value)
            if price < 0:
                return {'valid': False, 'error_type': 'range', 'message': 'Price cannot be negative'}
            return {'valid': True, 'error_type': '', 'message': ''}
        except ValueError:
            return {'valid': False, 'error_type': 'format', 'message': 'Price must be a number'}
    
    def _validate_currency(self, value: str) -> Dict:
        if value.upper() not in ['INR', '₹', 'RS']:
            return {'valid': False, 'error_type': 'value', 'message': 'Currency must be INR, ₹, or Rs'}
        return {'valid': True, 'error_type': '', 'message': ''}
    
    def _validate_percentage(self, value: str) -> Dict:
        try:
            pct = float(value)
            if not (0 <= pct <= 1):
                return {'valid': False, 'error_type': 'range', 'message': 'Percentage must be between 0 and 1'}
            return {'valid': True, 'error_type': '', 'message': ''}
        except ValueError:
            return {'valid': False, 'error_type': 'format', 'message': 'Percentage must be a number'}
    
    def _validate_gstin(self, value: str) -> Dict:
        import re
        if not re.match(r'^\d{2}[A-Z]{5}\d{4}[A-Z]{1}[A-Z\d]{1}[Z]{1}[A-Z\d]{1}$', value):
            return {'valid': False, 'error_type': 'format', 'message': 'Invalid GSTIN format'}
        return {'valid': True, 'error_type': '', 'message': ''}