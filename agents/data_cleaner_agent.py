"""
Data Cleaner Agent - Handles deterministic data cleaning and validation
"""
from typing import Dict, List, Any, Tuple
import pandas as pd
import re
from datetime import datetime
from dataclasses import dataclass


@dataclass
class CleaningRule:
    column: str
    rule_type: str
    pattern: str
    replacement: str
    validation_func: callable = None


@dataclass
class CleaningResult:
    column: str
    original_value: Any
    cleaned_value: Any
    rule_applied: str
    confidence: float


class DataCleanerAgent:
    def __init__(self, canonical_schema: Dict[str, str]):
        self.canonical_schema = canonical_schema
        self.cleaning_rules = self._initialize_cleaning_rules()
    
    def _initialize_cleaning_rules(self) -> Dict[str, List[CleaningRule]]:
        """Initialize cleaning rules for each canonical column"""
        rules = {
            'order_id': [
                CleaningRule('order_id', 'format', r'^ORD-\d+$', 'ORD-{id}', self._validate_order_id),
            ],
            'order_date': [
                CleaningRule('order_date', 'date_format', r'\d{4}-\d{2}-\d{2}', '{date}', self._validate_date),
                CleaningRule('order_date', 'date_format', r'\d{2}/\d{2}/\d{4}', '{date}', self._validate_date),
                CleaningRule('order_date', 'date_format', r'\d{2}-\w{3}-\d{4}', '{date}', self._validate_date),
            ],
            'customer_id': [
                CleaningRule('customer_id', 'format', r'^CUST-\d+$', 'CUST-{id}', self._validate_customer_id),
            ],
            'email': [
                CleaningRule('email', 'email_format', r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', '{email}', self._validate_email),
            ],
            'phone': [
                CleaningRule('phone', 'phone_format', r'^\+91-\d{10}$', '+91-{phone}', self._validate_phone),
            ],
            'postal_code': [
                CleaningRule('postal_code', 'numeric', r'^\d+$', '{code}', self._validate_postal_code),
            ],
            'quantity': [
                CleaningRule('quantity', 'numeric', r'^\d+$', '{qty}', self._validate_quantity),
            ],
            'unit_price': [
                CleaningRule('unit_price', 'currency', r'^[\d,]+\.?\d*$', '{price}', self._validate_price),
            ],
            'currency': [
                CleaningRule('currency', 'currency_code', r'^(INR|₹|Rs|inr)$', 'INR', self._validate_currency),
            ],
            'discount_pct': [
                CleaningRule('discount_pct', 'percentage', r'^[\d.]+%?$', '{pct}', self._validate_percentage),
            ],
            'tax_pct': [
                CleaningRule('tax_pct', 'percentage', r'^[\d.]+%?$', '{pct}', self._validate_percentage),
            ],
            'shipping_fee': [
                CleaningRule('shipping_fee', 'currency', r'^[\d,]+\.?\d*$', '{fee}', self._validate_price),
            ],
            'total_amount': [
                CleaningRule('total_amount', 'currency', r'^[\d,]+\.?\d*$', '{amount}', self._validate_price),
            ],
            'tax_id': [
                CleaningRule('tax_id', 'gstin_format', r'^\d{2}[A-Z]{5}\d{4}[A-Z]{1}[A-Z\d]{1}[Z]{1}[A-Z\d]{1}$', '{gstin}', self._validate_gstin),
            ]
        }
        return rules
    
    def clean_data(self, df: pd.DataFrame, mappings: List[Any]) -> Tuple[pd.DataFrame, List[CleaningResult]]:
        """Clean the dataframe based on mappings and rules"""
        cleaned_df = df.copy()
        cleaning_results = []
        
        # Apply column mappings
        for mapping in mappings:
            if hasattr(mapping, 'source_column') and hasattr(mapping, 'target_column'):
                if mapping.source_column in cleaned_df.columns:
                    cleaned_df = cleaned_df.rename(columns={mapping.source_column: mapping.target_column})
        
        # Apply cleaning rules
        for column in cleaned_df.columns:
            if column in self.cleaning_rules:
                column_results = self._clean_column(cleaned_df[column], column)
                cleaning_results.extend(column_results)
                
                # Apply the cleaning to the dataframe
                for result in column_results:
                    if result.rule_applied:
                        cleaned_df.loc[cleaned_df[column] == result.original_value, column] = result.cleaned_value
        
        return cleaned_df, cleaning_results
    
    def _clean_column(self, series: pd.Series, column_name: str) -> List[CleaningResult]:
        """Clean a specific column"""
        results = []
        rules = self.cleaning_rules.get(column_name, [])
        
        for value in series.unique():
            if pd.isna(value):
                continue
                
            original_value = value
            cleaned_value = value
            applied_rule = None
            
            for rule in rules:
                if self._apply_rule(value, rule):
                    cleaned_value = self._transform_value(value, rule)
                    applied_rule = rule.rule_type
                    break
            
            if applied_rule:
                results.append(CleaningResult(
                    column=column_name,
                    original_value=original_value,
                    cleaned_value=cleaned_value,
                    rule_applied=applied_rule,
                    confidence=0.9  # High confidence for deterministic rules
                ))
        
        return results
    
    def _apply_rule(self, value: Any, rule: CleaningRule) -> bool:
        """Check if a rule applies to a value"""
        try:
            if rule.rule_type == 'format':
                return bool(re.match(rule.pattern, str(value)))
            elif rule.rule_type == 'date_format':
                return self._is_valid_date_format(str(value))
            elif rule.rule_type == 'email_format':
                return bool(re.match(rule.pattern, str(value)))
            elif rule.rule_type == 'phone_format':
                return bool(re.match(rule.pattern, str(value)))
            elif rule.rule_type == 'numeric':
                return str(value).replace(',', '').replace('.', '').isdigit()
            elif rule.rule_type == 'currency':
                return bool(re.match(rule.pattern, str(value)))
            elif rule.rule_type == 'percentage':
                return bool(re.match(rule.pattern, str(value)))
            elif rule.rule_type == 'currency_code':
                return str(value).upper() in ['INR', '₹', 'RS', 'INR']
            elif rule.rule_type == 'gstin_format':
                return bool(re.match(rule.pattern, str(value)))
            return False
        except:
            return False
    
    def _transform_value(self, value: Any, rule: CleaningRule) -> Any:
        """Transform a value according to a rule"""
        try:
            if rule.rule_type == 'date_format':
                return self._normalize_date(str(value))
            elif rule.rule_type == 'currency':
                return self._normalize_currency(str(value))
            elif rule.rule_type == 'percentage':
                return self._normalize_percentage(str(value))
            elif rule.rule_type == 'currency_code':
                return 'INR'
            elif rule.rule_type == 'phone_format':
                return self._normalize_phone(str(value))
            elif rule.rule_type == 'numeric':
                return self._normalize_numeric(str(value))
            return value
        except:
            return value
    
    def _is_valid_date_format(self, date_str: str) -> bool:
        """Check if date string matches common formats"""
        formats = [
            '%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%d-%b-%Y', 
            '%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%d-%b-%Y'
        ]
        for fmt in formats:
            try:
                datetime.strptime(date_str, fmt)
                return True
            except ValueError:
                continue
        return False
    
    def _normalize_date(self, date_str: str) -> str:
        """Normalize date to ISO format"""
        formats = [
            ('%d/%m/%Y', '%Y-%m-%d'),
            ('%d-%m-%Y', '%Y-%m-%d'),
            ('%d-%b-%Y', '%Y-%m-%d'),
            ('%Y-%m-%d', '%Y-%m-%d')
        ]
        
        for input_fmt, output_fmt in formats:
            try:
                dt = datetime.strptime(date_str, input_fmt)
                return dt.strftime(output_fmt)
            except ValueError:
                continue
        
        return date_str
    
    def _normalize_currency(self, value: str) -> float:
        """Normalize currency values"""
        # Remove commas and currency symbols
        cleaned = re.sub(r'[,\s₹$]', '', value)
        return float(cleaned)
    
    def _normalize_percentage(self, value: str) -> float:
        """Normalize percentage values"""
        cleaned = value.replace('%', '')
        pct = float(cleaned)
        # Convert to decimal if > 1
        if pct > 1:
            pct = pct / 100
        return pct
    
    def _normalize_phone(self, value: str) -> str:
        """Normalize phone numbers"""
        # Extract digits
        digits = re.sub(r'\D', '', value)
        if len(digits) == 10:
            return f"+91-{digits}"
        return value
    
    def _normalize_numeric(self, value: str) -> int:
        """Normalize numeric values"""
        cleaned = re.sub(r'[,\s]', '', value)
        return int(float(cleaned))
    
    # Validation functions
    def _validate_order_id(self, value: str) -> bool:
        return bool(re.match(r'^ORD-\d+$', value))
    
    def _validate_customer_id(self, value: str) -> bool:
        return bool(re.match(r'^CUST-\d+$', value))
    
    def _validate_email(self, value: str) -> bool:
        return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', value))
    
    def _validate_phone(self, value: str) -> bool:
        return bool(re.match(r'^\+91-\d{10}$', value))
    
    def _validate_postal_code(self, value: str) -> bool:
        return bool(re.match(r'^\d{6}$', value))
    
    def _validate_quantity(self, value: str) -> bool:
        return str(value).isdigit() and int(value) > 0
    
    def _validate_price(self, value: str) -> bool:
        try:
            float(re.sub(r'[,\s]', '', value))
            return True
        except:
            return False
    
    def _validate_currency(self, value: str) -> bool:
        return value.upper() in ['INR', '₹', 'RS']
    
    def _validate_percentage(self, value: str) -> bool:
        try:
            pct = float(value.replace('%', ''))
            return 0 <= pct <= 1
        except:
            return False
    
    def _validate_gstin(self, value: str) -> bool:
        return bool(re.match(r'^\d{2}[A-Z]{5}\d{4}[A-Z]{1}[A-Z\d]{1}[Z]{1}[A-Z\d]{1}$', value))