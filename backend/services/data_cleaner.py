import pandas as pd
import numpy as np
import re
from datetime import datetime
from typing import Dict, List, Any, Tuple
import json
import os

class DataCleaner:
    def __init__(self):
        self.cleaning_rules_file = "data/cleaning_rules.json"
        self.cleaning_rules = self._load_cleaning_rules()
        
        # Standard cleaning patterns
        self.default_patterns = {
            'phone': r'^\+?1?[-.\s]?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})$',
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'postal_code': r'^\d{5}(-\d{4})?$',
            'tax_id': r'^\d{2}-\d{7}$',
            'date_formats': [
                '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d',
                '%d-%m-%Y', '%m-%d-%Y', '%Y-%m-%d %H:%M:%S',
                '%d %b %Y', '%b %d, %Y', '%B %d, %Y'
            ]
        }

    def clean_dataframe(self, df: pd.DataFrame, column_mapping: Dict[str, str], 
                       custom_rules: Dict[str, Any] = None) -> Dict:
        """
        Clean and validate the dataframe according to mapping and rules
        """
        custom_rules = custom_rules or {}
        
        # Store original metrics
        before_metrics = self._calculate_metrics(df)
        
        # Apply column mapping
        df_cleaned = df.rename(columns=column_mapping)
        
        # Apply cleaning rules
        cleaning_applied = []
        issues_found = []
        
        for column in df_cleaned.columns:
            if column in self.cleaning_rules:
                column_issues, column_cleaning = self._clean_column(
                    df_cleaned, column, self.cleaning_rules[column]
                )
                issues_found.extend(column_issues)
                cleaning_applied.extend(column_cleaning)
            elif column in custom_rules:
                column_issues, column_cleaning = self._clean_column(
                    df_cleaned, column, custom_rules[column]
                )
                issues_found.extend(column_issues)
                cleaning_applied.extend(column_cleaning)
            else:
                # Apply default cleaning based on column name
                column_issues, column_cleaning = self._apply_default_cleaning(
                    df_cleaned, column
                )
                issues_found.extend(column_issues)
                cleaning_applied.extend(column_cleaning)
        
        # Calculate final metrics
        after_metrics = self._calculate_metrics(df_cleaned)
        
        return {
            'cleaned_df': df_cleaned,
            'before_metrics': before_metrics,
            'after_metrics': after_metrics,
            'issues_found': issues_found,
            'cleaning_applied': cleaning_applied
        }
    
    def _clean_column(self, df: pd.DataFrame, column: str, rules: Dict[str, Any]) -> Tuple[List, List]:
        """
        Apply specific cleaning rules to a column
        """
        issues = []
        cleaning_applied = []
        
        for rule_type, rule_config in rules.items():
            if rule_type == 'trim_whitespace':
                if rule_config:
                    original_nulls = df[column].isnull().sum()
                    df[column] = df[column].astype(str).str.strip()
                    df[column] = df[column].replace('', np.nan)
                    new_nulls = df[column].isnull().sum()
                    if new_nulls > original_nulls:
                        cleaning_applied.append({
                            'column': column,
                            'rule': 'trim_whitespace',
                            'description': f'Trimmed whitespace, created {int(new_nulls - original_nulls)} nulls'
                        })
            
            elif rule_type == 'normalize_case':
                if rule_config == 'lower':
                    df[column] = df[column].astype(str).str.lower()
                    cleaning_applied.append({
                        'column': column,
                        'rule': 'normalize_case',
                        'description': 'Converted to lowercase'
                    })
                elif rule_config == 'title':
                    df[column] = df[column].astype(str).str.title()
                    cleaning_applied.append({
                        'column': column,
                        'rule': 'normalize_case',
                        'description': 'Converted to title case'
                    })
            
            elif rule_type == 'remove_currency':
                if rule_config:
                    original_values = df[column].copy()
                    df[column] = df[column].astype(str).str.replace(r'[$,\s]', '', regex=True)
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                    changed_count = (original_values != df[column].astype(str)).sum()
                    if changed_count > 0:
                        cleaning_applied.append({
                            'column': column,
                            'rule': 'remove_currency',
                            'description': f'Removed currency symbols from {int(changed_count)} values'
                        })
            
            elif rule_type == 'normalize_date':
                if rule_config:
                    df[column] = self._normalize_dates(df[column])
                    cleaning_applied.append({
                        'column': column,
                        'rule': 'normalize_date',
                        'description': 'Normalized date formats'
                    })
            
            elif rule_type == 'validate_format':
                if rule_config:
                    invalid_mask = ~df[column].astype(str).str.match(rule_config, na=False)
                    invalid_count = invalid_mask.sum()
            if invalid_count > 0:
                issues.append({
                    'column': column,
                    'issue_type': 'format_validation',
                    'count': int(invalid_count),
                    'description': f'{invalid_count} values do not match expected format',
                    'invalid_rows': [int(idx) for idx in df[invalid_mask].index.tolist()]
                })
        
        return issues, cleaning_applied
    
    def _apply_default_cleaning(self, df: pd.DataFrame, column: str) -> Tuple[List, List]:
        """
        Apply default cleaning based on column name patterns
        """
        issues = []
        cleaning_applied = []
        
        # Trim whitespace for all columns
        original_nulls = df[column].isnull().sum()
        df[column] = df[column].astype(str).str.strip()
        df[column] = df[column].replace('', np.nan)
        new_nulls = df[column].isnull().sum()
        if new_nulls > original_nulls:
            cleaning_applied.append({
                'column': column,
                'rule': 'trim_whitespace',
                'description': f'Trimmed whitespace, created {int(new_nulls - original_nulls)} nulls'
            })
        
        # Column-specific cleaning
        column_lower = column.lower()
        
        if 'email' in column_lower:
            # Validate email format
            email_pattern = self.default_patterns['email']
            invalid_emails = ~df[column].astype(str).str.match(email_pattern, na=True)
            invalid_count = invalid_emails.sum()
            if invalid_count > 0:
                issues.append({
                    'column': column,
                    'issue_type': 'invalid_email',
                    'count': int(invalid_count),
                    'description': f'{invalid_count} invalid email addresses',
                    'invalid_rows': [int(idx) for idx in df[invalid_emails].index.tolist()]
                })
        
        elif 'phone' in column_lower:
            # Clean phone numbers
            original_values = df[column].copy()
            df[column] = df[column].astype(str).str.replace(r'[^\d+]', '', regex=True)
            changed_count = (original_values != df[column]).sum()
            if changed_count > 0:
                cleaning_applied.append({
                    'column': column,
                    'rule': 'clean_phone',
                    'description': f'Cleaned phone numbers for {int(changed_count)} values'
                })
        
        elif 'revenue' in column_lower or 'income' in column_lower:
            # Remove currency symbols and convert to numeric
            original_values = df[column].copy()
            df[column] = df[column].astype(str).str.replace(r'[$,\s]', '', regex=True)
            df[column] = pd.to_numeric(df[column], errors='coerce')
            changed_count = (original_values != df[column].astype(str)).sum()
            if changed_count > 0:
                cleaning_applied.append({
                    'column': column,
                    'rule': 'remove_currency',
                    'description': f'Removed currency symbols from {int(changed_count)} values'
                })
        
        elif 'date' in column_lower:
            # Normalize dates
            df[column] = self._normalize_dates(df[column])
            cleaning_applied.append({
                'column': column,
                'rule': 'normalize_date',
                'description': 'Normalized date formats'
            })
        
        elif 'postal' in column_lower or 'zip' in column_lower:
            # Clean postal codes
            original_values = df[column].copy()
            df[column] = df[column].astype(str).str.replace(r'[^\d-]', '', regex=True)
            changed_count = (original_values != df[column]).sum()
            if changed_count > 0:
                cleaning_applied.append({
                    'column': column,
                    'rule': 'clean_postal_code',
                    'description': f'Cleaned postal codes for {int(changed_count)} values'
                })
        
        return issues, cleaning_applied
    
    def _normalize_dates(self, series: pd.Series) -> pd.Series:
        """
        Normalize various date formats to standard format
        """
        normalized = pd.Series(index=series.index, dtype='object')
        
        for idx, value in series.items():
            if pd.isna(value) or value == '':
                normalized[idx] = np.nan
                continue
            
            value_str = str(value).strip()
            parsed_date = None
            
            # Try different date formats
            for fmt in self.default_patterns['date_formats']:
                try:
                    parsed_date = datetime.strptime(value_str, fmt)
                    break
                except ValueError:
                    continue
            
            if parsed_date:
                normalized[idx] = parsed_date.strftime('%Y-%m-%d')
            else:
                normalized[idx] = value_str  # Keep original if can't parse
        
        return normalized
    
    def _calculate_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate data quality metrics
        """
        total_rows = len(df)
        total_cells = df.size
        
        # Convert numpy types to native Python types
        null_cells = int(df.isnull().sum().sum())
        duplicate_rows = int(df.duplicated().sum())
        total_rows_int = int(total_rows)
        total_cells_int = int(total_cells)
        
        metrics = {
            'total_rows': total_rows_int,
            'total_columns': int(len(df.columns)),
            'total_cells': total_cells_int,
            'null_cells': null_cells,
            'null_percentage': float((null_cells / total_cells_int) * 100),
            'duplicate_rows': duplicate_rows,
            'duplicate_percentage': float((duplicate_rows / total_rows_int) * 100),
            'column_metrics': {}
        }
        
        for column in df.columns:
            null_count = int(df[column].isnull().sum())
            unique_count = int(df[column].nunique())
            
            col_metrics = {
                'null_count': null_count,
                'null_percentage': float((null_count / total_rows_int) * 100),
                'unique_count': unique_count,
                'unique_percentage': float((unique_count / total_rows_int) * 100),
                'data_type': str(df[column].dtype)
            }
            metrics['column_metrics'][column] = col_metrics
        
        return metrics
    
    def _load_cleaning_rules(self) -> Dict[str, Any]:
        """
        Load cleaning rules from JSON file
        """
        if os.path.exists(self.cleaning_rules_file):
            try:
                with open(self.cleaning_rules_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading cleaning rules: {e}")
        
        return {}
    
    def save_cleaning_rules(self, rules: Dict[str, Any]):
        """
        Save cleaning rules to JSON file
        """
        os.makedirs(os.path.dirname(self.cleaning_rules_file), exist_ok=True)
        with open(self.cleaning_rules_file, 'w') as f:
            json.dump(rules, f, indent=2)