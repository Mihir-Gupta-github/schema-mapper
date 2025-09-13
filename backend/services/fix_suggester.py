import pandas as pd
import numpy as np
import re
import json
import os
from typing import List, Dict, Any, Tuple
from datetime import datetime
try:
    from langchain_community.llms import Ollama
except ImportError:
    from langchain.llms import Ollama
from langchain.prompts import PromptTemplate
from langchain.schema import BaseOutputParser

class FixSuggester:
    def __init__(self):
        self.ollama_model = "llama2"  # Default model
        self.llm = None
        self.cleaning_rules_file = "data/cleaning_rules.json"
        self.learned_fixes_file = "data/learned_fixes.json"
        self.cleaning_rules = self._load_cleaning_rules()
        self.learned_fixes = self._load_learned_fixes()
        
        # Initialize LLM if available
        try:
            self.llm = Ollama(model=self.ollama_model)
        except Exception as e:
            print(f"Warning: Could not initialize Ollama LLM: {e}")
            self.llm = None

    def suggest_fixes(self, df: pd.DataFrame, issues: List[Dict[str, Any]], 
                     column_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Generate targeted fix suggestions for remaining issues
        """
        fix_suggestions = []
        
        for issue in issues:
            column = issue['column']
            issue_type = issue['issue_type']
            invalid_rows = issue.get('invalid_rows', [])
            
            if issue_type == 'invalid_email':
                suggestions = self._suggest_email_fixes(df, column, invalid_rows)
            elif issue_type == 'format_validation':
                suggestions = self._suggest_format_fixes(df, column, invalid_rows, issue)
            elif issue_type == 'missing_values':
                suggestions = self._suggest_missing_value_fixes(df, column, invalid_rows)
            elif issue_type == 'duplicate_values':
                suggestions = self._suggest_duplicate_fixes(df, column, invalid_rows)
            else:
                suggestions = self._suggest_generic_fixes(df, column, invalid_rows, issue_type)
            
            fix_suggestions.extend(suggestions)
        
        # Apply learned fixes
        learned_suggestions = self._apply_learned_fixes(df, fix_suggestions)
        fix_suggestions.extend(learned_suggestions)
        
        return fix_suggestions
    
    def _suggest_email_fixes(self, df: pd.DataFrame, column: str, invalid_rows: List[int]) -> List[Dict[str, Any]]:
        """
        Suggest fixes for invalid email addresses
        """
        suggestions = []
        
        for row_idx in invalid_rows:
            if row_idx >= len(df):
                continue
                
            current_value = str(df.iloc[row_idx][column])
            
            # Common email fixes
            fixed_value = self._fix_email(current_value)
            
            if fixed_value != current_value:
                suggestions.append({
                    'row_index': row_idx,
                    'column': column,
                    'current_value': current_value,
                    'suggested_value': fixed_value,
                    'confidence': 0.8,
                    'reason': 'Applied common email formatting fixes',
                    'fix_type': 'email_format'
                })
            else:
                # Use AI for complex cases
                ai_suggestion = self._ai_suggest_email_fix(current_value)
                if ai_suggestion:
                    suggestions.append({
                        'row_index': row_idx,
                        'column': column,
                        'current_value': current_value,
                        'suggested_value': ai_suggestion,
                        'confidence': 0.6,
                        'reason': 'AI-suggested email fix',
                        'fix_type': 'ai_email_fix'
                    })
        
        return suggestions
    
    def _fix_email(self, email: str) -> str:
        """
        Apply deterministic email fixes
        """
        if not email or email.lower() == 'nan':
            return email
        
        # Remove extra spaces
        email = email.strip()
        
        # Fix common typos
        email = email.replace(' ', '')
        email = email.replace('..', '.')
        
        # Fix missing @
        if '@' not in email and '.' in email:
            parts = email.split('.')
            if len(parts) >= 2:
                email = f"{parts[0]}@{'.'.join(parts[1:])}"
        
        # Fix missing domain
        if '@' in email and '.' not in email.split('@')[1]:
            email = f"{email}.com"
        
        return email
    
    def _ai_suggest_email_fix(self, email: str) -> str:
        """
        Use AI to suggest email fixes for complex cases
        """
        if not self.llm:
            return None
        
        try:
            prompt = PromptTemplate(
                input_variables=["email"],
                template="""
                Fix this email address: "{email}"
                
                Rules:
                - Return only the fixed email address
                - If it cannot be fixed, return "INVALID"
                - Common fixes: add missing @, fix typos, add missing domain
                
                Fixed email:
                """
            )
            
            response = self.llm(prompt.format(email=email))
            fixed_email = response.strip()
            
            if fixed_email == "INVALID" or not fixed_email:
                return None
            
            return fixed_email
            
        except Exception as e:
            print(f"Error in AI email fix: {e}")
            return None
    
    def _suggest_format_fixes(self, df: pd.DataFrame, column: str, invalid_rows: List[int], 
                            issue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Suggest fixes for format validation issues
        """
        suggestions = []
        
        for row_idx in invalid_rows:
            if row_idx >= len(df):
                continue
                
            current_value = str(df.iloc[row_idx][column])
            
            # Apply learned fixes first
            learned_fix = self._get_learned_fix(column, current_value)
            if learned_fix:
                suggestions.append({
                    'row_index': row_idx,
                    'column': column,
                    'current_value': current_value,
                    'suggested_value': learned_fix,
                    'confidence': 0.9,
                    'reason': 'Applied learned fix pattern',
                    'fix_type': 'learned_pattern'
                })
                continue
            
            # Try deterministic fixes
            fixed_value = self._apply_deterministic_fixes(column, current_value)
            
            if fixed_value != current_value:
                suggestions.append({
                    'row_index': row_idx,
                    'column': column,
                    'current_value': current_value,
                    'suggested_value': fixed_value,
                    'confidence': 0.7,
                    'reason': 'Applied deterministic formatting rules',
                    'fix_type': 'deterministic_format'
                })
        
        return suggestions
    
    def _apply_deterministic_fixes(self, column: str, value: str) -> str:
        """
        Apply deterministic fixes based on column type
        """
        if not value or value.lower() == 'nan':
            return value
        
        column_lower = column.lower()
        
        if 'phone' in column_lower:
            # Clean phone number
            cleaned = re.sub(r'[^\d+]', '', value)
            if len(cleaned) == 10:
                return f"+1-{cleaned[:3]}-{cleaned[3:6]}-{cleaned[6:]}"
            elif len(cleaned) == 11 and cleaned.startswith('1'):
                return f"+1-{cleaned[1:4]}-{cleaned[4:7]}-{cleaned[7:]}"
        
        elif 'postal' in column_lower or 'zip' in column_lower:
            # Clean postal code
            cleaned = re.sub(r'[^\d-]', '', value)
            if len(cleaned) == 5:
                return cleaned
            elif len(cleaned) == 9:
                return f"{cleaned[:5]}-{cleaned[5:]}"
        
        elif 'tax' in column_lower or 'vat' in column_lower:
            # Clean tax ID
            cleaned = re.sub(r'[^\d-]', '', value)
            if len(cleaned) == 9:
                return f"{cleaned[:2]}-{cleaned[2:]}"
        
        elif 'revenue' in column_lower or 'income' in column_lower:
            # Clean currency
            cleaned = re.sub(r'[^\d.]', '', value)
            if cleaned:
                return cleaned
        
        return value
    
    def _suggest_missing_value_fixes(self, df: pd.DataFrame, column: str, invalid_rows: List[int]) -> List[Dict[str, Any]]:
        """
        Suggest fixes for missing values
        """
        suggestions = []
        
        # Get non-null values for pattern analysis
        non_null_values = df[column].dropna().astype(str).tolist()
        
        if not non_null_values:
            return suggestions
        
        # Find most common pattern
        most_common = max(set(non_null_values), key=non_null_values.count)
        
        for row_idx in invalid_rows:
            if row_idx >= len(df):
                continue
            
            suggestions.append({
                'row_index': row_idx,
                'column': column,
                'current_value': '',
                'suggested_value': most_common,
                'confidence': 0.5,
                'reason': f'Fill with most common value: {most_common}',
                'fix_type': 'missing_value_imputation'
            })
        
        return suggestions
    
    def _suggest_duplicate_fixes(self, df: pd.DataFrame, column: str, invalid_rows: List[int]) -> List[Dict[str, Any]]:
        """
        Suggest fixes for duplicate values
        """
        suggestions = []
        
        for row_idx in invalid_rows:
            if row_idx >= len(df):
                continue
            
            current_value = str(df.iloc[row_idx][column])
            
            # Suggest making it unique
            unique_value = f"{current_value}_{row_idx}"
            
            suggestions.append({
                'row_index': row_idx,
                'column': column,
                'current_value': current_value,
                'suggested_value': unique_value,
                'confidence': 0.6,
                'reason': 'Make value unique by adding row index',
                'fix_type': 'duplicate_resolution'
            })
        
        return suggestions
    
    def _suggest_generic_fixes(self, df: pd.DataFrame, column: str, invalid_rows: List[int], 
                             issue_type: str) -> List[Dict[str, Any]]:
        """
        Suggest generic fixes for other issue types
        """
        suggestions = []
        
        for row_idx in invalid_rows:
            if row_idx >= len(df):
                continue
            
            current_value = str(df.iloc[row_idx][column])
            
            # Basic cleaning
            cleaned_value = current_value.strip()
            
            if cleaned_value != current_value:
                suggestions.append({
                    'row_index': row_idx,
                    'column': column,
                    'current_value': current_value,
                    'suggested_value': cleaned_value,
                    'confidence': 0.5,
                    'reason': 'Basic whitespace cleaning',
                    'fix_type': 'basic_cleaning'
                })
        
        return suggestions
    
    def _apply_learned_fixes(self, df: pd.DataFrame, existing_suggestions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply learned fixes from previous sessions
        """
        suggestions = []
        
        for column in df.columns:
            for idx, value in df[column].items():
                learned_fix = self._get_learned_fix(column, str(value))
                if learned_fix and learned_fix != str(value):
                    # Check if we already have a suggestion for this row/column
                    existing = any(s['row_index'] == idx and s['column'] == column 
                                 for s in existing_suggestions)
                    if not existing:
                        suggestions.append({
                            'row_index': idx,
                            'column': column,
                            'current_value': str(value),
                            'suggested_value': learned_fix,
                            'confidence': 0.9,
                            'reason': 'Applied learned fix pattern',
                            'fix_type': 'learned_pattern'
                        })
        
        return suggestions
    
    def _get_learned_fix(self, column: str, value: str) -> str:
        """
        Get learned fix for a column/value combination
        """
        if column in self.learned_fixes:
            for pattern, fix in self.learned_fixes[column].items():
                if re.search(pattern, value, re.IGNORECASE):
                    return fix
        return None
    
    def promote_fix(self, fix_type: str, pattern: str, solution: str, confidence: float) -> bool:
        """
        Promote a fix to the cleaning rules for future use
        """
        try:
            # Add to learned fixes
            if fix_type not in self.learned_fixes:
                self.learned_fixes[fix_type] = {}
            
            self.learned_fixes[fix_type][pattern] = solution
            
            # Save learned fixes
            self._save_learned_fixes()
            
            return True
            
        except Exception as e:
            print(f"Error promoting fix: {e}")
            return False
    
    def get_cleaning_rules(self) -> Dict[str, Any]:
        """
        Get current cleaning rules
        """
        return self.cleaning_rules
    
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
    
    def _load_learned_fixes(self) -> Dict[str, Any]:
        """
        Load learned fixes from JSON file
        """
        if os.path.exists(self.learned_fixes_file):
            try:
                with open(self.learned_fixes_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading learned fixes: {e}")
        
        return {}
    
    def _save_learned_fixes(self):
        """
        Save learned fixes to JSON file
        """
        os.makedirs(os.path.dirname(self.learned_fixes_file), exist_ok=True)
        with open(self.learned_fixes_file, 'w') as f:
            json.dump(self.learned_fixes, f, indent=2)