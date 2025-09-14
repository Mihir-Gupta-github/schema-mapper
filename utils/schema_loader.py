"""
Schema loader utility
"""
import pandas as pd
from typing import Dict
from pathlib import Path


def load_canonical_schema(schema_path: Path) -> Dict[str, str]:
    """Load canonical schema from CSV file"""
    try:
        df = pd.read_csv(schema_path)
        
        # Create schema dictionary from canonical_name and description columns
        schema = {}
        for _, row in df.iterrows():
            canonical_name = row['canonical_name']
            description = row['description']
            schema[canonical_name] = description
        
        return schema
    
    except Exception as e:
        raise Exception(f"Error loading canonical schema: {e}")


def get_schema_examples(schema_path: Path) -> Dict[str, str]:
    """Get example values for each canonical column"""
    try:
        df = pd.read_csv(schema_path)
        
        examples = {}
        for _, row in df.iterrows():
            canonical_name = row['canonical_name']
            example = row['example']
            examples[canonical_name] = example
        
        return examples
    
    except Exception as e:
        raise Exception(f"Error loading schema examples: {e}")