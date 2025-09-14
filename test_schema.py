#!/usr/bin/env python3

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from src.schema_loader import SchemaLoader

def test_schema_loading():
    """Test schema loading directly."""
    print("Testing schema loading...")
    
    schema_loader = SchemaLoader()
    schema_path = "canonicalSchema/Project6StdFormat.csv"
    
    print(f"Schema path: {schema_path}")
    print(f"Path exists: {Path(schema_path).exists()}")
    
    if Path(schema_path).exists():
        print("File exists, attempting to load...")
        success = schema_loader.load_schema(schema_path)
        print(f"Load success: {success}")
        
        if success:
            columns = schema_loader.get_all_columns()
            print(f"Loaded {len(columns)} columns")
            print(f"First 5 columns: {columns[:5]}")
        else:
            print("Failed to load schema")
    else:
        print("Schema file not found")

if __name__ == "__main__":
    test_schema_loading()