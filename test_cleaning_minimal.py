#!/usr/bin/env python3
"""
Minimal test for the cleaning endpoint
"""
import requests
import json

def test_minimal_cleaning():
    """Test with minimal data"""
    
    # Create minimal CSV content
    csv_content = "id,name,email\n1,John,john@email.com\n2,Jane,jane@email.com"
    
    # Test schema mapping first
    mapping_request = {
        'file_content': csv_content.encode().hex(),
        'custom_mappings': {}
    }
    
    print("Testing schema mapping...")
    mapping_response = requests.post('http://localhost:8000/map-schema', json=mapping_request)
    print(f"Mapping status: {mapping_response.status_code}")
    
    if mapping_response.status_code == 200:
        mapping_result = mapping_response.json()
        print(f"Mapping result: {mapping_result['suggested_mapping']}")
        
        # Test data cleaning
        cleaning_request = {
            'file_content': csv_content.encode().hex(),
            'column_mapping': mapping_result['suggested_mapping'],
            'cleaning_rules': {
                'trim_whitespace': True
            }
        }
        
        print("Testing data cleaning...")
        cleaning_response = requests.post('http://localhost:8000/clean-data', json=cleaning_request)
        print(f"Cleaning status: {cleaning_response.status_code}")
        print(f"Cleaning response: {cleaning_response.text}")
    else:
        print(f"Mapping failed: {mapping_response.text}")

if __name__ == "__main__":
    test_minimal_cleaning()