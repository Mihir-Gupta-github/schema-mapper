#!/usr/bin/env python3
"""
Comprehensive test script for the Schema Mapper & Data Quality Fixer application
"""
import requests
import json
import time
import os

API_BASE_URL = "http://localhost:8000"

def test_api_health():
    """Test if the API is running"""
    try:
        response = requests.get(f"{API_BASE_URL}/", timeout=5)
        if response.status_code == 200:
            print("✅ API is running")
            return True
        else:
            print(f"❌ API health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Cannot connect to API: {e}")
        return False

def test_canonical_schema():
    """Test canonical schema endpoint"""
    try:
        response = requests.get(f"{API_BASE_URL}/canonical-schema", timeout=5)
        if response.status_code == 200:
            schema_data = response.json()
            print(f"✅ Canonical schema loaded: {len(schema_data['columns'])} columns")
            return schema_data
        else:
            print(f"❌ Canonical schema failed: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error loading canonical schema: {e}")
        return None

def test_schema_mapping(filename):
    """Test schema mapping with a sample file"""
    try:
        with open(filename, 'rb') as f:
            file_content = f.read()
        
        mapping_request = {
            'file_content': file_content.hex(),
            'custom_mappings': {}
        }
        
        response = requests.post(f"{API_BASE_URL}/map-schema", json=mapping_request, timeout=10)
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Schema mapping successful for {filename}")
            print(f"   Mappings: {len(result['suggested_mapping'])}")
            print(f"   Confidence scores: {list(result['confidence_scores'].values())[:3]}...")
            return result
        else:
            print(f"❌ Schema mapping failed for {filename}: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error testing schema mapping for {filename}: {e}")
        return None

def test_data_cleaning(filename, mapping_result):
    """Test data cleaning with mapping results"""
    try:
        with open(filename, 'rb') as f:
            file_content = f.read()
        
        cleaning_request = {
            'file_content': file_content.hex(),
            'column_mapping': mapping_result['suggested_mapping'],
            'cleaning_rules': {
                'trim_whitespace': True,
                'normalize_case': 'title',
                'remove_currency': True,
                'normalize_dates': True,
                'validate_emails': True,
                'clean_phone_numbers': True
            }
        }
        
        response = requests.post(f"{API_BASE_URL}/clean-data", json=cleaning_request, timeout=15)
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Data cleaning successful for {filename}")
            print(f"   Before: {result['before_metrics']['total_rows']} rows, {result['before_metrics']['null_percentage']:.1f}% nulls")
            print(f"   After: {result['after_metrics']['total_rows']} rows, {result['after_metrics']['null_percentage']:.1f}% nulls")
            print(f"   Issues found: {len(result['issues_found'])}")
            print(f"   Cleaning applied: {len(result['cleaning_applied'])}")
            return result
        else:
            print(f"❌ Data cleaning failed for {filename}: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error testing data cleaning for {filename}: {e}")
        return None

def test_fix_suggestions(cleaning_result):
    """Test fix suggestions"""
    try:
        if not cleaning_result or not cleaning_result.get('issues_found'):
            print("ℹ️ No issues found, skipping fix suggestions test")
            return None
        
        fix_request = {
            'cleaned_file_path': cleaning_result['cleaned_file_path'],
            'issues': cleaning_result['issues_found'],
            'column_mapping': {}  # This would come from the mapping result
        }
        
        response = requests.post(f"{API_BASE_URL}/suggest-fixes", json=fix_request, timeout=10)
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Fix suggestions generated: {len(result['fix_suggestions'])} suggestions")
            return result
        else:
            print(f"❌ Fix suggestions failed: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error testing fix suggestions: {e}")
        return None

def main():
    """Main test function"""
    print("🚀 Testing Schema Mapper & Data Quality Fixer Application")
    print("=" * 60)
    
    # Test API health
    if not test_api_health():
        print("\n❌ API is not running. Please start the backend first:")
        print("   python3 start_backend.py")
        return
    
    # Test canonical schema
    print("\n📋 Testing Canonical Schema...")
    schema_data = test_canonical_schema()
    if not schema_data:
        return
    
    # Test with sample files
    sample_files = [
        "Project6InputData1.csv",
        "Project6InputData2.csv", 
        "Project6InputData3.csv"
    ]
    
    for filename in sample_files:
        if not os.path.exists(filename):
            print(f"⚠️ Sample file {filename} not found, skipping...")
            continue
        
        print(f"\n📄 Testing {filename}...")
        
        # Test schema mapping
        mapping_result = test_schema_mapping(filename)
        if not mapping_result:
            continue
        
        # Test data cleaning
        cleaning_result = test_data_cleaning(filename, mapping_result)
        if not cleaning_result:
            continue
        
        # Test fix suggestions
        fix_result = test_fix_suggestions(cleaning_result)
    
    print("\n" + "=" * 60)
    print("🎉 Application testing completed!")
    print("\nTo start the frontend:")
    print("   python3 start_frontend.py")

if __name__ == "__main__":
    main()