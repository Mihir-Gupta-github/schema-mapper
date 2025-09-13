#!/usr/bin/env python3
"""
Test script for the Schema Mapper & Data Quality Fixer application
"""
import sys
import os
import pandas as pd
import requests
import json
import time

# Add backend to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

def test_backend_services():
    """Test the backend services directly"""
    print("🧪 Testing Backend Services...")
    
    try:
        from backend.services.schema_mapper import SchemaMapper
        from backend.services.data_cleaner import DataCleaner
        from backend.services.fix_suggester import FixSuggester
        
        # Test schema mapper
        print("  📋 Testing Schema Mapper...")
        mapper = SchemaMapper()
        source_cols = ['CUST_ID', 'CUSTOMER NAME', 'EMAIL ADDRESS', 'PHONE NUMBER']
        canonical_cols = ['customer_id', 'customer_name', 'email', 'phone']
        
        result = mapper.map_columns(source_cols, canonical_cols)
        print(f"    ✅ Mapping result: {result['mapping']}")
        print(f"    ✅ Confidence scores: {result['confidence_scores']}")
        
        # Test data cleaner
        print("  🧹 Testing Data Cleaner...")
        cleaner = DataCleaner()
        
        # Create test data
        test_data = pd.DataFrame({
            'customer_id': ['CUST_001', 'CUST_002', ''],
            'customer_name': ['John Smith', '  Jane Doe  ', 'Bob Johnson'],
            'email': ['john@email.com', 'invalid-email', 'jane@email.com'],
            'phone': ['+1-555-123-4567', '555.987.6543', '555-456-7890'],
            'annual_revenue': ['$150,000', '200000', '$75,000.00']
        })
        
        column_mapping = {
            'customer_id': 'customer_id',
            'customer_name': 'customer_name', 
            'email': 'email',
            'phone': 'phone',
            'annual_revenue': 'annual_revenue'
        }
        
        cleaning_result = cleaner.clean_dataframe(test_data, column_mapping)
        print(f"    ✅ Before metrics: {cleaning_result['before_metrics']['total_rows']} rows")
        print(f"    ✅ After metrics: {cleaning_result['after_metrics']['total_rows']} rows")
        print(f"    ✅ Issues found: {len(cleaning_result['issues_found'])}")
        print(f"    ✅ Cleaning applied: {len(cleaning_result['cleaning_applied'])}")
        
        # Test fix suggester
        print("  🔧 Testing Fix Suggester...")
        suggester = FixSuggester()
        
        if cleaning_result['issues_found']:
            suggestions = suggester.suggest_fixes(
                cleaning_result['cleaned_df'],
                cleaning_result['issues_found'],
                column_mapping
            )
            print(f"    ✅ Fix suggestions generated: {len(suggestions)}")
        
        print("✅ Backend services test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Backend services test failed: {e}")
        return False

def test_api_endpoints():
    """Test the API endpoints"""
    print("🌐 Testing API Endpoints...")
    
    base_url = "http://localhost:8000"
    
    try:
        # Test health check
        print("  🔍 Testing health check...")
        response = requests.get(f"{base_url}/", timeout=5)
        if response.status_code == 200:
            print("    ✅ Health check passed")
        else:
            print(f"    ❌ Health check failed: {response.status_code}")
            return False
        
        # Test canonical schema
        print("  📋 Testing canonical schema endpoint...")
        response = requests.get(f"{base_url}/canonical-schema", timeout=5)
        if response.status_code == 200:
            schema_data = response.json()
            print(f"    ✅ Canonical schema loaded: {len(schema_data['columns'])} columns")
        else:
            print(f"    ❌ Canonical schema failed: {response.status_code}")
            return False
        
        # Test schema mapping with sample data
        print("  🗺️ Testing schema mapping endpoint...")
        
        # Read sample file
        sample_file = "Project6InputData2.csv"
        if os.path.exists(sample_file):
            with open(sample_file, 'rb') as f:
                file_content = f.read()
            
            mapping_request = {
                "file_content": file_content.hex(),
                "custom_mappings": {}
            }
            
            response = requests.post(
                f"{base_url}/map-schema",
                json=mapping_request,
                timeout=10
            )
            
            if response.status_code == 200:
                mapping_result = response.json()
                print(f"    ✅ Schema mapping successful: {len(mapping_result['suggested_mapping'])} mappings")
                print(f"    ✅ Confidence scores: {list(mapping_result['confidence_scores'].values())}")
            else:
                print(f"    ❌ Schema mapping failed: {response.status_code} - {response.text}")
                return False
        else:
            print(f"    ⚠️ Sample file {sample_file} not found, skipping mapping test")
        
        print("✅ API endpoints test completed successfully!")
        return True
        
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to API. Is the backend running?")
        return False
    except Exception as e:
        print(f"❌ API endpoints test failed: {e}")
        return False

def test_sample_data():
    """Test with sample data files"""
    print("📊 Testing Sample Data...")
    
    sample_files = [
        "Project6InputData1.csv",
        "Project6InputData2.csv", 
        "Project6InputData3.csv"
    ]
    
    for sample_file in sample_files:
        if os.path.exists(sample_file):
            print(f"  📄 Testing {sample_file}...")
            try:
                df = pd.read_csv(sample_file)
                print(f"    ✅ Loaded: {len(df)} rows, {len(df.columns)} columns")
                print(f"    ✅ Columns: {list(df.columns)}")
                
                # Check for common data quality issues
                null_count = df.isnull().sum().sum()
                duplicate_count = df.duplicated().sum()
                print(f"    ✅ Null values: {null_count}")
                print(f"    ✅ Duplicate rows: {duplicate_count}")
                
            except Exception as e:
                print(f"    ❌ Error reading {sample_file}: {e}")
        else:
            print(f"  ⚠️ {sample_file} not found")
    
    print("✅ Sample data test completed!")

def main():
    """Main test function"""
    print("🚀 Starting Schema Mapper & Data Quality Fixer Tests")
    print("=" * 60)
    
    # Test sample data
    test_sample_data()
    print()
    
    # Test backend services
    backend_ok = test_backend_services()
    print()
    
    # Test API endpoints (only if backend is running)
    api_ok = test_api_endpoints()
    print()
    
    # Summary
    print("=" * 60)
    print("📊 Test Summary:")
    print(f"  Backend Services: {'✅ PASS' if backend_ok else '❌ FAIL'}")
    print(f"  API Endpoints: {'✅ PASS' if api_ok else '❌ FAIL'}")
    
    if backend_ok and api_ok:
        print("\n🎉 All tests passed! The application is ready to use.")
        print("\nTo start the application:")
        print("  1. Start backend: python start_backend.py")
        print("  2. Start frontend: python start_frontend.py")
    else:
        print("\n⚠️ Some tests failed. Please check the errors above.")
        
        if not api_ok:
            print("\nTo start the backend:")
            print("  python start_backend.py")

if __name__ == "__main__":
    main()