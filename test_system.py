"""
Test script for the Schema Mapper & Data Quality Fixer system
"""
import pandas as pd
import requests
import json
import time
from pathlib import Path


def test_backend_health():
    """Test backend health endpoint"""
    try:
        response = requests.get("http://localhost:8000/health")
        if response.status_code == 200:
            print("✅ Backend health check passed")
            return True
        else:
            print(f"❌ Backend health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Backend health check failed: {e}")
        return False


def test_schema_endpoint():
    """Test schema endpoint"""
    try:
        response = requests.get("http://localhost:8000/schema")
        if response.status_code == 200:
            schema = response.json()["schema"]
            print(f"✅ Schema endpoint working - {len(schema)} columns loaded")
            return True
        else:
            print(f"❌ Schema endpoint failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Schema endpoint failed: {e}")
        return False


def test_file_upload():
    """Test file upload with sample data"""
    try:
        # Test with Project6InputData1.csv
        sample_file = Path("sampleDataset/Project6InputData1.csv")
        if not sample_file.exists():
            print("❌ Sample file not found")
            return False
        
        with open(sample_file, 'rb') as f:
            files = {"file": (sample_file.name, f.read(), "text/csv")}
            response = requests.post("http://localhost:8000/upload", files=files)
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ File upload successful - Session ID: {result['session_id']}")
            return result['session_id']
        else:
            print(f"❌ File upload failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"❌ File upload failed: {e}")
        return None


def test_processing_result(session_id):
    """Test processing result retrieval"""
    try:
        response = requests.get(f"http://localhost:8000/result/{session_id}")
        if response.status_code == 200:
            result = response.json()
            summary = result['summary']
            print(f"✅ Processing result retrieved:")
            print(f"   - Total rows: {summary['total_rows']}")
            print(f"   - Total columns: {summary['total_columns']}")
            print(f"   - Mapped columns: {summary['mapped_columns']}")
            print(f"   - Cleaning rules applied: {summary['cleaning_rules_applied']}")
            return True
        else:
            print(f"❌ Processing result retrieval failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Processing result retrieval failed: {e}")
        return False


def test_manual_mapping(session_id):
    """Test manual mapping functionality"""
    try:
        # Test manual mapping (if there are unmapped columns)
        response = requests.get(f"http://localhost:8000/result/{session_id}")
        if response.status_code == 200:
            result = response.json()
            unmapped_columns = result.get('unmapped_columns', [])
            
            if unmapped_columns:
                # Test with a dummy mapping
                test_mapping = {unmapped_columns[0]: "order_id"}
                response = requests.post(
                    f"http://localhost:8000/manual-mapping/{session_id}",
                    json=test_mapping
                )
                
                if response.status_code == 200:
                    print("✅ Manual mapping test passed")
                    return True
                else:
                    print(f"❌ Manual mapping test failed: {response.status_code}")
                    return False
            else:
                print("ℹ️ No unmapped columns to test manual mapping")
                return True
        else:
            print("❌ Could not retrieve result for manual mapping test")
            return False
    except Exception as e:
        print(f"❌ Manual mapping test failed: {e}")
        return False


def test_download(session_id):
    """Test download functionality"""
    try:
        response = requests.get(f"http://localhost:8000/download/{session_id}")
        if response.status_code == 200:
            result = response.json()
            csv_content = result['csv_content']
            print(f"✅ Download test passed - CSV content length: {len(csv_content)}")
            return True
        else:
            print(f"❌ Download test failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Download test failed: {e}")
        return False


def run_all_tests():
    """Run all tests"""
    print("🧪 Starting Schema Mapper & Data Quality Fixer Tests")
    print("=" * 60)
    
    # Test backend health
    if not test_backend_health():
        print("❌ Backend is not running. Please start it first.")
        return False
    
    # Test schema endpoint
    if not test_schema_endpoint():
        return False
    
    # Test file upload
    session_id = test_file_upload()
    if not session_id:
        return False
    
    # Wait a moment for processing
    time.sleep(2)
    
    # Test processing result
    if not test_processing_result(session_id):
        return False
    
    # Test manual mapping
    if not test_manual_mapping(session_id):
        return False
    
    # Test download
    if not test_download(session_id):
        return False
    
    print("=" * 60)
    print("🎉 All tests passed! The system is working correctly.")
    return True


if __name__ == "__main__":
    run_all_tests()