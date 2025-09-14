"""
Simple test to verify the system structure
"""
import os
from pathlib import Path

def test_file_structure():
    """Test that all required files exist"""
    print("🔧 Schema Mapper & Data Quality Fixer - File Structure Test")
    print("=" * 60)
    
    required_files = [
        "requirements.txt",
        "README.md",
        "agents/__init__.py",
        "agents/schema_mapper_agent.py",
        "agents/data_cleaner_agent.py", 
        "agents/llm_fix_agent.py",
        "agents/data_processing_graph.py",
        "backend/main.py",
        "frontend/app.py",
        "utils/__init__.py",
        "utils/schema_loader.py",
        "utils/data_utils.py",
        "canonicalSchema/Project6StdFormat.csv",
        "sampleDataset/Project6InputData1.csv",
        "sampleDataset/Project6InputData2.csv",
        "sampleDataset/Project6InputData3.csv"
    ]
    
    missing_files = []
    existing_files = []
    
    for file_path in required_files:
        if Path(file_path).exists():
            existing_files.append(file_path)
            print(f"✅ {file_path}")
        else:
            missing_files.append(file_path)
            print(f"❌ {file_path}")
    
    print("\n" + "=" * 60)
    print(f"📊 Summary: {len(existing_files)}/{len(required_files)} files exist")
    
    if missing_files:
        print(f"❌ Missing files: {missing_files}")
        return False
    else:
        print("🎉 All required files are present!")
        return True

def test_canonical_schema():
    """Test canonical schema loading"""
    print("\n📋 Testing Canonical Schema")
    print("-" * 30)
    
    try:
        schema_path = Path("canonicalSchema/Project6StdFormat.csv")
        if schema_path.exists():
            with open(schema_path, 'r') as f:
                lines = f.readlines()
            
            print(f"✅ Schema file exists with {len(lines)} lines")
            
            # Check header
            if len(lines) > 0:
                header = lines[0].strip().split(',')
                expected_headers = ['canonical_name', 'description', 'example']
                
                if header == expected_headers:
                    print("✅ Schema headers are correct")
                else:
                    print(f"❌ Schema headers incorrect. Expected: {expected_headers}, Got: {header}")
                    return False
            
            # Check data rows
            if len(lines) > 1:
                data_rows = len(lines) - 1
                print(f"✅ Schema has {data_rows} data rows")
                
                # Show first few columns
                for i, line in enumerate(lines[1:4], 1):
                    parts = line.strip().split(',')
                    if len(parts) >= 3:
                        print(f"   {i}. {parts[0]} - {parts[1]}")
                
                return True
            else:
                print("❌ No data rows in schema")
                return False
        else:
            print("❌ Schema file not found")
            return False
    
    except Exception as e:
        print(f"❌ Error reading schema: {e}")
        return False

def test_sample_data():
    """Test sample data files"""
    print("\n📁 Testing Sample Data Files")
    print("-" * 30)
    
    sample_files = [
        "sampleDataset/Project6InputData1.csv",
        "sampleDataset/Project6InputData2.csv", 
        "sampleDataset/Project6InputData3.csv"
    ]
    
    for sample_file in sample_files:
        try:
            if Path(sample_file).exists():
                with open(sample_file, 'r') as f:
                    lines = f.readlines()
                
                if len(lines) > 0:
                    header = lines[0].strip().split(',')
                    data_rows = len(lines) - 1
                    print(f"✅ {sample_file}: {len(header)} columns, {data_rows} rows")
                else:
                    print(f"❌ {sample_file}: Empty file")
            else:
                print(f"❌ {sample_file}: File not found")
        except Exception as e:
            print(f"❌ {sample_file}: Error reading file - {e}")

def main():
    """Run all tests"""
    print("🧪 Schema Mapper & Data Quality Fixer - System Test")
    print("=" * 60)
    
    # Test file structure
    structure_ok = test_file_structure()
    
    # Test canonical schema
    schema_ok = test_canonical_schema()
    
    # Test sample data
    test_sample_data()
    
    print("\n" + "=" * 60)
    if structure_ok and schema_ok:
        print("🎉 System structure is correct!")
        print("\n📋 Next steps:")
        print("1. Install dependencies: pip install -r requirements.txt")
        print("2. Set up OpenAI API key in .env file")
        print("3. Start backend: python start_backend.py")
        print("4. Start frontend: python start_frontend.py")
        print("5. Access application at http://localhost:8501")
    else:
        print("❌ System structure has issues that need to be fixed")

if __name__ == "__main__":
    main()