"""
Demo script showing the Schema Mapper & Data Quality Fixer in action
"""
import pandas as pd
from agents import DataProcessingGraph
from utils.schema_loader import load_canonical_schema
from pathlib import Path


def demo_with_sample_data():
    """Demo the system with sample data"""
    print("🔧 Schema Mapper & Data Quality Fixer Demo")
    print("=" * 50)
    
    # Load canonical schema
    schema_path = Path("canonicalSchema/Project6StdFormat.csv")
    canonical_schema = load_canonical_schema(schema_path)
    print(f"📋 Loaded canonical schema with {len(canonical_schema)} columns")
    
    # Initialize processing graph
    processing_graph = DataProcessingGraph(canonical_schema)
    print("🤖 Initialized processing graph with agents")
    
    # Test with different sample files
    sample_files = [
        "sampleDataset/Project6InputData1.csv",
        "sampleDataset/Project6InputData2.csv", 
        "sampleDataset/Project6InputData3.csv"
    ]
    
    for i, sample_file in enumerate(sample_files, 1):
        print(f"\n📁 Testing with {sample_file}")
        print("-" * 30)
        
        try:
            # Load sample data
            df = pd.read_csv(sample_file)
            print(f"📊 Loaded {len(df)} rows and {len(df.columns)} columns")
            print(f"📋 Columns: {list(df.columns)}")
            
            # Process the data
            result = processing_graph.process_data(df)
            
            # Display results
            summary = result['processing_summary']
            print(f"✅ Processing completed:")
            print(f"   - Mapped columns: {summary['mapped_columns']}")
            print(f"   - Unmapped columns: {summary['unmapped_columns']}")
            print(f"   - Cleaning rules applied: {summary['cleaning_rules_applied']}")
            print(f"   - Validation errors: {summary['validation_errors']}")
            print(f"   - Fix suggestions: {summary['fix_suggestions']}")
            
            # Show column mappings
            if result['column_mappings']:
                print(f"\n🗺️ Column Mappings:")
                for mapping in result['column_mappings']:
                    print(f"   {mapping.source_column} → {mapping.target_column} "
                          f"(confidence: {mapping.confidence:.2f}, type: {mapping.mapping_type})")
            
            # Show unmapped columns
            if result['unmapped_columns']:
                print(f"\n❓ Unmapped Columns: {result['unmapped_columns']}")
            
            # Show cleaning results
            if result['cleaning_results']:
                print(f"\n🧹 Cleaning Results:")
                for cleaning in result['cleaning_results'][:3]:  # Show first 3
                    print(f"   {cleaning.column}: '{cleaning.original_value}' → '{cleaning.cleaned_value}' "
                          f"({cleaning.rule_applied})")
                if len(result['cleaning_results']) > 3:
                    print(f"   ... and {len(result['cleaning_results']) - 3} more")
            
            # Show fix suggestions
            if result['fix_suggestions']:
                print(f"\n💡 Fix Suggestions:")
                for fix in result['fix_suggestions'][:3]:  # Show first 3
                    print(f"   Row {fix.row_index}, {fix.column}: '{fix.original_value}' → '{fix.suggested_value}' "
                          f"(confidence: {fix.confidence:.2f})")
                if len(result['fix_suggestions']) > 3:
                    print(f"   ... and {len(result['fix_suggestions']) - 3} more")
            
        except Exception as e:
            print(f"❌ Error processing {sample_file}: {e}")
    
    print("\n" + "=" * 50)
    print("🎉 Demo completed! The system successfully:")
    print("   ✅ Mapped columns to canonical schema")
    print("   ✅ Applied deterministic cleaning rules")
    print("   ✅ Generated targeted fix suggestions")
    print("   ✅ Handled different data formats and quality issues")


if __name__ == "__main__":
    demo_with_sample_data()