"""
Streamlit Frontend for Schema Mapper & Data Quality Fixer
"""
import streamlit as st
import pandas as pd
import requests
import json
from typing import Dict, List, Any
import plotly.express as px
import plotly.graph_objects as go
from streamlit_aggrid import AgGrid, GridOptionsBuilder
import io


# Configuration
st.set_page_config(
    page_title="Schema Mapper & Data Quality Fixer",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API Configuration
API_BASE_URL = "http://localhost:8000"


def main():
    """Main application"""
    st.title("🔧 Schema Mapper & Data Quality Fixer")
    st.markdown("Upload your CSV file and get clean, canonical data with intelligent mapping and quality fixes.")
    
    # Sidebar
    with st.sidebar:
        st.header("📋 Canonical Schema")
        display_canonical_schema()
    
    # Main content
    tab1, tab2, tab3, tab4 = st.tabs(["📤 Upload", "🗺️ Mapping", "🔍 Quality Check", "📥 Download"])
    
    with tab1:
        upload_tab()
    
    with tab2:
        mapping_tab()
    
    with tab3:
        quality_check_tab()
    
    with tab4:
        download_tab()


def display_canonical_schema():
    """Display canonical schema in sidebar"""
    try:
        response = requests.get(f"{API_BASE_URL}/schema")
        if response.status_code == 200:
            schema = response.json()["schema"]
            
            for col_name, description in schema.items():
                with st.expander(f"**{col_name}**"):
                    st.write(description)
        else:
            st.error("Failed to load canonical schema")
    except Exception as e:
        st.error(f"Error loading schema: {e}")


def upload_tab():
    """Upload and initial processing tab"""
    st.header("📤 Upload Your CSV File")
    
    # File upload
    uploaded_file = st.file_uploader(
        "Choose a CSV file",
        type="csv",
        help="Upload a CSV file to start the mapping and cleaning process"
    )
    
    if uploaded_file is not None:
        # Display file info
        st.success(f"✅ File uploaded: {uploaded_file.name}")
        st.info(f"📊 File size: {uploaded_file.size:,} bytes")
        
        # Preview original data
        try:
            df_original = pd.read_csv(uploaded_file)
            st.subheader("📋 Original Data Preview")
            st.dataframe(df_original.head(10), use_container_width=True)
            
            # Upload and process button
            if st.button("🚀 Start Uploading and Cleaning", type="primary"):
                with st.spinner("Processing your file..."):
                    result = process_file(uploaded_file)
                    
                if result:
                    st.session_state['processing_result'] = result
                    st.session_state['session_id'] = result['session_id']
                    st.success("✅ File processed successfully!")
                    
                    # Display summary
                    display_processing_summary(result['summary'])
                    
                    # Show automatic mappings
                    if result['column_mappings']:
                        st.subheader("🎯 Automatic Column Mappings")
                        display_column_mappings(result['column_mappings'])
                    
                    # Show unmapped columns
                    if result['unmapped_columns']:
                        st.subheader("❓ Unmapped Columns")
                        st.warning(f"These columns need manual mapping: {', '.join(result['unmapped_columns'])}")
                        st.info("Go to the 'Mapping' tab to manually map these columns.")
        
        except Exception as e:
            st.error(f"Error reading file: {e}")


def process_file(uploaded_file) -> Dict[str, Any]:
    """Process uploaded file via API"""
    try:
        files = {"file": (uploaded_file.name, uploaded_file.getvalue(), "text/csv")}
        response = requests.post(f"{API_BASE_URL}/upload", files=files)
        
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"API Error: {response.text}")
            return None
    
    except Exception as e:
        st.error(f"Error processing file: {e}")
        return None


def display_processing_summary(summary: Dict[str, Any]):
    """Display processing summary"""
    st.subheader("📊 Processing Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Rows", summary['total_rows'])
    
    with col2:
        st.metric("Total Columns", summary['total_columns'])
    
    with col3:
        st.metric("Mapped Columns", summary['mapped_columns'])
    
    with col4:
        st.metric("Cleaning Rules Applied", summary['cleaning_rules_applied'])


def display_column_mappings(mappings: List[Dict[str, Any]]):
    """Display column mappings"""
    df_mappings = pd.DataFrame(mappings)
    
    # Color code by confidence
    def color_confidence(val):
        if val >= 0.9:
            return 'background-color: #d4edda'  # Green
        elif val >= 0.7:
            return 'background-color: #fff3cd'  # Yellow
        else:
            return 'background-color: #f8d7da'  # Red
    
    styled_df = df_mappings.style.applymap(color_confidence, subset=['confidence'])
    st.dataframe(styled_df, use_container_width=True)


def mapping_tab():
    """Manual mapping tab"""
    st.header("🗺️ Manual Column Mapping")
    
    if 'session_id' not in st.session_state:
        st.warning("Please upload a file first in the 'Upload' tab.")
        return
    
    session_id = st.session_state['session_id']
    
    # Get current result
    try:
        response = requests.get(f"{API_BASE_URL}/result/{session_id}")
        if response.status_code != 200:
            st.error("Failed to load processing result")
            return
        
        result = response.json()
        
        # Show unmapped columns
        unmapped_columns = result.get('unmapped_columns', [])
        
        if not unmapped_columns:
            st.success("✅ All columns have been mapped!")
            return
        
        st.subheader("📝 Map Unmapped Columns")
        
        # Manual mapping interface
        manual_mappings = {}
        
        for col in unmapped_columns:
            st.write(f"**{col}**")
            
            # Get canonical schema for dropdown
            schema_response = requests.get(f"{API_BASE_URL}/schema")
            if schema_response.status_code == 200:
                schema = schema_response.json()["schema"]
                canonical_columns = list(schema.keys())
                
                selected_target = st.selectbox(
                    f"Map '{col}' to:",
                    ["Select target column..."] + canonical_columns,
                    key=f"mapping_{col}"
                )
                
                if selected_target != "Select target column...":
                    manual_mappings[col] = selected_target
        
        # Apply mappings button
        if manual_mappings and st.button("✅ Apply Manual Mappings", type="primary"):
            with st.spinner("Applying manual mappings..."):
                response = requests.post(
                    f"{API_BASE_URL}/manual-mapping/{session_id}",
                    json=manual_mappings
                )
                
                if response.status_code == 200:
                    st.success("✅ Manual mappings applied successfully!")
                    st.rerun()
                else:
                    st.error(f"Error applying mappings: {response.text}")
    
    except Exception as e:
        st.error(f"Error in mapping tab: {e}")


def quality_check_tab():
    """Quality check and fix suggestions tab"""
    st.header("🔍 Data Quality Check")
    
    if 'session_id' not in st.session_state:
        st.warning("Please upload a file first in the 'Upload' tab.")
        return
    
    session_id = st.session_state['session_id']
    
    try:
        response = requests.get(f"{API_BASE_URL}/result/{session_id}")
        if response.status_code != 200:
            st.error("Failed to load processing result")
            return
        
        result = response.json()
        
        # Display cleaning results
        if result.get('cleaning_results'):
            st.subheader("🧹 Automatic Cleaning Results")
            display_cleaning_results(result['cleaning_results'])
        
        # Display validation errors
        validation_errors = result.get('validation_errors', [])
        if validation_errors:
            st.subheader("⚠️ Validation Errors")
            display_validation_errors(validation_errors)
        
        # Display fix suggestions
        fix_suggestions = result.get('fix_suggestions', [])
        if fix_suggestions:
            st.subheader("💡 Fix Suggestions")
            display_fix_suggestions(fix_suggestions, session_id)
        else:
            st.success("✅ No fix suggestions needed - data quality looks good!")
    
    except Exception as e:
        st.error(f"Error in quality check tab: {e}")


def display_cleaning_results(cleaning_results: List[Dict[str, Any]]):
    """Display cleaning results"""
    if not cleaning_results:
        st.info("No automatic cleaning was applied.")
        return
    
    df_cleaning = pd.DataFrame(cleaning_results)
    
    # Group by column
    for column in df_cleaning['column'].unique():
        st.write(f"**{column}**")
        column_results = df_cleaning[df_cleaning['column'] == column]
        
        # Show before/after comparison
        for _, row in column_results.iterrows():
            col1, col2, col3 = st.columns([1, 1, 2])
            
            with col1:
                st.write("**Before:**")
                st.code(str(row['original_value']))
            
            with col2:
                st.write("**After:**")
                st.code(str(row['cleaned_value']))
            
            with col3:
                st.write("**Rule Applied:**")
                st.info(row['rule_applied'])


def display_validation_errors(validation_errors: List[Dict[str, Any]]):
    """Display validation errors"""
    if not validation_errors:
        st.success("✅ No validation errors found!")
        return
    
    df_errors = pd.DataFrame(validation_errors)
    
    # Summary by column
    error_summary = df_errors.groupby('column').size().reset_index(name='error_count')
    
    st.write("**Error Summary by Column:**")
    fig = px.bar(error_summary, x='column', y='error_count', 
                 title="Validation Errors by Column")
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed errors
    st.write("**Detailed Errors:**")
    st.dataframe(df_errors, use_container_width=True)


def display_fix_suggestions(fix_suggestions: List[Dict[str, Any]], session_id: str):
    """Display fix suggestions with apply functionality"""
    if not fix_suggestions:
        st.info("No fix suggestions available.")
        return
    
    st.write("Select the fixes you want to apply:")
    
    # Create a dataframe for the suggestions
    df_suggestions = pd.DataFrame(fix_suggestions)
    
    # Add selection column
    df_suggestions['apply'] = False
    
    # Display suggestions with checkboxes
    selected_fixes = []
    
    for idx, suggestion in enumerate(fix_suggestions):
        with st.expander(f"Row {suggestion['row_index']} - {suggestion['column']}"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Original Value:**")
                st.code(str(suggestion['original_value']))
            
            with col2:
                st.write("**Suggested Value:**")
                st.code(str(suggestion['suggested_value']))
            
            st.write("**Reason:**")
            st.info(suggestion['reason'])
            
            st.write(f"**Confidence:** {suggestion['confidence']:.2f}")
            
            apply_fix = st.checkbox(f"Apply this fix", key=f"fix_{idx}")
            
            if apply_fix:
                selected_fixes.append({
                    "column": suggestion['column'],
                    "row_index": suggestion['row_index'],
                    "original_value": suggestion['original_value'],
                    "suggested_value": suggestion['suggested_value'],
                    "reason": suggestion['reason'],
                    "confidence": suggestion['confidence']
                })
    
    # Apply selected fixes
    if selected_fixes and st.button("🔧 Apply Selected Fixes", type="primary"):
        with st.spinner("Applying fixes..."):
            response = requests.post(
                f"{API_BASE_URL}/apply-fixes/{session_id}",
                json=selected_fixes
            )
            
            if response.status_code == 200:
                st.success(f"✅ Applied {len(selected_fixes)} fixes successfully!")
                st.rerun()
            else:
                st.error(f"Error applying fixes: {response.text}")


def download_tab():
    """Download cleaned data tab"""
    st.header("📥 Download Cleaned Data")
    
    if 'session_id' not in st.session_state:
        st.warning("Please upload a file first in the 'Upload' tab.")
        return
    
    session_id = st.session_state['session_id']
    
    try:
        # Get final result
        response = requests.get(f"{API_BASE_URL}/result/{session_id}")
        if response.status_code != 200:
            st.error("Failed to load processing result")
            return
        
        result = response.json()
        
        # Display final data preview
        st.subheader("📋 Final Cleaned Data Preview")
        df_final = pd.DataFrame(result['final_dataframe'])
        st.dataframe(df_final.head(20), use_container_width=True)
        
        # Download button
        csv_data = df_final.to_csv(index=False)
        
        st.download_button(
            label="📥 Download Cleaned CSV",
            data=csv_data,
            file_name=f"cleaned_data_{session_id}.csv",
            mime="text/csv",
            type="primary"
        )
        
        # Final summary
        st.subheader("📊 Final Processing Summary")
        summary = result['summary']
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Rows", summary['total_rows'])
        
        with col2:
            st.metric("Total Columns", summary['total_columns'])
        
        with col3:
            st.metric("Applied Fixes", summary['applied_fixes'])
        
        with col4:
            st.metric("Validation Errors", summary['validation_errors'])
    
    except Exception as e:
        st.error(f"Error in download tab: {e}")


if __name__ == "__main__":
    main()