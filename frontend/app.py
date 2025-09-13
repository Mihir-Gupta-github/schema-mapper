import streamlit as st
import pandas as pd
import requests
import json
import io
from typing import Dict, List, Any
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Configure page
st.set_page_config(
    page_title="Schema Mapper & Data Quality Fixer",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API Configuration
API_BASE_URL = "http://localhost:8000"

class DataQualityApp:
    def __init__(self):
        self.session_state = st.session_state
        self.initialize_session_state()
    
    def initialize_session_state(self):
        """Initialize session state variables"""
        if 'current_step' not in self.session_state:
            self.session_state.current_step = 'upload'
        if 'uploaded_file' not in self.session_state:
            self.session_state.uploaded_file = None
        if 'mapping_result' not in self.session_state:
            self.session_state.mapping_result = None
        if 'cleaning_result' not in self.session_state:
            self.session_state.cleaning_result = None
        if 'fix_suggestions' not in self.session_state:
            self.session_state.fix_suggestions = []
        if 'custom_mappings' not in self.session_state:
            self.session_state.custom_mappings = {}
    
    def run(self):
        """Main application runner"""
        st.title("🔧 Schema Mapper & Data Quality Fixer")
        st.markdown("Upload your CSV file and get intelligent schema mapping, data cleaning, and targeted fix suggestions.")
        
        # Sidebar navigation
        self.render_sidebar()
        
        # Main content area
        if self.session_state.current_step == 'upload':
            self.render_upload_step()
        elif self.session_state.current_step == 'mapping':
            self.render_mapping_step()
        elif self.session_state.current_step == 'cleaning':
            self.render_cleaning_step()
        elif self.session_state.current_step == 'fixes':
            self.render_fixes_step()
        elif self.session_state.current_step == 'results':
            self.render_results_step()
    
    def render_sidebar(self):
        """Render sidebar navigation"""
        st.sidebar.title("Navigation")
        
        steps = [
            ("📁 Upload", "upload"),
            ("🗺️ Schema Mapping", "mapping"),
            ("🧹 Data Cleaning", "cleaning"),
            ("🔧 Fix Suggestions", "fixes"),
            ("📊 Results", "results")
        ]
        
        for step_name, step_key in steps:
            if st.sidebar.button(step_name, key=f"nav_{step_key}"):
                self.session_state.current_step = step_key
        
        # Show current step
        current_step_names = {v: k for k, v in steps}
        st.sidebar.markdown(f"**Current Step:** {current_step_names[self.session_state.current_step]}")
        
        # Show file info if uploaded
        if self.session_state.uploaded_file:
            st.sidebar.markdown("---")
            st.sidebar.markdown("**Uploaded File:**")
            st.sidebar.markdown(f"- Name: {self.session_state.uploaded_file.name}")
            st.sidebar.markdown(f"- Size: {len(self.session_state.uploaded_file.getvalue())} bytes")
    
    def render_upload_step(self):
        """Render file upload step"""
        st.header("📁 Upload Your CSV File")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            uploaded_file = st.file_uploader(
                "Choose a CSV file",
                type=['csv'],
                help="Upload a CSV file to start the schema mapping and data cleaning process"
            )
            
            if uploaded_file:
                self.session_state.uploaded_file = uploaded_file
                
                # Show file preview
                try:
                    df = pd.read_csv(uploaded_file)
                    st.success(f"✅ File uploaded successfully! Found {len(df)} rows and {len(df.columns)} columns.")
                    
                    with st.expander("📋 File Preview", expanded=True):
                        st.dataframe(df.head(10))
                    
                    # Show column info
                    with st.expander("📊 Column Information"):
                        col_info = pd.DataFrame({
                            'Column': df.columns,
                            'Data Type': df.dtypes.astype(str),
                            'Non-Null Count': df.count(),
                            'Null Count': df.isnull().sum(),
                            'Unique Values': df.nunique()
                        })
                        st.dataframe(col_info)
                    
                    if st.button("🚀 Start Schema Mapping", type="primary"):
                        self.session_state.current_step = 'mapping'
                        st.rerun()
                        
                except Exception as e:
                    st.error(f"❌ Error reading file: {e}")
        
        with col2:
            st.markdown("### 📋 Sample Files")
            st.markdown("Try these sample files to test the application:")
            
            sample_files = [
                ("Project6InputData1.csv", "Clean, canonical-like headers"),
                ("Project6InputData2.csv", "Messy headers, mixed formats"),
                ("Project6InputData3.csv", "Different headers, missing columns")
            ]
            
            for filename, description in sample_files:
                if st.button(f"📄 {filename}", key=f"sample_{filename}"):
                    try:
                        with open(filename, 'rb') as f:
                            file_content = f.read()
                        self.session_state.uploaded_file = io.BytesIO(file_content)
                        self.session_state.uploaded_file.name = filename
                        st.success(f"✅ Loaded sample file: {filename}")
                        st.rerun()
                    except FileNotFoundError:
                        st.error(f"❌ Sample file not found: {filename}")
    
    def render_mapping_step(self):
        """Render schema mapping step"""
        st.header("🗺️ Schema Mapping")
        
        if not self.session_state.uploaded_file:
            st.warning("⚠️ Please upload a file first.")
            return
        
        # Get canonical schema
        try:
            response = requests.get(f"{API_BASE_URL}/canonical-schema")
            if response.status_code == 200:
                canonical_data = response.json()
                canonical_columns = canonical_data['columns']
            else:
                st.error("❌ Could not load canonical schema")
                return
        except Exception as e:
            st.error(f"❌ Error connecting to API: {e}")
            return
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("📋 Canonical Schema")
            canonical_df = pd.DataFrame(canonical_data['schema'])
            st.dataframe(canonical_df, use_container_width=True)
        
        with col2:
            st.subheader("📊 Your File Columns")
            if self.session_state.uploaded_file:
                self.session_state.uploaded_file.seek(0)
                df = pd.read_csv(self.session_state.uploaded_file)
                source_columns = df.columns.tolist()
                
                col_df = pd.DataFrame({
                    'Source Column': source_columns,
                    'Sample Values': [str(df[col].iloc[0]) if len(df) > 0 else '' for col in source_columns]
                })
                st.dataframe(col_df, use_container_width=True)
        
        # Perform mapping
        if st.button("🔍 Analyze Schema Mapping", type="primary"):
            with st.spinner("Analyzing schema mapping..."):
                try:
                    # Prepare file content
                    self.session_state.uploaded_file.seek(0)
                    file_content = self.session_state.uploaded_file.read()
                    
                    # Call mapping API
                    mapping_request = {
                        "file_content": file_content.hex(),
                        "custom_mappings": self.session_state.custom_mappings
                    }
                    
                    response = requests.post(
                        f"{API_BASE_URL}/map-schema",
                        json=mapping_request
                    )
                    
                    if response.status_code == 200:
                        self.session_state.mapping_result = response.json()
                        st.success("✅ Schema mapping completed!")
                    else:
                        st.error(f"❌ Mapping failed: {response.text}")
                        
                except Exception as e:
                    st.error(f"❌ Error during mapping: {e}")
        
        # Show mapping results
        if self.session_state.mapping_result:
            self.render_mapping_results()
    
    def render_mapping_results(self):
        """Render mapping results with confidence scores"""
        mapping_result = self.session_state.mapping_result
        
        st.subheader("🎯 Mapping Results")
        
        # Create mapping visualization
        mapping_data = []
        for source_col, canonical_col in mapping_result['suggested_mapping'].items():
            confidence = mapping_result['confidence_scores'].get(source_col, 0)
            mapping_data.append({
                'Source Column': source_col,
                'Canonical Column': canonical_col,
                'Confidence': confidence,
                'Status': '✅ High' if confidence >= 0.8 else '⚠️ Medium' if confidence >= 0.6 else '❌ Low'
            })
        
        mapping_df = pd.DataFrame(mapping_data)
        
        # Color code by confidence
        def color_confidence(val):
            if val >= 0.8:
                return 'background-color: #d4edda'
            elif val >= 0.6:
                return 'background-color: #fff3cd'
            else:
                return 'background-color: #f8d7da'
        
        styled_df = mapping_df.style.applymap(color_confidence, subset=['Confidence'])
        st.dataframe(styled_df, use_container_width=True)
        
        # Show unmapped columns
        if mapping_result['unmapped_columns']:
            st.warning(f"⚠️ Unmapped columns: {', '.join(mapping_result['unmapped_columns'])}")
        
        # Show extra columns
        if mapping_result['extra_columns']:
            st.info(f"ℹ️ Extra canonical columns not found: {', '.join(mapping_result['extra_columns'])}")
        
        # Custom mapping overrides
        st.subheader("🔧 Custom Mapping Overrides")
        st.markdown("Override any mapping suggestions below:")
        
        for source_col, canonical_col in mapping_result['suggested_mapping'].items():
            confidence = mapping_result['confidence_scores'].get(source_col, 0)
            
            col1, col2 = st.columns([2, 1])
            with col1:
                new_mapping = st.selectbox(
                    f"Map '{source_col}' to:",
                    options=[''] + mapping_result['suggested_mapping'].values(),
                    index=list(mapping_result['suggested_mapping'].values()).index(canonical_col) + 1 if canonical_col in mapping_result['suggested_mapping'].values() else 0,
                    key=f"override_{source_col}"
                )
            with col2:
                st.metric("Confidence", f"{confidence:.2f}")
            
            if new_mapping and new_mapping != canonical_col:
                self.session_state.custom_mappings[source_col] = new_mapping
        
        # Proceed to cleaning
        if st.button("🧹 Proceed to Data Cleaning", type="primary"):
            self.session_state.current_step = 'cleaning'
            st.rerun()
    
    def render_cleaning_step(self):
        """Render data cleaning step"""
        st.header("🧹 Data Cleaning & Validation")
        
        if not self.session_state.mapping_result:
            st.warning("⚠️ Please complete schema mapping first.")
            return
        
        st.subheader("📊 Cleaning Configuration")
        
        # Show what will be cleaned
        mapping_result = self.session_state.mapping_result
        st.markdown("**Columns to be cleaned:**")
        for source_col, canonical_col in mapping_result['suggested_mapping'].items():
            st.markdown(f"- `{source_col}` → `{canonical_col}`")
        
        # Cleaning options
        st.subheader("⚙️ Cleaning Options")
        
        col1, col2 = st.columns(2)
        
        with col1:
            trim_whitespace = st.checkbox("Trim whitespace", value=True)
            normalize_case = st.selectbox("Normalize case", ["None", "lower", "title", "upper"])
            remove_currency = st.checkbox("Remove currency symbols", value=True)
        
        with col2:
            normalize_dates = st.checkbox("Normalize date formats", value=True)
            validate_emails = st.checkbox("Validate email formats", value=True)
            clean_phone_numbers = st.checkbox("Clean phone numbers", value=True)
        
        # Start cleaning
        if st.button("🚀 Start Data Cleaning", type="primary"):
            with st.spinner("Cleaning and validating data..."):
                try:
                    # Prepare cleaning request
                    self.session_state.uploaded_file.seek(0)
                    file_content = self.session_state.uploaded_file.read()
                    
                    cleaning_rules = {
                        'trim_whitespace': trim_whitespace,
                        'normalize_case': normalize_case if normalize_case != "None" else None,
                        'remove_currency': remove_currency,
                        'normalize_dates': normalize_dates,
                        'validate_emails': validate_emails,
                        'clean_phone_numbers': clean_phone_numbers
                    }
                    
                    cleaning_request = {
                        "file_content": file_content.hex(),
                        "column_mapping": mapping_result['suggested_mapping'],
                        "cleaning_rules": cleaning_rules
                    }
                    
                    response = requests.post(
                        f"{API_BASE_URL}/clean-data",
                        json=cleaning_request
                    )
                    
                    if response.status_code == 200:
                        self.session_state.cleaning_result = response.json()
                        st.success("✅ Data cleaning completed!")
                    else:
                        st.error(f"❌ Cleaning failed: {response.text}")
                        
                except Exception as e:
                    st.error(f"❌ Error during cleaning: {e}")
        
        # Show cleaning results
        if self.session_state.cleaning_result:
            self.render_cleaning_results()
    
    def render_cleaning_results(self):
        """Render cleaning results with before/after metrics"""
        cleaning_result = self.session_state.cleaning_result
        
        st.subheader("📈 Cleaning Results")
        
        # Before/After metrics comparison
        before_metrics = cleaning_result['before_metrics']
        after_metrics = cleaning_result['after_metrics']
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Rows",
                after_metrics['total_rows'],
                delta=after_metrics['total_rows'] - before_metrics['total_rows']
            )
        
        with col2:
            st.metric(
                "Null Percentage",
                f"{after_metrics['null_percentage']:.1f}%",
                delta=f"{after_metrics['null_percentage'] - before_metrics['null_percentage']:.1f}%"
            )
        
        with col3:
            st.metric(
                "Duplicate Rows",
                after_metrics['duplicate_rows'],
                delta=after_metrics['duplicate_rows'] - before_metrics['duplicate_rows']
            )
        
        with col4:
            st.metric(
                "Total Columns",
                after_metrics['total_columns'],
                delta=after_metrics['total_columns'] - before_metrics['total_columns']
            )
        
        # Issues found
        if cleaning_result['issues_found']:
            st.subheader("⚠️ Issues Found")
            for issue in cleaning_result['issues_found']:
                with st.expander(f"❌ {issue['column']}: {issue['issue_type']} ({issue['count']} issues)"):
                    st.write(f"**Description:** {issue['description']}")
                    if 'invalid_rows' in issue:
                        st.write(f"**Affected rows:** {len(issue['invalid_rows'])}")
        
        # Cleaning applied
        if cleaning_result['cleaning_applied']:
            st.subheader("✅ Cleaning Applied")
            for cleaning in cleaning_result['cleaning_applied']:
                st.success(f"**{cleaning['column']}:** {cleaning['description']}")
        
        # Proceed to fixes
        if cleaning_result['issues_found']:
            if st.button("🔧 Get Fix Suggestions", type="primary"):
                self.session_state.current_step = 'fixes'
                st.rerun()
        else:
            st.success("🎉 No issues found! Data is clean.")
            if st.button("📊 View Results", type="primary"):
                self.session_state.current_step = 'results'
                st.rerun()
    
    def render_fixes_step(self):
        """Render fix suggestions step"""
        st.header("🔧 Targeted Fix Suggestions")
        
        if not self.session_state.cleaning_result:
            st.warning("⚠️ Please complete data cleaning first.")
            return
        
        # Get fix suggestions
        if not self.session_state.fix_suggestions:
            with st.spinner("Generating fix suggestions..."):
                try:
                    fix_request = {
                        "cleaned_file_path": self.session_state.cleaning_result['cleaned_file_path'],
                        "issues": self.session_state.cleaning_result['issues_found'],
                        "column_mapping": self.session_state.mapping_result['suggested_mapping']
                    }
                    
                    response = requests.post(
                        f"{API_BASE_URL}/suggest-fixes",
                        json=fix_request
                    )
                    
                    if response.status_code == 200:
                        self.session_state.fix_suggestions = response.json()['fix_suggestions']
                        st.success("✅ Fix suggestions generated!")
                    else:
                        st.error(f"❌ Failed to generate suggestions: {response.text}")
                        
                except Exception as e:
                    st.error(f"❌ Error generating suggestions: {e}")
        
        # Display fix suggestions
        if self.session_state.fix_suggestions:
            st.subheader("🎯 Suggested Fixes")
            
            for i, fix in enumerate(self.session_state.fix_suggestions):
                with st.expander(f"Fix {i+1}: {fix['column']} (Row {fix['row_index']}) - Confidence: {fix['confidence']:.2f}"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**Current Value:**")
                        st.code(fix['current_value'])
                    
                    with col2:
                        st.markdown("**Suggested Value:**")
                        st.code(fix['suggested_value'])
                    
                    st.markdown(f"**Reason:** {fix['reason']}")
                    st.markdown(f"**Fix Type:** {fix['fix_type']}")
                    
                    # Action buttons
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if st.button(f"✅ Apply Fix {i+1}", key=f"apply_{i}"):
                            # Apply the fix (implement this)
                            st.success("Fix applied!")
                    
                    with col2:
                        if st.button(f"❌ Reject Fix {i+1}", key=f"reject_{i}"):
                            # Remove the fix from suggestions
                            self.session_state.fix_suggestions.pop(i)
                            st.rerun()
                    
                    with col3:
                        if st.button(f"📚 Promote Fix {i+1}", key=f"promote_{i}"):
                            # Promote the fix to learning system
                            try:
                                promote_request = {
                                    "fix_type": fix['fix_type'],
                                    "pattern": fix['current_value'],
                                    "solution": fix['suggested_value'],
                                    "confidence": fix['confidence']
                                }
                                
                                response = requests.post(
                                    f"{API_BASE_URL}/promote-fix",
                                    json=promote_request
                                )
                                
                                if response.status_code == 200:
                                    st.success("✅ Fix promoted to learning system!")
                                else:
                                    st.error("❌ Failed to promote fix")
                                    
                            except Exception as e:
                                st.error(f"❌ Error promoting fix: {e}")
        else:
            st.info("ℹ️ No fix suggestions available.")
        
        # Proceed to results
        if st.button("📊 View Final Results", type="primary"):
            self.session_state.current_step = 'results'
            st.rerun()
    
    def render_results_step(self):
        """Render final results step"""
        st.header("📊 Final Results")
        
        if not self.session_state.cleaning_result:
            st.warning("⚠️ Please complete the cleaning process first.")
            return
        
        # Download cleaned data
        st.subheader("📥 Download Cleaned Data")
        
        try:
            cleaned_df = pd.read_csv(self.session_state.cleaning_result['cleaned_file_path'])
            
            csv = cleaned_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Cleaned CSV",
                data=csv,
                file_name=f"cleaned_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
            
            # Show cleaned data preview
            st.subheader("📋 Cleaned Data Preview")
            st.dataframe(cleaned_df.head(20), use_container_width=True)
            
            # Summary statistics
            st.subheader("📈 Data Quality Summary")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Rows", len(cleaned_df))
                st.metric("Total Columns", len(cleaned_df.columns))
                st.metric("Null Values", cleaned_df.isnull().sum().sum())
            
            with col2:
                st.metric("Duplicate Rows", cleaned_df.duplicated().sum())
                st.metric("Data Completeness", f"{((cleaned_df.size - cleaned_df.isnull().sum().sum()) / cleaned_df.size * 100):.1f}%")
                st.metric("Unique Rows", len(cleaned_df.drop_duplicates()))
            
        except Exception as e:
            st.error(f"❌ Error loading cleaned data: {e}")
        
        # Start new process
        st.subheader("🔄 Start New Process")
        if st.button("📁 Upload New File", type="primary"):
            # Reset session state
            for key in ['uploaded_file', 'mapping_result', 'cleaning_result', 'fix_suggestions', 'custom_mappings']:
                if key in self.session_state:
                    del self.session_state[key]
            self.session_state.current_step = 'upload'
            st.rerun()

def main():
    """Main function"""
    app = DataQualityApp()
    app.run()

if __name__ == "__main__":
    main()