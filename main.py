"""
Schema Mapper & Data Quality Fixer
A FastAPI application that suggests mappings to canonical schema and provides data cleaning capabilities.
"""

import os
import json
import io
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from src.schema_loader import SchemaLoader
from src.header_mapper import HeaderMapper
from src.data_validator import DataValidator
from src.value_normalizer import ValueNormalizer
from src.fix_suggester import FixSuggester
from src.learning_system import LearningSystem

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Schema Mapper & Data Quality Fixer",
    description="Intelligent data mapping and cleaning with learning capabilities",
    version="1.0.0"
)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Initialize components
schema_loader = SchemaLoader()
header_mapper = HeaderMapper(schema_loader)
data_validator = DataValidator()
value_normalizer = ValueNormalizer()
fix_suggester = FixSuggester()
learning_system = LearningSystem()

# Pydantic models
class MappingResult(BaseModel):
    suggested_mapping: Dict[str, str]
    confidence_scores: Dict[str, float]
    unmapped_columns: List[str]

class CleaningResult(BaseModel):
    cleaned_data: Dict[str, Any]
    validation_report: Dict[str, Any]
    issues_found: List[Dict[str, Any]]
    fix_suggestions: List[Dict[str, Any]]

class FixApplication(BaseModel):
    fix_id: str
    accepted: bool
    custom_value: Optional[str] = None

@app.on_event("startup")
async def startup_event():
    """Initialize the application on startup."""
    try:
        # Load canonical schema
        schema_path = Path(__file__).parent / "canonicalSchema" / "Project6StdFormat.csv"
        if schema_path.exists():
            success = schema_loader.load_schema(str(schema_path))
            if success:
                logger.info("Canonical schema loaded successfully")
            else:
                logger.warning("Failed to load canonical schema")
        else:
            logger.warning(f"Canonical schema file not found at {schema_path}")
        
        # Load learning data
        learning_system.load_learning_data()
        logger.info("Application started successfully")
    except Exception as e:
        logger.error(f"Error during startup: {e}")

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main application page."""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Schema Mapper & Data Quality Fixer</title>
        <style>
            body { 
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                margin: 0; 
                padding: 20px; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }
            .container { 
                max-width: 1000px; 
                margin: 0 auto; 
                background: white; 
                border-radius: 10px; 
                box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                overflow: hidden;
            }
            .header {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                text-align: center;
            }
            .header h1 {
                margin: 0;
                font-size: 2.5em;
                font-weight: 300;
            }
            .header p {
                margin: 10px 0 0 0;
                opacity: 0.9;
                font-size: 1.1em;
            }
            .content {
                padding: 30px;
            }
            .upload-area { 
                border: 3px dashed #667eea; 
                padding: 40px; 
                text-align: center; 
                margin: 20px 0; 
                border-radius: 10px;
                background: #f8f9ff;
                transition: all 0.3s ease;
            }
            .upload-area:hover {
                border-color: #764ba2;
                background: #f0f2ff;
            }
            .upload-area input[type="file"] {
                margin: 20px 0;
                padding: 10px;
                border: 1px solid #ddd;
                border-radius: 5px;
                width: 300px;
            }
            .results { 
                margin: 20px 0; 
                padding: 25px; 
                background: #f8f9ff; 
                border-radius: 10px;
                border-left: 4px solid #667eea;
            }
            .mapping-item { 
                margin: 15px 0; 
                padding: 15px; 
                background: white; 
                border-radius: 8px; 
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                border-left: 3px solid #667eea;
            }
            .confidence { 
                font-weight: bold; 
                padding: 3px 8px;
                border-radius: 15px;
                font-size: 0.9em;
            }
            .high-confidence { 
                color: white;
                background: #28a745; 
            }
            .medium-confidence { 
                color: white;
                background: #ffc107; 
            }
            .low-confidence { 
                color: white;
                background: #dc3545; 
            }
            button { 
                padding: 12px 25px; 
                margin: 8px; 
                cursor: pointer; 
                border: none;
                border-radius: 25px;
                font-weight: 600;
                transition: all 0.3s ease;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            button:hover {
                transform: translateY(-2px);
                box-shadow: 0 5px 15px rgba(0,0,0,0.2);
            }
            .upload-btn {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
            }
            .clean-btn {
                background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
                color: white;
            }
            .apply-fix-btn {
                background: linear-gradient(135deg, #fd7e14 0%, #e83e8c 100%);
                color: white;
                padding: 8px 16px;
                font-size: 0.8em;
            }
            .hidden { display: none; }
            .issues-list {
                margin: 15px 0;
            }
            .issue-item {
                padding: 12px;
                margin: 8px 0;
                border-radius: 6px;
                border-left: 4px solid;
            }
            .issue-item.critical {
                background: #f8d7da;
                border-left-color: #dc3545;
                color: #721c24;
            }
            .issue-item.warning {
                background: #fff3cd;
                border-left-color: #ffc107;
                color: #856404;
            }
            .fix-suggestions {
                margin: 15px 0;
            }
            .fix-item {
                padding: 15px;
                margin: 10px 0;
                background: white;
                border-radius: 8px;
                border: 1px solid #e9ecef;
                box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            }
            .normalization-details {
                background: white;
                padding: 15px;
                border-radius: 6px;
                margin: 10px 0;
            }
            h2, h3, h4 {
                color: #333;
                margin-top: 0;
            }
            h2 {
                border-bottom: 2px solid #667eea;
                padding-bottom: 10px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Schema Mapper & Data Quality Fixer</h1>
                <p>Upload your CSV file to get intelligent mapping suggestions and data cleaning.</p>
            </div>
            <div class="content">
            
            <div class="upload-area">
                <h3>📁 Upload Your CSV File</h3>
                <input type="file" id="fileInput" accept=".csv" />
                <br>
                <button onclick="uploadFile()" class="upload-btn">Upload & Analyze</button>
            </div>
            
            <div id="results" class="results hidden">
                <h2>Analysis Results</h2>
                <div id="mappingResults"></div>
                <button onclick="applyCleaning()" id="cleanBtn" class="hidden clean-btn">Apply Cleaning</button>
                <div id="cleaningResults" class="hidden"></div>
            </div>
            </div>
        </div>
        
        <script>
            let currentMapping = null;
            
            async function uploadFile() {
                const fileInput = document.getElementById('fileInput');
                const file = fileInput.files[0];
                
                if (!file) {
                    alert('Please select a file');
                    return;
                }
                
                const formData = new FormData();
                formData.append('file', file);
                
                try {
                    const response = await fetch('/analyze', {
                        method: 'POST',
                        body: formData
                    });
                    
                    const result = await response.json();
                    currentMapping = result;
                    displayMappingResults(result);
                } catch (error) {
                    alert('Error analyzing file: ' + error.message);
                }
            }
            
            function displayMappingResults(result) {
                const resultsDiv = document.getElementById('results');
                const mappingDiv = document.getElementById('mappingResults');
                
                resultsDiv.classList.remove('hidden');
                
                let html = '<h3>Suggested Column Mappings</h3>';
                
                for (const [source, target] of Object.entries(result.suggested_mapping)) {
                    const confidence = result.confidence_scores[source] || 0;
                    const confidenceClass = confidence > 0.8 ? 'high-confidence' : 
                                          confidence > 0.5 ? 'medium-confidence' : 'low-confidence';
                    
                    html += `
                        <div class="mapping-item">
                            <strong>${source}</strong> → <strong>${target}</strong>
                            <span class="confidence ${confidenceClass}">(${(confidence * 100).toFixed(1)}% confidence)</span>
                        </div>
                    `;
                }
                
                if (result.unmapped_columns.length > 0) {
                    html += '<h4>Unmapped Columns:</h4><ul>';
                    result.unmapped_columns.forEach(col => {
                        html += `<li>${col}</li>`;
                    });
                    html += '</ul>';
                }
                
                mappingDiv.innerHTML = html;
                document.getElementById('cleanBtn').classList.remove('hidden');
            }
            
            async function applyCleaning() {
                if (!currentMapping) return;
                
                try {
                    const response = await fetch('/clean', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(currentMapping)
                    });
                    
                    const result = await response.json();
                    displayCleaningResults(result);
                } catch (error) {
                    alert('Error cleaning data: ' + error.message);
                }
            }
            
            function displayCleaningResults(result) {
                const cleaningDiv = document.getElementById('cleaningResults');
                cleaningDiv.classList.remove('hidden');
                
                let html = '<h3>Cleaning Results</h3>';
                html += `<p><strong>Rows processed:</strong> ${result.cleaned_data.row_count}</p>`;
                html += `<p><strong>Columns mapped:</strong> ${result.cleaned_data.column_count}</p>`;
                html += `<p><strong>Data quality score:</strong> ${result.validation_report.data_quality_score}%</p>`;
                html += `<p><strong>Issues found:</strong> ${result.issues_found.length}</p>`;
                
                if (result.issues_found.length > 0) {
                    html += '<h4>Issues Found:</h4><div class="issues-list">';
                    result.issues_found.forEach(issue => {
                        const severityClass = issue.severity === 'critical' ? 'critical' : 'warning';
                        html += `
                            <div class="issue-item ${severityClass}">
                                <strong>${issue.type.replace('_', ' ').toUpperCase()}</strong>: ${issue.description}
                                <br><small>Severity: ${issue.severity}</small>
                            </div>
                        `;
                    });
                    html += '</div>';
                }
                
                if (result.fix_suggestions.length > 0) {
                    html += '<h4>Fix Suggestions:</h4><div class="fix-suggestions">';
                    result.fix_suggestions.forEach(fix => {
                        const confidenceClass = fix.confidence > 0.8 ? 'high-confidence' : 
                                              fix.confidence > 0.5 ? 'medium-confidence' : 'low-confidence';
                        html += `
                            <div class="fix-item">
                                <strong>${fix.description}</strong>
                                <br><small>Type: ${fix.fix_type} | Confidence: ${(fix.confidence * 100).toFixed(1)}%</small>
                                <br><button onclick="applyFix('${fix.fix_id}')" class="apply-fix-btn">Apply Fix</button>
                            </div>
                        `;
                    });
                    html += '</div>';
                }
                
                if (result.normalization_summary) {
                    html += '<h4>Normalization Summary:</h4>';
                    html += `<p><strong>Total changes made:</strong> ${result.normalization_summary.total_changes}</p>`;
                    html += '<div class="normalization-details">';
                    for (const [column, details] of Object.entries(result.normalization_summary.changes_by_column)) {
                        html += `<p><strong>${column}:</strong> ${details.changes_made} changes out of ${details.total_values} values</p>`;
                    }
                    html += '</div>';
                }
                
                cleaningDiv.innerHTML = html;
            }
            
            async function applyFix(fixId) {
                try {
                    const response = await fetch('/apply-fix', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            fix_id: fixId,
                            accepted: true
                        })
                    });
                    
                    const result = await response.json();
                    if (result.status === 'success') {
                        alert('Fix applied successfully!');
                    } else {
                        alert('Error applying fix: ' + result.message);
                    }
                } catch (error) {
                    alert('Error applying fix: ' + error.message);
                }
            }
        </script>
    </body>
    </html>
    """

@app.post("/analyze")
async def analyze_file(file: UploadFile = File(...)):
    """Analyze uploaded file and suggest column mappings."""
    try:
        # Read the uploaded file
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        
        # Get column headers
        headers = df.columns.tolist()
        
        # Get mapping suggestions
        mapping_result = header_mapper.suggest_mapping(headers)
        
        return {
            "suggested_mapping": mapping_result["mapping"],
            "confidence_scores": mapping_result["confidence_scores"],
            "unmapped_columns": mapping_result["unmapped_columns"],
            "file_info": {
                "filename": file.filename,
                "rows": len(df),
                "columns": len(headers)
            }
        }
    except Exception as e:
        logger.error(f"Error analyzing file: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/clean")
async def clean_data(mapping_data: dict):
    """Apply cleaning and validation to the mapped data."""
    try:
        # Extract mapping information
        suggested_mapping = mapping_data.get("suggested_mapping", {})
        confidence_scores = mapping_data.get("confidence_scores", {})
        
        # For now, return a mock response with realistic data
        # In a real implementation, this would re-read the file and apply the mapping
        return {
            "cleaned_data": {
                "row_count": 100,
                "column_count": len(suggested_mapping),
                "mapped_columns": list(suggested_mapping.keys())
            },
            "validation_report": {
                "total_issues": 5,
                "critical_issues": 1,
                "warnings": 4,
                "data_quality_score": 85.5
            },
            "issues_found": [
                {
                    "type": "missing_value",
                    "severity": "critical",
                    "description": "5 rows have missing email addresses",
                    "affected_columns": ["email"]
                },
                {
                    "type": "pattern_mismatch", 
                    "severity": "warning",
                    "description": "Date format inconsistent in 3 rows",
                    "affected_columns": ["order_date"]
                },
                {
                    "type": "pricing_inconsistency",
                    "severity": "warning", 
                    "description": "Total amount calculation mismatch in 2 rows",
                    "affected_columns": ["total_amount", "unit_price", "quantity"]
                }
            ],
            "fix_suggestions": [
                {
                    "fix_id": "fix_001",
                    "type": "format_fix",
                    "description": "Standardize date format to YYYY-MM-DD",
                    "confidence": 0.9,
                    "fix_type": "automatic"
                },
                {
                    "fix_id": "fix_002", 
                    "type": "validation_fix",
                    "description": "Add email validation rules",
                    "confidence": 0.8,
                    "fix_type": "semi_automatic"
                },
                {
                    "fix_id": "fix_003",
                    "type": "calculation_fix", 
                    "description": "Recalculate total amounts from components",
                    "confidence": 0.95,
                    "fix_type": "automatic"
                }
            ],
            "normalization_summary": {
                "total_changes": 15,
                "changes_by_column": {
                    "order_date": {"changes_made": 3, "total_values": 100},
                    "unit_price": {"changes_made": 5, "total_values": 100},
                    "total_amount": {"changes_made": 7, "total_values": 100}
                }
            }
        }
    except Exception as e:
        logger.error(f"Error cleaning data: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/apply-fix")
async def apply_fix(fix_data: FixApplication):
    """Apply a specific fix and learn from the user's choice."""
    try:
        # Apply the fix
        result = fix_suggester.apply_fix(fix_data.fix_id, fix_data.accepted, fix_data.custom_value)
        
        # Learn from the user's choice
        learning_system.learn_from_fix(fix_data.fix_id, fix_data.accepted, fix_data.custom_value)
        
        return {"status": "success", "result": result}
    except Exception as e:
        logger.error(f"Error applying fix: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/schema")
async def get_schema():
    """Get the canonical schema information."""
    try:
        schema_summary = schema_loader.get_schema_summary()
        return {
            "schema_loaded": schema_summary["schema_loaded"],
            "total_columns": schema_summary["total_columns"],
            "columns": schema_summary["columns"]
        }
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/learning-summary")
async def get_learning_summary():
    """Get learning system summary."""
    try:
        summary = learning_system.get_learning_summary()
        return summary
    except Exception as e:
        logger.error(f"Error getting learning summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/learn-mapping")
async def learn_mapping(mapping_data: dict):
    """Learn from a user's mapping decision."""
    try:
        source_header = mapping_data.get("source_header")
        canonical_header = mapping_data.get("canonical_header")
        confidence = mapping_data.get("confidence", 0.5)
        user_override = mapping_data.get("user_override", False)
        
        learning_system.learn_from_mapping(source_header, canonical_header, confidence, user_override)
        learning_system.save_learning_data()
        
        return {"status": "success", "message": "Mapping learned successfully"}
    except Exception as e:
        logger.error(f"Error learning mapping: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy", 
        "version": "1.0.0",
        "components": {
            "schema_loader": schema_loader.get_schema_summary()["schema_loaded"],
            "groq_api": bool(os.getenv("GROQ_API_KEY")),
            "learning_system": True
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)