"""
FastAPI Backend for Schema Mapper & Data Quality Fixer
"""
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import json
from typing import Dict, List, Any, Optional
import os
from pathlib import Path

from agents import DataProcessingGraph
from utils.schema_loader import load_canonical_schema
from utils.data_utils import save_processing_result, load_processing_result


app = FastAPI(title="Schema Mapper & Data Quality Fixer API", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables
canonical_schema = None
processing_graph = None


@app.on_event("startup")
async def startup_event():
    """Initialize the application"""
    global canonical_schema, processing_graph
    
    # Load canonical schema
    schema_path = Path(__file__).parent.parent / "canonicalSchema" / "Project6StdFormat.csv"
    canonical_schema = load_canonical_schema(schema_path)
    
    # Initialize processing graph
    processing_graph = DataProcessingGraph(canonical_schema)


@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Schema Mapper & Data Quality Fixer API"}


@app.get("/schema")
async def get_canonical_schema():
    """Get the canonical schema"""
    return {"schema": canonical_schema}


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload and process a CSV file"""
    try:
        # Validate file type
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="File must be a CSV")
        
        # Read the CSV file
        import io
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        
        # Process the data
        result = processing_graph.process_data(df)
        
        # Save processing result
        session_id = save_processing_result(result)
        
        return {
            "session_id": session_id,
            "summary": result["processing_summary"],
            "column_mappings": [
                {
                    "source": mapping.source_column,
                    "target": mapping.target_column,
                    "confidence": mapping.confidence,
                    "type": mapping.mapping_type
                }
                for mapping in result["column_mappings"]
            ],
            "unmapped_columns": result["unmapped_columns"],
            "cleaning_results": [
                {
                    "column": cleaning.column,
                    "original_value": str(cleaning.original_value),
                    "cleaned_value": str(cleaning.cleaned_value),
                    "rule_applied": cleaning.rule_applied,
                    "confidence": cleaning.confidence
                }
                for cleaning in result["cleaning_results"]
            ],
            "validation_errors": result["validation_errors"],
            "fix_suggestions": [
                {
                    "column": fix.column,
                    "row_index": fix.row_index,
                    "original_value": str(fix.original_value),
                    "suggested_value": str(fix.suggested_value),
                    "reason": fix.reason,
                    "confidence": fix.confidence,
                    "fix_type": fix.fix_type
                }
                for fix in result["fix_suggestions"]
            ]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.post("/manual-mapping/{session_id}")
async def apply_manual_mapping(session_id: str, mappings: Dict[str, str]):
    """Apply manual column mappings"""
    try:
        # Load existing result
        result = load_processing_result(session_id)
        if not result:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Get original dataframe
        original_df = result["raw_dataframe"]
        
        # Process with manual mappings
        new_result = processing_graph.process_data(
            original_df, 
            manual_mappings=mappings
        )
        
        # Update the result
        result.update(new_result)
        save_processing_result(result, session_id)
        
        return {
            "message": "Manual mappings applied successfully",
            "summary": result["processing_summary"]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error applying manual mappings: {str(e)}")


@app.post("/apply-fixes/{session_id}")
async def apply_fixes(session_id: str, fixes: List[Dict[str, Any]]):
    """Apply selected fix suggestions"""
    try:
        # Load existing result
        result = load_processing_result(session_id)
        if not result:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Get original dataframe
        original_df = result["raw_dataframe"]
        
        # Process with applied fixes
        new_result = processing_graph.process_data(
            original_df,
            applied_fixes=fixes
        )
        
        # Update the result
        result.update(new_result)
        save_processing_result(result, session_id)
        
        return {
            "message": "Fixes applied successfully",
            "summary": result["processing_summary"]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error applying fixes: {str(e)}")


@app.get("/result/{session_id}")
async def get_processing_result(session_id: str):
    """Get processing result for a session"""
    try:
        result = load_processing_result(session_id)
        if not result:
            raise HTTPException(status_code=404, detail="Session not found")
        
        return {
            "summary": result["processing_summary"],
            "final_dataframe": result["final_dataframe"].to_dict('records'),
            "column_mappings": [
                {
                    "source": mapping.source_column,
                    "target": mapping.target_column,
                    "confidence": mapping.confidence,
                    "type": mapping.mapping_type
                }
                for mapping in result["column_mappings"]
            ],
            "cleaning_results": [
                {
                    "column": cleaning.column,
                    "original_value": str(cleaning.original_value),
                    "cleaned_value": str(cleaning.cleaned_value),
                    "rule_applied": cleaning.rule_applied,
                    "confidence": cleaning.confidence
                }
                for cleaning in result["cleaning_results"]
            ],
            "validation_errors": result["validation_errors"],
            "fix_suggestions": [
                {
                    "column": fix.column,
                    "row_index": fix.row_index,
                    "original_value": str(fix.original_value),
                    "suggested_value": str(fix.suggested_value),
                    "reason": fix.reason,
                    "confidence": fix.confidence,
                    "fix_type": fix.fix_type
                }
                for fix in result["fix_suggestions"]
            ]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving result: {str(e)}")


@app.get("/download/{session_id}")
async def download_cleaned_data(session_id: str):
    """Download the cleaned data as CSV"""
    try:
        result = load_processing_result(session_id)
        if not result:
            raise HTTPException(status_code=404, detail="Session not found")
        
        df = result["final_dataframe"]
        
        # Convert to CSV
        csv_content = df.to_csv(index=False)
        
        return JSONResponse(
            content={"csv_content": csv_content},
            headers={"Content-Disposition": f"attachment; filename=cleaned_data_{session_id}.csv"}
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error downloading data: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "canonical_schema_loaded": canonical_schema is not None}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)