from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import pandas as pd
import json
import os
from datetime import datetime
import logging
import numpy as np

from .services.schema_mapper import SchemaMapper
from .services.data_cleaner import DataCleaner
from .services.fix_suggester import FixSuggester
from .models.schemas import (
    MappingRequest, MappingResponse, CleaningRequest, 
    CleaningResponse, FixRequest, FixResponse, FixPromotionRequest
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Schema Mapper & Data Quality Fixer", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
schema_mapper = SchemaMapper()
data_cleaner = DataCleaner()
fix_suggester = FixSuggester()

# Data persistence directory
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def convert_numpy_types(obj):
    """Recursively convert numpy types to native Python types"""
    if isinstance(obj, (np.integer, np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float16, np.float32, np.float64)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_numpy_types(item) for item in obj)
    else:
        return obj

@app.get("/")
async def root():
    return {"message": "Schema Mapper & Data Quality Fixer API"}

@app.get("/canonical-schema")
async def get_canonical_schema():
    """Get the canonical schema definition"""
    try:
        schema_path = "Project6StdFormat.csv"
        if os.path.exists(schema_path):
            df = pd.read_csv(schema_path)
            return {
                "schema": df.to_dict('records'),
                "columns": df['name'].tolist()
            }
        else:
            raise HTTPException(status_code=404, detail="Canonical schema not found")
    except Exception as e:
        logger.error(f"Error loading canonical schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/map-schema", response_model=MappingResponse)
async def map_schema(request: MappingRequest):
    """Map uploaded CSV columns to canonical schema"""
    try:
        # Save uploaded file temporarily
        temp_path = f"{DATA_DIR}/temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        logger.info(f"Processing file content of type: {type(request.file_content)}")
        logger.info(f"File content length: {len(request.file_content) if hasattr(request.file_content, '__len__') else 'unknown'}")
        logger.info(f"File content preview: {str(request.file_content)[:100]}...")
        
        with open(temp_path, "wb") as buffer:
            if isinstance(request.file_content, str):
                decoded_content = bytes.fromhex(request.file_content)
                logger.info(f"Decoded content length: {len(decoded_content)}")
                logger.info(f"Decoded content preview: {decoded_content[:100]}...")
                buffer.write(decoded_content)
            else:
                buffer.write(request.file_content)
        
        # Check if file was written correctly
        file_size = os.path.getsize(temp_path)
        logger.info(f"Temp file size: {file_size} bytes")
        
        # Read the CSV
        df = pd.read_csv(temp_path)
        logger.info(f"CSV loaded with {len(df)} rows and {len(df.columns)} columns")
        logger.info(f"Columns: {df.columns.tolist()}")
        
        # Get canonical schema
        canonical_schema = await get_canonical_schema()
        canonical_columns = canonical_schema["columns"]
        
        # Perform mapping
        mapping_result = schema_mapper.map_columns(
            df.columns.tolist(), 
            canonical_columns,
            request.custom_mappings or {}
        )
        
        # Clean up temp file
        os.remove(temp_path)
        
        return MappingResponse(
            success=True,
            suggested_mapping=mapping_result["mapping"],
            confidence_scores=mapping_result["confidence_scores"],
            unmapped_columns=mapping_result["unmapped_columns"],
            extra_columns=mapping_result["extra_columns"]
        )
        
    except Exception as e:
        logger.error(f"Error in schema mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/clean-data")
async def clean_data(request: CleaningRequest):
    """Clean and validate data according to mapping"""
    try:
        # Save uploaded file temporarily
        temp_path = f"{DATA_DIR}/temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        logger.info(f"Cleaning: Processing file content of type: {type(request.file_content)}")
        logger.info(f"Cleaning: File content length: {len(request.file_content) if hasattr(request.file_content, '__len__') else 'unknown'}")
        
        with open(temp_path, "wb") as buffer:
            if isinstance(request.file_content, str):
                decoded_content = bytes.fromhex(request.file_content)
                logger.info(f"Cleaning: Decoded content length: {len(decoded_content)}")
                buffer.write(decoded_content)
            else:
                buffer.write(request.file_content)
        
        # Check if file was written correctly
        file_size = os.path.getsize(temp_path)
        logger.info(f"Cleaning: Temp file size: {file_size} bytes")
        
        # Read the CSV
        df = pd.read_csv(temp_path)
        logger.info(f"Cleaning: CSV loaded with {len(df)} rows and {len(df.columns)} columns")
        
        # Clean the data
        logger.info(f"Cleaning: Starting data cleaning with {len(request.column_mapping)} mappings")
        cleaning_result = data_cleaner.clean_dataframe(
            df, 
            request.column_mapping,
            request.cleaning_rules or {}
        )
        logger.info(f"Cleaning: Data cleaning completed successfully")
        
        # Save cleaned data
        cleaned_path = f"{DATA_DIR}/cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        logger.info(f"Cleaning: Saving cleaned data to {cleaned_path}")
        cleaning_result["cleaned_df"].to_csv(cleaned_path, index=False)
        logger.info(f"Cleaning: Cleaned data saved successfully")
        
        # Clean up temp file
        os.remove(temp_path)
        logger.info(f"Cleaning: Temp file removed")
        
        logger.info(f"Cleaning: Creating response")
        
        # Convert numpy types to native Python types
        before_metrics = convert_numpy_types(cleaning_result["before_metrics"])
        after_metrics = convert_numpy_types(cleaning_result["after_metrics"])
        issues_found = convert_numpy_types(cleaning_result["issues_found"])
        cleaning_applied = convert_numpy_types(cleaning_result["cleaning_applied"])
        
        # Create a simple dict response first to test
        response_data = {
            "success": True,
            "before_metrics": before_metrics,
            "after_metrics": after_metrics,
            "issues_found": issues_found,
            "cleaning_applied": cleaning_applied,
            "cleaned_file_path": cleaned_path
        }
        
        logger.info(f"Cleaning: Response data created, testing serialization...")
        
        # Test if the data can be serialized
        try:
            json.dumps(response_data)
            logger.info(f"Cleaning: Data serialization test passed")
        except Exception as e:
            logger.error(f"Cleaning: Data serialization test failed: {e}")
            raise HTTPException(status_code=500, detail=f"Serialization error: {e}")
        
        logger.info(f"Cleaning: Returning response data directly")
        return response_data
        
    except Exception as e:
        logger.error(f"Error in data cleaning: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/suggest-fixes", response_model=FixResponse)
async def suggest_fixes(request: FixRequest):
    """Get targeted fix suggestions for remaining issues"""
    try:
        # Load the cleaned data
        df = pd.read_csv(request.cleaned_file_path)
        
        # Get fix suggestions
        fix_suggestions = fix_suggester.suggest_fixes(
            df,
            request.issues,
            request.column_mapping
        )
        
        return FixResponse(
            success=True,
            fix_suggestions=fix_suggestions
        )
        
    except Exception as e:
        logger.error(f"Error suggesting fixes: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/promote-fix")
async def promote_fix(request: FixPromotionRequest):
    """Promote a fix to the cleaning rules for future use"""
    try:
        # Save the promoted fix to the cleaning rules
        success = fix_suggester.promote_fix(
            request.fix_type,
            request.pattern,
            request.solution,
            request.confidence
        )
        
        return {"success": success, "message": "Fix promoted successfully"}
        
    except Exception as e:
        logger.error(f"Error promoting fix: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cleaning-rules")
async def get_cleaning_rules():
    """Get current cleaning rules"""
    try:
        rules = fix_suggester.get_cleaning_rules()
        return {"rules": rules}
    except Exception as e:
        logger.error(f"Error getting cleaning rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)