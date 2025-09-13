from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class MappingRequest(BaseModel):
    file_content: str  # Hex-encoded string
    custom_mappings: Optional[Dict[str, str]] = None

class MappingResponse(BaseModel):
    success: bool
    suggested_mapping: Dict[str, str]
    confidence_scores: Dict[str, float]
    unmapped_columns: List[str]
    extra_columns: List[str]

class CleaningRequest(BaseModel):
    file_content: str  # Hex-encoded string
    column_mapping: Dict[str, str]
    cleaning_rules: Optional[Dict[str, Any]] = None

class CleaningResponse(BaseModel):
    success: bool
    before_metrics: Dict[str, Any]
    after_metrics: Dict[str, Any]
    issues_found: List[Dict[str, Any]]
    cleaning_applied: List[Dict[str, Any]]
    cleaned_file_path: str

class FixRequest(BaseModel):
    cleaned_file_path: str
    issues: List[Dict[str, Any]]
    column_mapping: Dict[str, str]

class FixSuggestion(BaseModel):
    row_index: int
    column: str
    current_value: str
    suggested_value: str
    confidence: float
    reason: str
    fix_type: str

class FixResponse(BaseModel):
    success: bool
    fix_suggestions: List[FixSuggestion]

class FixPromotionRequest(BaseModel):
    fix_type: str
    pattern: str
    solution: str
    confidence: float