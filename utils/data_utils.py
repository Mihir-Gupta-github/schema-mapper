"""
Data utilities for processing and storage
"""
import pandas as pd
import json
import uuid
from typing import Dict, Any
from pathlib import Path
import os


# Create data directory if it doesn't exist
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)


def save_processing_result(result: Dict[str, Any], session_id: str = None) -> str:
    """Save processing result to disk"""
    if session_id is None:
        session_id = str(uuid.uuid4())
    
    # Convert DataFrame to dict for JSON serialization
    serializable_result = {}
    for key, value in result.items():
        if isinstance(value, pd.DataFrame):
            serializable_result[key] = value.to_dict('records')
        else:
            serializable_result[key] = value
    
    # Save to file
    file_path = DATA_DIR / f"{session_id}.json"
    with open(file_path, 'w') as f:
        json.dump(serializable_result, f, default=str)
    
    return session_id


def load_processing_result(session_id: str) -> Dict[str, Any]:
    """Load processing result from disk"""
    file_path = DATA_DIR / f"{session_id}.json"
    
    if not file_path.exists():
        return None
    
    try:
        with open(file_path, 'r') as f:
            result = json.load(f)
        
        # Convert dict back to DataFrame where needed
        if 'final_dataframe' in result:
            result['final_dataframe'] = pd.DataFrame(result['final_dataframe'])
        if 'raw_dataframe' in result:
            result['raw_dataframe'] = pd.DataFrame(result['raw_dataframe'])
        
        return result
    
    except Exception as e:
        print(f"Error loading processing result: {e}")
        return None


def cleanup_old_sessions(max_age_hours: int = 24):
    """Clean up old session files"""
    import time
    
    current_time = time.time()
    max_age_seconds = max_age_hours * 3600
    
    for file_path in DATA_DIR.glob("*.json"):
        file_age = current_time - file_path.stat().st_mtime
        if file_age > max_age_seconds:
            file_path.unlink()
            print(f"Cleaned up old session: {file_path.name}")


def get_session_info(session_id: str) -> Dict[str, Any]:
    """Get basic info about a session"""
    file_path = DATA_DIR / f"{session_id}.json"
    
    if not file_path.exists():
        return None
    
    try:
        stat = file_path.stat()
        return {
            "session_id": session_id,
            "created_at": stat.st_ctime,
            "modified_at": stat.st_mtime,
            "file_size": stat.st_size
        }
    except Exception as e:
        print(f"Error getting session info: {e}")
        return None