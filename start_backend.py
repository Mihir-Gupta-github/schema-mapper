"""
Start the FastAPI backend server
"""
import uvicorn
import os
from pathlib import Path

# Set working directory to project root
os.chdir(Path(__file__).parent)

if __name__ == "__main__":
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )