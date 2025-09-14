"""
Start both backend and frontend servers
"""
import subprocess
import sys
import os
import time
import threading
from pathlib import Path

# Set working directory to project root
os.chdir(Path(__file__).parent)


def start_backend():
    """Start the FastAPI backend"""
    print("🚀 Starting FastAPI backend on http://localhost:8000")
    subprocess.run([
        sys.executable, "-m", "uvicorn", 
        "backend.main:app",
        "--host", "0.0.0.0",
        "--port", "8000",
        "--reload"
    ])


def start_frontend():
    """Start the Streamlit frontend"""
    print("🚀 Starting Streamlit frontend on http://localhost:8501")
    subprocess.run([
        sys.executable, "-m", "streamlit", "run", 
        "frontend/app.py",
        "--server.port", "8501",
        "--server.address", "0.0.0.0"
    ])


if __name__ == "__main__":
    print("🔧 Schema Mapper & Data Quality Fixer")
    print("=" * 50)
    
    # Start backend in a separate thread
    backend_thread = threading.Thread(target=start_backend)
    backend_thread.daemon = True
    backend_thread.start()
    
    # Wait a moment for backend to start
    time.sleep(3)
    
    # Start frontend
    start_frontend()