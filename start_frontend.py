"""
Start the Streamlit frontend
"""
import subprocess
import sys
import os
from pathlib import Path

# Set working directory to project root
os.chdir(Path(__file__).parent)

if __name__ == "__main__":
    subprocess.run([
        sys.executable, "-m", "streamlit", "run", 
        "frontend/app.py",
        "--server.port", "8501",
        "--server.address", "0.0.0.0"
    ])