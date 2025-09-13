#!/usr/bin/env python3
"""
Startup script for the Streamlit frontend
"""
import sys
import os
import subprocess
import time
import requests

def check_backend():
    """Check if the backend is running"""
    try:
        response = requests.get("http://localhost:8000/", timeout=5)
        return response.status_code == 200
    except:
        return False

def main():
    """Main startup function"""
    print("🚀 Starting Schema Mapper & Data Quality Fixer Frontend")
    
    # Check if backend is running
    print("🔍 Checking backend connection...")
    if not check_backend():
        print("⚠️  Backend is not running. Please start the backend first:")
        print("   python start_backend.py")
        print("   or")
        print("   cd backend && python main.py")
        return
    
    print("✅ Backend is running!")
    
    # Start Streamlit
    print("🌐 Starting Streamlit frontend on http://localhost:8501")
    
    try:
        # Change to frontend directory
        os.chdir('frontend')
        
        # Start Streamlit
        subprocess.run([
            sys.executable, '-m', 'streamlit', 'run', 'app.py',
            '--server.port', '8501',
            '--server.address', '0.0.0.0',
            '--browser.gatherUsageStats', 'false'
        ])
    except KeyboardInterrupt:
        print("\n👋 Frontend stopped by user")
    except Exception as e:
        print(f"❌ Error starting frontend: {e}")

if __name__ == "__main__":
    main()