#!/usr/bin/env python3
"""
Startup script for the FastAPI backend
"""
import sys
import os
import subprocess
import time

# Add backend to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

def check_ollama():
    """Check if Ollama is running and has the required model"""
    try:
        # Check if ollama is running
        result = subprocess.run(['ollama', 'list'], capture_output=True, text=True)
        if result.returncode != 0:
            print("⚠️  Ollama is not running. Starting Ollama...")
            subprocess.Popen(['ollama', 'serve'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(5)
        
        # Check if llama2 model is available
        result = subprocess.run(['ollama', 'list'], capture_output=True, text=True)
        if 'llama2' not in result.stdout:
            print("📥 Downloading llama2 model (this may take a while)...")
            subprocess.run(['ollama', 'pull', 'llama2'], check=True)
            print("✅ llama2 model downloaded successfully!")
        
        return True
    except Exception as e:
        print(f"⚠️  Ollama setup failed: {e}")
        print("The application will work without AI features.")
        return False

def main():
    """Main startup function"""
    print("🚀 Starting Schema Mapper & Data Quality Fixer Backend")
    
    # Check Ollama
    ollama_available = check_ollama()
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    # Start the FastAPI server
    print("🌐 Starting FastAPI server on http://localhost:8000")
    print("📚 API documentation available at http://localhost:8000/docs")
    
    try:
        import uvicorn
        from backend.main import app
        uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
    except ImportError:
        print("❌ Required packages not installed. Please run: pip install -r requirements.txt")
    except Exception as e:
        print(f"❌ Error starting server: {e}")

if __name__ == "__main__":
    main()