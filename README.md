# Schema Mapper & Data Quality Fixer

A comprehensive application that provides intelligent schema mapping, data cleaning, and targeted fix suggestions for CSV files. Built with FastAPI backend, Streamlit frontend, and Ollama/LangChain for AI-powered features.

## Features

- **🔍 Intelligent Schema Mapping**: Automatically maps CSV columns to canonical schema with confidence scores
- **🧹 Deterministic Data Cleaning**: Applies consistent cleaning rules for data normalization
- **🔧 Targeted Fix Suggestions**: AI-powered suggestions for remaining data quality issues
- **📚 Learning System**: Promotes good fixes to improve future processing
- **📊 Before/After Metrics**: Clear visualization of data quality improvements
- **🎯 One-Click Processing**: Simple workflow from upload to clean data

## Architecture

- **Backend**: FastAPI with intelligent mapping, cleaning, and fix suggestion services
- **Frontend**: Streamlit with intuitive step-by-step interface
- **AI**: Ollama with LangChain for targeted fix suggestions
- **Storage**: JSON-based persistence for learned fixes and cleaning rules

## Quick Start

### Prerequisites

- Python 3.8+
- Ollama (for AI features)
- Required Python packages (see requirements.txt)

### Installation

1. **Clone and setup**:
   ```bash
   cd /workspace
   pip install -r requirements.txt
   ```

2. **Start Ollama** (for AI features):
   ```bash
   # Install Ollama if not already installed
   curl -fsSL https://ollama.ai/install.sh | sh
   
   # Start Ollama service
   ollama serve
   
   # Download the required model (in another terminal)
   ollama pull llama2
   ```

3. **Start the backend**:
   ```bash
   python start_backend.py
   ```
   The API will be available at http://localhost:8000

4. **Start the frontend** (in another terminal):
   ```bash
   python start_frontend.py
   ```
   The UI will be available at http://localhost:8501

## Usage

### 1. Upload Your File
- Upload a CSV file or use the provided sample files
- Preview the data and column information

### 2. Schema Mapping
- Review automatic column mapping suggestions with confidence scores
- Override mappings as needed
- See unmapped and extra columns

### 3. Data Cleaning
- Configure cleaning options (whitespace, case, currency, dates, etc.)
- Run deterministic cleaning and validation
- Review before/after metrics

### 4. Fix Suggestions
- Get targeted AI-powered fix suggestions for remaining issues
- Apply, reject, or promote fixes
- Promoted fixes are learned for future use

### 5. Download Results
- Download the cleaned CSV file
- Review final data quality metrics

## Sample Data

The application includes three sample datasets to test different scenarios:

- **Project6InputData1.csv**: Clean, canonical-like headers
- **Project6InputData2.csv**: Messy headers, mixed date formats, currency symbols
- **Project6InputData3.csv**: Different headers, missing columns, extra columns

## API Endpoints

- `GET /canonical-schema`: Get the canonical schema definition
- `POST /map-schema`: Map CSV columns to canonical schema
- `POST /clean-data`: Clean and validate data
- `POST /suggest-fixes`: Get targeted fix suggestions
- `POST /promote-fix`: Promote a fix to the learning system
- `GET /cleaning-rules`: Get current cleaning rules

## Configuration

### Canonical Schema
The canonical schema is defined in `Project6StdFormat.csv` with columns:
- customer_id, customer_name, email, phone, address
- city, state, postal_code, country, registration_date
- tax_id, annual_revenue, industry, employee_count, status

### Cleaning Rules
Cleaning rules are stored in `data/cleaning_rules.json` and include:
- Whitespace trimming
- Case normalization
- Currency symbol removal
- Date format normalization
- Format validation

### Learned Fixes
Promoted fixes are stored in `data/learned_fixes.json` for reuse in future processing.

## Development

### Project Structure
```
/workspace
├── backend/
│   ├── main.py                 # FastAPI application
│   ├── models/
│   │   └── schemas.py         # Pydantic models
│   └── services/
│       ├── schema_mapper.py   # Schema mapping logic
│       ├── data_cleaner.py    # Data cleaning logic
│       └── fix_suggester.py   # Fix suggestion logic
├── frontend/
│   └── app.py                 # Streamlit application
├── data/                      # Data persistence
├── requirements.txt           # Python dependencies
├── start_backend.py          # Backend startup script
├── start_frontend.py         # Frontend startup script
└── README.md                 # This file
```

### Adding New Cleaning Rules
1. Modify `backend/services/data_cleaner.py`
2. Add new patterns to `default_patterns`
3. Update the cleaning logic in `_apply_default_cleaning`

### Adding New Fix Types
1. Modify `backend/services/fix_suggester.py`
2. Add new fix suggestion methods
3. Update the `suggest_fixes` method to handle new issue types

## Troubleshooting

### Ollama Issues
- Ensure Ollama is running: `ollama serve`
- Check if llama2 model is available: `ollama list`
- Download model if missing: `ollama pull llama2`

### Backend Issues
- Check if port 8000 is available
- Verify all dependencies are installed
- Check logs for specific error messages

### Frontend Issues
- Ensure backend is running on port 8000
- Check if port 8501 is available
- Verify Streamlit is installed correctly

## License

This project is part of a data quality solution demonstration.