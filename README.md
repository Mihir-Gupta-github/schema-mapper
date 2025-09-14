# Schema Mapper & Data Quality Fixer

An intelligent data processing application that automatically maps CSV columns to a canonical schema, cleans data using deterministic rules, and provides targeted fix suggestions using LLM-powered analysis.

## Features

- **Automatic Column Mapping**: Intelligently maps source columns to canonical schema with confidence scoring
- **Deterministic Data Cleaning**: Applies rule-based cleaning for common data quality issues
- **LLM-Powered Fix Suggestions**: Provides targeted suggestions for remaining data quality issues
- **Interactive Manual Override**: Allows manual column mapping and fix selection
- **Learning System**: Remembers and promotes successful fixes for future use
- **Before/After Comparison**: Visual comparison of data transformations
- **Clean Data Download**: Export cleaned data in canonical format

## Architecture

The application uses an agentic AI architecture with LangGraph orchestration:

- **Schema Mapper Agent**: Handles column mapping with rule-based and semantic matching
- **Data Cleaner Agent**: Applies deterministic cleaning rules
- **LLM Fix Agent**: Generates targeted fix suggestions using OpenAI
- **Data Processing Graph**: Orchestrates the entire pipeline using LangGraph

## Tech Stack

- **Frontend**: Streamlit
- **Backend**: FastAPI
- **AI Orchestration**: LangGraph
- **LLM**: OpenAI GPT-3.5-turbo
- **Data Processing**: Pandas, NumPy
- **Visualization**: Plotly

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables:
```bash
cp .env.example .env
# Edit .env and add your OpenAI API key
```

## Usage

### Start the Backend Server
```bash
python start_backend.py
```

### Start the Frontend Interface
```bash
python start_frontend.py
```

### Access the Application
- Frontend: http://localhost:8501
- Backend API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

## User Flow

1. **Upload**: Upload a CSV file through the Streamlit interface
2. **Automatic Processing**: System automatically maps columns and applies cleaning rules
3. **Review Mappings**: Review automatic column mappings with confidence scores
4. **Manual Mapping**: Manually map any unmapped columns
5. **Quality Check**: Review cleaning results and validation errors
6. **Apply Fixes**: Select and apply LLM-generated fix suggestions
7. **Download**: Download the final cleaned dataset

## Sample Data

The application comes with sample data files to test different scenarios:

- `Project6InputData1.csv`: Clean data with canonical-like headers
- `Project6InputData2.csv`: Messy data with formatting issues
- `Project6InputData3.csv`: Different headers with missing columns and extra fields

## API Endpoints

- `POST /upload`: Upload and process CSV file
- `GET /schema`: Get canonical schema
- `POST /manual-mapping/{session_id}`: Apply manual column mappings
- `POST /apply-fixes/{session_id}`: Apply selected fix suggestions
- `GET /result/{session_id}`: Get processing results
- `GET /download/{session_id}`: Download cleaned data

## Configuration

The canonical schema is defined in `canonicalSchema/Project6StdFormat.csv` with columns:
- `canonical_name`: The target column name
- `description`: Description of the column
- `example`: Example value for the column

## Learning System

The application learns from user interactions:
- Successful manual mappings are remembered
- Applied fixes are promoted to the cleaning rules
- Confidence scores improve over time

## Error Handling

- Comprehensive error handling for file uploads
- Validation of data formats and types
- Graceful fallbacks for LLM failures
- Clear error messages and suggestions

## Performance

- Deterministic rules are applied first for speed
- LLM is used only for targeted suggestions
- Session-based processing for scalability
- Efficient data storage and retrieval

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.