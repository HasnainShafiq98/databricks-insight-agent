# Complete Setup and Usage Guide

## 📋 Table of Contents
1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Data Pipeline Setup](#data-pipeline-setup)
5. [Running the Application](#running-the-application)
6. [Usage Examples](#usage-examples)
7. [Architecture Overview](#architecture-overview)
8. [Development Workflow](#development-workflow)

---

## Prerequisites

### Required Software
- **Python 3.9+** (3.10 or 3.11 recommended)
- **Git** for version control
- **Databricks workspace** with SQL Warehouse access
- **OpenAI API key** (optional, for enhanced query understanding)

### Databricks Setup
1. Create a Databricks workspace
2. Create a SQL Warehouse (Serverless recommended)
3. Generate a personal access token:
   - User Settings → Access Tokens → Generate New Token
4. Note your workspace URL and SQL Warehouse HTTP path

---

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/HasnainShafiq98/databricks-insight-agent.git
cd databricks-insight-agent
```

### 2. Create Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## Configuration

### 1. Environment Variables

Create a `.env` file in the project root:

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```ini
# Databricks Configuration
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_ACCESS_TOKEN=dapi1234567890abcdef

# OpenAI Configuration (optional)
OPENAI_API_KEY=sk-proj-...

# FAISS Configuration
FAISS_INDEX_PATH=./data/faiss_index.faiss
DOCUMENTS_PATH=./data/documents

# Security Configuration
MAX_QUERY_LENGTH=10000
RATE_LIMIT_PER_MINUTE=60
ALLOWED_SCHEMAS=default,analytics
ENABLE_SQL_INJECTION_PROTECTION=true

# Logging
LOG_LEVEL=INFO
```

### 2. Create Data Directories

```bash
mkdir -p data/documents data/cache data/csv
```

---

## Data Pipeline Setup

### Step 1: Prepare Your CSV Files

Place your CSV files in the `data/csv/` directory. Example structure:

```
data/csv/
  ├── sales.csv
  ├── customers.csv
  └── products.csv
```

### Step 2: Run Data Ingestion

```python
from databricks_client import DatabricksClient
from data_pipeline import DeltaTablePipeline, CSVIngestionPipeline
import os
from dotenv import load_dotenv

# Load configuration
load_dotenv()

# Initialize Databricks client
client = DatabricksClient(
    server_hostname=os.getenv('DATABRICKS_SERVER_HOSTNAME'),
    http_path=os.getenv('DATABRICKS_HTTP_PATH'),
    access_token=os.getenv('DATABRICKS_ACCESS_TOKEN')
)

# Create pipeline
delta_pipeline = DeltaTablePipeline(
    databricks_client=client,
    catalog="hive_metastore",
    schema="default"
)

# Ingest data
ingestion = CSVIngestionPipeline(delta_pipeline)

# Process sales data
ingestion.ingest_sales_data('data/csv/sales.csv')

# Process customer data
ingestion.ingest_customer_data('data/csv/customers.csv')
```

### Step 3: Verify Delta Tables

The pipeline creates three layers:

1. **Bronze Tables** (Raw data):
   - `bronze_sales`
   - `bronze_customers`
   - `bronze_products`

2. **Silver Tables** (Cleaned data):
   - `silver_sales`
   - `silver_customers`
   - `silver_products`

3. **Gold Tables** (Business metrics):
   - `gold_sales_by_region`
   - `gold_customer_metrics`

---

## Running the Application

### Option 1: Streamlit Web UI (Recommended)

```bash
streamlit run app.py
```

Access the UI at: `http://localhost:8501`

### Option 2: Command Line Interface

```bash
python main.py
```

### Option 3: Python API

```python
from agent import DatabricksInsightAgent
from databricks_client import DatabricksClient
from sql_generator import SchemaManager, SQLGenerator
from context_retriever import ContextRetriever
from security import SecurityValidator, SecurityConfig, RateLimiter, SchemaValidator

# Initialize components (see main.py for full setup)
agent = DatabricksInsightAgent(
    databricks_client=client,
    schema_manager=schema_manager,
    sql_generator=sql_generator,
    context_retriever=context_retriever,
    security_validator=security_validator
)

# Process query
response = agent.process_query("What are the top 5 products by sales?")

print(f"Insights: {response.insights}")
print(f"SQL: {response.sql_query}")
print(f"Results: {response.results}")
```

---

## Usage Examples

### Example 1: Simple Aggregation Query

**User Query:** "Show me total sales by region"

**Generated SQL:**
```sql
SELECT region, SUM(amount) as total_sales
FROM sales
GROUP BY region
ORDER BY total_sales DESC
```

**Agent Response:**
> Based on the sales data, here are the total sales by region:
> - North: $1,234,567
> - South: $987,654
> - East: $876,543
> - West: $765,432
> - Central: $654,321

### Example 2: Filtered Query with Time Range

**User Query:** "What were our top 5 products last month?"

**Generated SQL:**
```sql
SELECT p.name, SUM(s.amount) as revenue
FROM sales s
JOIN products p ON s.product_id = p.product_id
WHERE s.date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 1 MONTH)
  AND s.date < DATE_TRUNC('month', CURRENT_DATE)
GROUP BY p.name
ORDER BY revenue DESC
LIMIT 5
```

### Example 3: Context-Enhanced Query

**User Query:** "What is customer lifetime value?"

**Agent Response:**
> Customer Lifetime Value (CLV) is calculated as the average revenue per customer over their entire relationship with the company. Based on our KPI definitions, CLV = Average Revenue per Customer × Average Customer Lifespan.
>
> From the current data:
> - Average order value: $156
> - Average orders per customer: 3.2
> - Estimated CLV: $499

### Example 4: Hybrid Query (SQL + Context)

**User Query:** "Analyze our Q4 sales performance"

**Action:** The agent retrieves business context about Q4 seasonality and runs SQL to get actual Q4 numbers, then combines both for a comprehensive answer.

---

## Architecture Overview

### Component Structure

```
databricks-insight-agent/
├── app.py                      # Streamlit web UI
├── main.py                     # CLI entry point
├── agent.py                    # Main orchestrator
├── databricks_client.py        # Databricks connectivity
├── sql_generator.py            # Schema-aware SQL generation
├── sql_error_correction.py     # Auto-correction of SQL errors
├── context_retriever.py        # FAISS-based RAG
├── document_processor.py       # Document chunking
├── data_pipeline.py            # Delta table pipeline
├── dbfs_integration.py         # DBFS storage
├── security.py                 # Security validation
├── requirements.txt            # Dependencies
└── data/
    ├── csv/                    # Input CSV files
    ├── documents/              # Knowledge base docs
    ├── cache/                  # Local FAISS cache
    └── faiss_index.faiss       # Vector index
```

### Data Flow

```
User Query
    ↓
Security Validation
    ↓
Intent Analysis
    ↓
┌─────────────────┐         ┌──────────────────┐
│  SQL Generator  │         │ Context Retriever│
│  (Delta Tables) │         │  (FAISS + DBFS)  │
└────────┬────────┘         └────────┬─────────┘
         │                           │
         ↓                           ↓
    Databricks SQL            Retrieved Context
    (Auto-Retry)
         │                           │
         └───────────┬───────────────┘
                     ↓
            Insight Generation
                     ↓
              User Response
```

---

## Development Workflow

### Git Branching Strategy

```bash
# Main branches
main          # Production-ready code
dev           # Development integration

# Feature branches
feature/*     # New features
bugfix/*      # Bug fixes
hotfix/*      # Urgent production fixes
```

### Making Changes

#### 1. Create Feature Branch

```bash
git checkout dev
git pull origin dev
git checkout -b feature/add-new-metric
```

#### 2. Make Changes and Commit

```bash
# Make your changes
git add .
git commit -m "Add new metric calculation for customer retention"
```

#### 3. Push and Create Pull Request

```bash
git push origin feature/add-new-metric
```

Then create a PR to merge into `dev`.

#### 4. Merge to Main (After Testing)

```bash
git checkout dev
git pull origin dev
git checkout main
git pull origin main
git merge dev
git push origin main
git tag -a v1.0.1 -m "Release version 1.0.1"
git push origin v1.0.1
```

### Testing

```bash
# Run unit tests
pytest test_core.py -v

# Run integration tests
pytest tests/ -v --integration

# Run with coverage
pytest --cov=. --cov-report=html
```

### Code Quality

```bash
# Format code
black *.py

# Check linting
flake8 *.py

# Type checking
mypy *.py
```

---

## Common Troubleshooting

### Issue: "Module not found" errors
**Solution:** Ensure virtual environment is activated and dependencies installed:
```bash
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### Issue: Databricks connection fails
**Solution:** 
1. Verify credentials in `.env` file
2. Check SQL Warehouse is running
3. Test connection: `python -c "from databricks import sql; print('OK')"`

### Issue: FAISS index errors
**Solution:** 
1. Delete existing index: `rm data/faiss_index.faiss`
2. Restart application to rebuild index

### Issue: SQL generation errors
**Solution:** The agent includes auto-retry logic. Check logs for correction attempts:
```bash
tail -f logs/agent.log
```

---

## Additional Resources

- **Databricks Documentation:** https://docs.databricks.com/
- **FAISS Documentation:** https://github.com/facebookresearch/faiss
- **Streamlit Documentation:** https://docs.streamlit.io/
- **Project Repository:** https://github.com/HasnainShafiq98/databricks-insight-agent

---

## Support

For issues, questions, or contributions:
- Open an issue on GitHub
- Email: support@databricks-insight-agent.com
- Slack: #databricks-agent

---

**Last Updated:** January 11, 2026
**Version:** 2.0.0
