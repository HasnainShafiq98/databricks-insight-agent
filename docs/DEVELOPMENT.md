# Development Guide

## 📚 Table of Contents
1. [Project Structure](#project-structure)
2. [Architecture Deep Dive](#architecture-deep-dive)
3. [Component Details](#component-details)
4. [Development Workflow](#development-workflow)
5. [Testing Strategy](#testing-strategy)
6. [Deployment](#deployment)
7. [Best Practices](#best-practices)

---

## Project Structure

```
databricks-insight-agent/
│
├── Core Application Files
│   ├── app.py                          # Streamlit web UI
│   ├── main.py                         # CLI entry point
│   ├── agent.py                        # Main orchestrator
│   ├── examples.py                     # Example queries
│   └── example_usage.py                # Complete demo script
│
├── Data Layer
│   ├── databricks_client.py            # Databricks SQL connector
│   ├── data_pipeline.py                # Bronze/Silver/Gold pipeline
│   └── dbfs_integration.py             # DBFS storage management
│
├── Intelligence Layer
│   ├── sql_generator.py                # Schema-aware SQL generation
│   ├── sql_error_correction.py         # Auto-correction & retry
│   ├── context_retriever.py            # FAISS-based RAG
│   └── document_processor.py           # Document chunking
│
├── Security Layer
│   └── security.py                     # Validation & rate limiting
│
├── Testing & Documentation
│   ├── test_core.py                    # Unit tests
│   ├── README.md                       # Project overview
│   ├── ARCHITECTURE.md                 # Architecture details
│   ├── SECURITY.md                     # Security documentation
│   ├── SETUP_GUIDE.md                  # Complete setup guide
│   └── DEVELOPMENT.md                  # This file
│
├── Configuration
│   ├── requirements.txt                # Python dependencies
│   ├── .env.example                    # Environment template
│   ├── .gitignore                      # Git ignore rules
│   └── .env                            # Local config (not in git)
│
└── Data Directory
    ├── data/
    │   ├── csv/                        # Input CSV files
    │   ├── documents/                  # Knowledge base documents
    │   ├── cache/                      # Local FAISS cache
    │   └── faiss_index.faiss           # Vector index
    └── logs/                           # Application logs
```

---

## Architecture Deep Dive

### 1. Query Processing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        User Input Layer                          │
│                  (Streamlit UI / CLI / API)                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Security Gateway                            │
│  • Input validation      • Rate limiting                        │
│  • SQL injection check   • Schema validation                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Query Analyzer (Agent)                        │
│  • Intent detection      • Filter identification                │
│  • Confidence scoring    • Missing info detection               │
└─────┬──────────────────────┬────────────────────┬───────────────┘
      │                      │                    │
      ▼                      ▼                    ▼
┌──────────┐          ┌──────────┐         ┌──────────┐
│   SQL    │          │ Context  │         │  Hybrid  │
│   Only   │          │   Only   │         │   Mode   │
└────┬─────┘          └────┬─────┘         └────┬─────┘
     │                     │                     │
     ▼                     ▼                     ▼
┌──────────────────┐  ┌──────────────────┐  ┌─────┐
│ SQL Generator    │  │ FAISS Retriever  │  │Both │
│ • Schema check   │  │ • Embed query    │  └──┬──┘
│ • Build query    │  │ • Vector search  │     │
│ • Validate       │  │ • Rank results   │     │
└────┬─────────────┘  └────┬─────────────┘     │
     │                     │                    │
     ▼                     │                    │
┌──────────────────┐       │                    │
│ Error Corrector  │       │                    │
│ • Detect error   │       │                    │
│ • Apply fix      │       │                    │
│ • Retry query    │       │                    │
└────┬─────────────┘       │                    │
     │                     │                    │
     ▼                     ▼                    ▼
┌─────────────────────────────────────────────────┐
│             Databricks Query Executor            │
│  • Execute SQL    • Fetch results               │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────┐
│            Insight Generator                     │
│  • Combine SQL + Context                        │
│  • Generate narrative                           │
│  • Format response                              │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────┐
│                User Response                     │
│  • Insights      • SQL query                    │
│  • Results       • Context                      │
└─────────────────────────────────────────────────┘
```

### 2. Delta Table Pipeline

```
CSV Files (Local/DBFS)
         │
         ▼
┌─────────────────────────────────────────┐
│         BRONZE LAYER                     │
│  • Raw data ingestion                   │
│  • Minimal validation                   │
│  • Add metadata (_ingestion_timestamp)  │
│  • Schema: Inferred from CSV            │
└─────────────────┬───────────────────────┘
                  │
                  ▼
         Data Cleaning & Validation
                  │
                  ▼
┌─────────────────────────────────────────┐
│         SILVER LAYER                     │
│  • Data quality checks                  │
│  • Type normalization                   │
│  • Duplicate removal                    │
│  • Standardized formats                 │
│  • Constraints enforcement              │
└─────────────────┬───────────────────────┘
                  │
                  ▼
         Business Logic & Aggregation
                  │
                  ▼
┌─────────────────────────────────────────┐
│         GOLD LAYER                       │
│  • Business metrics                     │
│  • KPI calculations                     │
│  • Denormalized for performance         │
│  • Ready for analytics                  │
└─────────────────────────────────────────┘
```

### 3. RAG Pipeline

```
Documents (Text/MD files)
         │
         ▼
┌─────────────────────────┐
│   Document Chunker      │
│  • Split by paragraphs  │
│  • Maintain context     │
│  • Add metadata         │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│   Embedding Model       │
│  (all-MiniLM-L6-v2)     │
│  • Convert to vectors   │
│  • 384 dimensions       │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│   FAISS Index           │
│  • Vector storage       │
│  • L2 distance metric   │
│  • Stored in DBFS       │
└──────────┬──────────────┘
           │
           ▼
     Query Processing
           │
           ▼
┌─────────────────────────┐
│   Similarity Search     │
│  • Embed query          │
│  • Find top-k nearest   │
│  • Return with scores   │
└─────────────────────────┘
```

---

## Component Details

### Agent (agent.py)

**Responsibility:** Main orchestration and decision-making

**Key Methods:**
- `process_query()`: Main entry point for query processing
- `analyze_query()`: Determines query intent and requirements
- `_generate_safe_sql()`: Coordinates SQL generation with validation
- `_generate_insights()`: Combines results and context into insights

**Decision Logic:**
```python
if query requires data:
    if query has clear metrics:
        → SQL_ONLY
    elif query asks about definitions:
        → CONTEXT_ONLY
    elif query needs both data and explanation:
        → BOTH
else:
    if missing critical information:
        → CLARIFICATION
```

### SQL Generator (sql_generator.py)

**Responsibility:** Schema-aware SQL generation

**Key Features:**
- Only uses columns that exist in schema
- Builds parameterized queries
- Supports filters, aggregations, joins
- Never hallucinates column names

**Example:**
```python
sql_generator.generate_sql(
    table_name="sales",
    columns=["region", "amount"],
    aggregations={"total": "SUM(amount)"},
    group_by=["region"],
    order_by=[("total", "DESC")],
    limit=10
)
```

### SQL Error Corrector (sql_error_correction.py)

**Responsibility:** Auto-correction of SQL errors

**Correction Strategies:**
1. **Column Name Typos**: Uses Levenshtein distance to find similar columns
2. **Table Name Errors**: Suggests closest matching table
3. **Syntax Errors**: Fixes common patterns (missing commas, quotes)
4. **Type Mismatches**: Adds explicit CAST statements
5. **GROUP BY Issues**: Automatically adds missing columns

**Example:**
```python
# Original (wrong)
SELECT transactoin_id FROM sales

# Auto-corrected
SELECT transaction_id FROM sales
```

### Context Retriever (context_retriever.py)

**Responsibility:** Semantic document retrieval using FAISS

**Features:**
- Embedding generation with SentenceTransformers
- Efficient vector search
- Persistent index storage
- Metadata filtering

**Usage:**
```python
retriever = ContextRetriever()
retriever.add_documents(documents)
results = retriever.search("What is CLV?", top_k=3)
```

### Data Pipeline (data_pipeline.py)

**Responsibility:** ETL from CSV to Delta tables

**Bronze Table Creation:**
```sql
CREATE TABLE bronze_sales
AS SELECT *, 
   current_timestamp() as _ingestion_timestamp,
   'source.csv' as _source_file
FROM csv_data
```

**Silver Table Creation:**
```sql
CREATE TABLE silver_sales
AS SELECT
   TRIM(transaction_id) as transaction_id,
   CAST(amount AS DECIMAL(10,2)) as amount,
   TO_DATE(date, 'yyyy-MM-dd') as date
FROM bronze_sales
WHERE amount > 0 
  AND transaction_id IS NOT NULL
```

**Gold Table Creation:**
```sql
CREATE TABLE gold_sales_by_region
AS SELECT
   region,
   SUM(amount) as total_sales,
   COUNT(*) as transaction_count,
   AVG(amount) as avg_transaction
FROM silver_sales
GROUP BY region
```

---

## Development Workflow

### Setting Up Development Environment

```bash
# 1. Clone and setup
git clone https://github.com/HasnainShafiq98/databricks-insight-agent.git
cd databricks-insight-agent
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your credentials

# 3. Create development branch
git checkout -b dev
git push -u origin dev

# 4. Create feature branch
git checkout -b feature/my-new-feature
```

### Git Workflow

#### Branch Structure
```
main            # Production releases only
  └── dev       # Integration branch
       ├── feature/add-metric
       ├── feature/new-datasource
       ├── bugfix/sql-error
       └── hotfix/security-patch
```

#### Making Changes

```bash
# 1. Update from dev
git checkout dev
git pull origin dev

# 2. Create feature branch
git checkout -b feature/add-customer-churn

# 3. Make changes and commit frequently
git add .
git commit -m "feat: Add customer churn prediction

- Implement churn calculation logic
- Add unit tests
- Update documentation"

# 4. Push to remote
git push origin feature/add-customer-churn

# 5. Create Pull Request
# Go to GitHub and create PR: feature/add-customer-churn → dev

# 6. After PR approval, merge to dev
git checkout dev
git pull origin dev

# 7. Periodically merge dev to main for releases
git checkout main
git merge dev
git tag -a v1.2.0 -m "Release v1.2.0: Add customer churn analysis"
git push origin main --tags
```

#### Commit Message Convention

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting)
- `refactor`: Code refactoring
- `test`: Adding tests
- `chore`: Build/tooling changes

**Examples:**
```bash
feat(sql): Add support for window functions

Implement ROW_NUMBER, RANK, and DENSE_RANK functions
in SQL generator with proper partitioning support.

Closes #123
```

```bash
fix(security): Prevent SQL injection in ORDER BY clause

Added validation for ORDER BY column names to prevent
injection through sorting parameters.

Security issue reported by @username
```

---

## Testing Strategy

### Unit Tests

```python
# test_core.py
import pytest
from sql_generator import SQLGenerator, SchemaManager, TableSchema

def test_sql_generation():
    schema_manager = SchemaManager()
    schema_manager.add_table(TableSchema(
        name="sales",
        columns=["id", "amount", "region"],
        column_types={"id": "STRING", "amount": "DECIMAL", "region": "STRING"}
    ))
    
    generator = SQLGenerator(schema_manager)
    sql = generator.generate_sql(
        table_name="sales",
        columns=["region"],
        aggregations={"total": "SUM(amount)"},
        group_by=["region"]
    )
    
    assert "SELECT region, SUM(amount) as total" in sql
    assert "GROUP BY region" in sql
```

### Integration Tests

```python
# tests/test_integration.py
def test_end_to_end_query(agent):
    response = agent.process_query("Show total sales by region")
    
    assert response.success == True
    assert response.sql_query is not None
    assert "SELECT" in response.sql_query
    assert response.insights != ""
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html

# Run specific test
pytest test_core.py::test_sql_generation -v

# Run integration tests only
pytest tests/ -k integration -v
```

---

## Deployment

### Local Development

```bash
# Streamlit UI
streamlit run app.py

# CLI
python main.py

# Run examples
python example_usage.py
```

### Docker Deployment

```dockerfile
# Dockerfile
FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address", "0.0.0.0"]
```

```bash
# Build and run
docker build -t databricks-agent .
docker run -p 8501:8501 --env-file .env databricks-agent
```

### Databricks Deployment

```python
# Upload to DBFS
dbutils.fs.cp("file:/path/to/app", "dbfs:/apps/insight-agent/", recurse=True)

# Create job
{
  "name": "Insight Agent",
  "tasks": [{
    "task_key": "run_agent",
    "python_file": "dbfs:/apps/insight-agent/main.py",
    "cluster_spec": {...}
  }]
}
```

---

## Best Practices

### Code Quality

1. **Use Type Hints**
```python
def generate_sql(
    self,
    table_name: str,
    columns: Optional[List[str]] = None
) -> Optional[str]:
    ...
```

2. **Document Functions**
```python
def process_query(self, user_query: str) -> AgentResponse:
    """
    Process a user query end-to-end.
    
    Args:
        user_query: Natural language query from user
        
    Returns:
        AgentResponse with results and insights
        
    Raises:
        ValueError: If query is empty
        SecurityError: If query violates security rules
    """
```

3. **Error Handling**
```python
try:
    results = self.databricks_client.execute_query(sql)
except ConnectionError as e:
    logger.error(f"Database connection failed: {e}")
    return error_response("Unable to connect to database")
except Exception as e:
    logger.exception("Unexpected error")
    return error_response("An unexpected error occurred")
```

### Performance Optimization

1. **Cache Embeddings**
```python
@lru_cache(maxsize=1000)
def get_embedding(text: str) -> np.ndarray:
    return model.encode(text)
```

2. **Batch Processing**
```python
# Bad: Process one by one
for doc in documents:
    embedding = model.encode(doc)
    
# Good: Batch process
embeddings = model.encode(documents, batch_size=32)
```

3. **Optimize Delta Tables**
```sql
-- Regular maintenance
OPTIMIZE sales_table ZORDER BY (date, region);
VACUUM sales_table RETAIN 168 HOURS;
```

### Security

1. **Never Log Sensitive Data**
```python
# Bad
logger.info(f"User query: {query} with token: {token}")

# Good
logger.info(f"Processing query for user: {user_id}")
```

2. **Validate All Inputs**
```python
if not query or len(query) > MAX_LENGTH:
    raise ValueError("Invalid query length")

if not re.match(r'^[a-zA-Z0-9\s\-_.,?]+$', query):
    raise ValueError("Query contains invalid characters")
```

3. **Use Environment Variables**
```python
# Bad
token = "dapi1234567890"

# Good
token = os.getenv('DATABRICKS_ACCESS_TOKEN')
if not token:
    raise ConfigurationError("Missing access token")
```

---

## Troubleshooting

### Common Issues

**Issue: "FAISS index not loading"**
```bash
# Solution: Rebuild index
rm data/faiss_index.faiss
python -c "from context_retriever import *; rebuild_index()"
```

**Issue: "SQL generation returns None"**
```python
# Check schema is loaded
print(schema_manager.get_all_tables())

# Verify table structure
print(schema_manager.get_table('sales').columns)
```

**Issue: "Rate limit exceeded"**
```python
# Adjust rate limits in .env
RATE_LIMIT_PER_MINUTE=120

# Or clear rate limiter state
rate_limiter.reset()
```

---

**Version:** 2.0.0  
**Last Updated:** January 11, 2026  
**Maintainers:** @HasnainShafiq98
