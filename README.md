# Databricks Insight Agent

An AI-powered analytics assistant that connects to Databricks lakehouse, understands natural language queries, generates safe SQL, and provides business-focused insights using FAISS-based context retrieval.

## Features

- 🧠 **Intelligent Query Understanding**: Analyzes user queries to identify intent, filters, and required data
- 🔒 **Security First**: Built-in protection against SQL injection, input validation, and rate limiting
- 🎯 **Schema-Aware SQL Generation**: Generates SQL only from known schema - no hallucinated columns
- 🔍 **Context Retrieval**: Uses FAISS vector search for semantic document retrieval
- 📊 **Business Insights**: Combines SQL results with retrieved context for clear, actionable insights
- ⚡ **Smart Decision Making**: Automatically decides whether to run SQL, retrieve context, or both
- 🛡️ **Protection Rules**: Comprehensive security and validation at multiple levels

## Architecture

```
User Query
    ↓
Security Validation (Input validation, Rate limiting)
    ↓
Query Analysis (Intent detection, Filter identification)
    ↓
Decision Logic (SQL / Context / Both / Clarification)
    ↓
┌─────────────────┐         ┌──────────────────┐
│  SQL Generator  │         │ Context Retriever│
│  (Schema-based) │         │  (FAISS Search)  │
└────────┬────────┘         └────────┬─────────┘
         │                           │
         ↓                           ↓
    Databricks            Retrieved Documents
    Query Results
         │                           │
         └───────────┬───────────────┘
                     ↓
            Insight Generation
                     ↓
            User Response
```

## Installation

1. **Clone the repository**:
```bash
git clone https://github.com/HasnainShafiq98/databricks-insight-agent.git
cd databricks-insight-agent
```

2. **Create a virtual environment**:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

4. **Configure environment variables**:
```bash
cp .env.example .env
# Edit .env with your Databricks credentials
```

## Configuration

Edit the `.env` file with your settings:

```ini
# Databricks Configuration
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_ACCESS_TOKEN=your-access-token

# OpenAI Configuration (optional, for enhanced query understanding)
OPENAI_API_KEY=your-openai-api-key

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

## Usage

### Command Line Interface

Run the interactive CLI:

```bash
python main.py
```

Example interactions:

```
You: Show me total sales by region
✅ Generated SQL: SELECT region, SUM(amount) as total FROM sales GROUP BY region
📊 Results: 3 records
💡 Insights: Found regional sales distribution...

You: What tables are available?
📋 Schema: sales, customers, products

You: Explain the sales table
💡 Context: The sales table contains transaction data with columns...
```

### Python API

```python
from main import initialize_agent, load_configuration

# Initialize
config = load_configuration()
agent = initialize_agent(config)

# Process query
response = agent.process_query("Show me top 10 customers by revenue")

# Access results
if response.success:
    print(f"SQL: {response.sql_query}")
    print(f"Results: {response.results}")
    print(f"Insights: {response.insights}")
```

## Security Features

### 1. Input Validation
- Query length limits
- Empty query detection
- SQL injection pattern detection
- Dangerous keyword blocking

### 2. SQL Generation Safety
- Only references known schema
- No column hallucination
- Parameterized value handling
- SELECT-only queries

### 3. SQL Validation
- Syntax validation using sqlparse
- Schema whitelist enforcement
- Query type verification

### 4. Rate Limiting
- Per-user rate limits
- Configurable limits
- In-memory tracking

### 5. Access Control
- Schema-level restrictions
- Token-based authentication
- Audit logging

## Project Structure

```
databricks-insight-agent/
├── main.py                  # Application entry point
├── agent.py                 # Main agent orchestration
├── sql_generator.py         # SQL generation from schema
├── databricks_client.py     # Databricks connectivity
├── context_retriever.py     # FAISS-based context retrieval
├── security.py              # Security and validation
├── requirements.txt         # Python dependencies
├── .env.example            # Configuration template
├── .gitignore              # Git ignore rules
└── README.md               # This file
```

## Components

### DatabricksInsightAgent
Main orchestrator that:
- Validates user input
- Analyzes query intent
- Decides execution strategy
- Coordinates SQL and context retrieval
- Generates business insights

### SQLGenerator
Schema-aware SQL generation:
- Validates all column references
- Prevents SQL injection
- Handles filters, aggregations, ordering
- Ensures type safety

### ContextRetriever
FAISS-based semantic search:
- Indexes documentation and schema info
- Retrieves relevant context
- Supports incremental updates
- Persists index to disk

### SecurityValidator
Multi-layer protection:
- Input sanitization
- SQL injection prevention
- Pattern matching for dangerous queries
- Rate limiting enforcement

### DatabricksClient
Connection management:
- Secure token authentication
- Query execution
- Schema introspection
- Connection pooling

## Example Queries

The agent can handle various query types:

**Data Retrieval**:
- "Show me sales data for last month"
- "Get top 10 products by revenue"
- "List all customers from USA"

**Aggregation**:
- "What's the total sales by region?"
- "Calculate average order value"
- "Count transactions per day"

**Context/Documentation**:
- "Explain the sales table"
- "What columns are in customers table?"
- "How to join sales and products?"

**Mixed Queries**:
- "Show revenue by region and explain the trend"
- "Get customer data and tell me about the schema"

## Development

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=. --cov-report=html
```

### Adding New Tables

Update the schema manager in `main.py`:

```python
new_table = TableSchema(
    name="your_table",
    columns=["col1", "col2", "col3"],
    column_types={"col1": "STRING", "col2": "INT", "col3": "DATE"},
    description="Table description"
)
schema_manager.add_table(new_table)
```

### Adding Documents to FAISS

```python
from context_retriever import Document

docs = [
    Document(
        content="Your documentation content",
        metadata={"table": "table_name", "type": "schema"}
    )
]
context_retriever.add_documents(docs)
context_retriever.save_index()
```

## Security Best Practices

1. **Never commit credentials**: Use environment variables
2. **Rotate tokens regularly**: Update Databricks access tokens
3. **Monitor rate limits**: Adjust based on usage patterns
4. **Audit logs**: Review query patterns for anomalies
5. **Schema restrictions**: Only expose necessary tables
6. **Input validation**: Always enabled in production

## Troubleshooting

### Connection Issues
```bash
# Test Databricks connection
python -c "from databricks_client import DatabricksClient; \
client = DatabricksClient('hostname', 'path', 'token'); \
print(client.test_connection())"
```

### FAISS Index Issues
```bash
# Clear and rebuild index
rm -f data/faiss_index.faiss*
python main.py
# Index will be recreated with sample documents
```

### Rate Limit Errors
Adjust in `.env`:
```ini
RATE_LIMIT_PER_MINUTE=120
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details

## Support

For issues and questions:
- Open an issue on GitHub
- Contact: [Your Contact Information]

## Roadmap

- [ ] LLM integration for better query understanding
- [ ] Multi-table join support
- [ ] Query history and learning
- [ ] REST API interface
- [ ] Web UI dashboard
- [ ] Advanced analytics (forecasting, anomaly detection)
- [ ] Multi-language support
- [ ] Enhanced visualization