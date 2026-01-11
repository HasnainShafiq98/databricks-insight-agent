# Architecture Documentation

## System Overview

The Databricks Insight Agent is an AI-powered analytics assistant that provides natural language access to Databricks lakehouse data with built-in security and intelligent query understanding.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          User Interface                          │
│                     (CLI / API / Future: Web)                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DatabricksInsightAgent                        │
│                   (Main Orchestrator)                            │
│                                                                  │
│  • Query Analysis        • Decision Logic                       │
│  • Component Coordination • Response Generation                 │
└─┬───────────────────┬───────────────────┬──────────────────────┘
  │                   │                   │
  ▼                   ▼                   ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Security    │  │     SQL      │  │   Context    │
│  Validator   │  │  Generator   │  │  Retriever   │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                  │
       ▼                 ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Rate Limiter │  │   Schema     │  │    FAISS     │
│              │  │   Manager    │  │    Index     │
└──────────────┘  └──────────────┘  └──────────────┘
                         │
                         ▼
                  ┌──────────────┐
                  │ Databricks   │
                  │   Client     │
                  └──────┬───────┘
                         │
                         ▼
                  ┌──────────────┐
                  │ Databricks   │
                  │  Lakehouse   │
                  └──────────────┘
```

## Component Details

### 1. DatabricksInsightAgent (agent.py)

**Purpose**: Main orchestrator that coordinates all components

**Key Responsibilities**:
- Receive and validate user queries
- Analyze query intent
- Decide execution strategy (SQL, context, or both)
- Coordinate SQL generation and context retrieval
- Generate business insights from results
- Handle errors and request clarifications

**Key Methods**:
```python
process_query(user_query: str) -> AgentResponse
analyze_query(user_query: str) -> QueryAnalysis
_generate_safe_sql(...) -> Optional[str]
_generate_insights(...) -> str
```

**Data Flow**:
1. Receives user query
2. Security validation
3. Rate limit check
4. Query analysis
5. Decision: SQL / Context / Both / Clarification
6. Execute chosen path(s)
7. Generate insights
8. Return response

### 2. SecurityValidator (security.py)

**Purpose**: Multi-layer security protection

**Components**:
- `SecurityConfig`: Configuration for security rules
- `SecurityValidator`: Input validation and SQL injection prevention
- `SchemaValidator`: Schema-based validation
- `RateLimiter`: API rate limiting

**Protection Layers**:
1. Input sanitization
2. Query length limits
3. SQL injection pattern detection
4. Dangerous keyword blocking
5. SQL syntax validation
6. Schema whitelist enforcement
7. Rate limiting

**Example Usage**:
```python
validator = SecurityValidator(config)
is_valid, error = validator.validate_query(user_input)
sql_valid, error = validator.validate_sql(generated_sql)
```

### 3. SQLGenerator (sql_generator.py)

**Purpose**: Generate safe SQL from schema and user intent

**Components**:
- `TableSchema`: Represents table structure
- `SchemaManager`: Manages database schema
- `SQLGenerator`: Generates SQL from parameters

**Key Features**:
- Schema-aware generation
- No column hallucination
- Type-safe queries
- Parameterized filters
- Aggregation support
- Order and limit clauses

**Example Usage**:
```python
sql = generator.generate_sql(
    table_name="sales",
    columns=["region", "amount"],
    filters={"date": "2024-01-01"},
    aggregations={"amount": "SUM"},
    group_by=["region"],
    limit=10
)
```

### 4. ContextRetriever (context_retriever.py)

**Purpose**: Semantic search for relevant documentation

**Components**:
- `Document`: Represents a knowledge base document
- `ContextRetriever`: FAISS-based semantic search

**Key Features**:
- Vector embeddings using Sentence Transformers
- FAISS indexing for fast search
- Persistent storage
- Incremental updates
- Metadata support

**Example Usage**:
```python
retriever = ContextRetriever(index_path="./faiss_index.faiss")
retriever.add_documents(documents)
context = retriever.get_context(query, top_k=3)
```

### 5. DatabricksClient (databricks_client.py)

**Purpose**: Manage Databricks connections and queries

**Key Features**:
- Connection pooling
- Query execution
- Schema introspection
- Error handling
- Context manager support

**Example Usage**:
```python
with DatabricksClient(hostname, path, token) as client:
    results = client.execute_query(sql)
    schema = client.get_table_schema("sales")
```

## Query Processing Pipeline

### Step-by-Step Flow

```
1. User Input
   "Show me top 10 customers by revenue"
   │
   ▼
2. Security Validation
   ✓ Length check
   ✓ Dangerous keywords
   ✓ SQL injection patterns
   │
   ▼
3. Rate Limiting
   ✓ Check user quota
   ✓ Update call counter
   │
   ▼
4. Query Analysis
   • Type: SQL_ONLY
   • Tables: customers, sales
   • Intent: aggregation, ordering
   • Filters: none
   │
   ▼
5. Context Retrieval (if needed)
   • Search: "customer revenue"
   • Results: relevant docs
   │
   ▼
6. SQL Generation
   • Validate tables exist
   • Validate columns exist
   • Generate: SELECT customer_id, 
              SUM(amount) as revenue
              FROM sales
              GROUP BY customer_id
              ORDER BY revenue DESC
              LIMIT 10
   │
   ▼
7. SQL Validation
   ✓ Syntax valid
   ✓ SELECT only
   ✓ Schema allowed
   │
   ▼
8. Execute Query
   • Connect to Databricks
   • Run SQL
   • Fetch results
   │
   ▼
9. Generate Insights
   • Combine results + context
   • Statistical analysis
   • Business insights
   │
   ▼
10. Return Response
    ✓ Success
    📊 SQL query
    📈 Results (10 rows)
    💡 Insights
```

## Data Models

### QueryAnalysis
```python
@dataclass
class QueryAnalysis:
    query_type: QueryType              # SQL, CONTEXT, BOTH, CLARIFICATION
    needs_filters: bool                # Whether filters are needed
    missing_information: List[str]     # What info is missing
    confidence: float                  # Confidence score
    identified_filters: Dict[str, Any] # Extracted filters
    target_tables: List[str]           # Tables to query
```

### AgentResponse
```python
@dataclass
class AgentResponse:
    success: bool                      # Whether query succeeded
    query_type: QueryType              # Type of query processed
    sql_query: Optional[str]           # Generated SQL (if any)
    results: Optional[List[Dict]]      # Query results (if any)
    context: Optional[str]             # Retrieved context (if any)
    insights: str                      # Generated insights
    clarification_needed: Optional[str] # Clarification message (if any)
    error: Optional[str]               # Error message (if any)
```

### TableSchema
```python
@dataclass
class TableSchema:
    name: str                          # Table name
    columns: List[str]                 # Column names
    column_types: Dict[str, str]       # Column -> type mapping
    description: Optional[str]         # Table description
```

## Decision Logic

The agent uses the following logic to decide execution strategy:

```python
def decide_strategy(query: str) -> QueryType:
    """
    SQL_ONLY:
      - Data retrieval keywords (show, get, find, list, count, sum, etc.)
      - No explanation needed
      
    CONTEXT_ONLY:
      - Explanation keywords (explain, what is, how to, describe, etc.)
      - No data needed
      
    BOTH:
      - Data + explanation needed
      - Example: "Show sales and explain the trend"
      
    CLARIFICATION:
      - Missing information (table name, filters, etc.)
      - Ambiguous query
    """
```

## Error Handling

### Error Categories

1. **Security Errors**
   - SQL injection detected → Block + log
   - Rate limit exceeded → Throttle + inform
   - Invalid input → Reject + explain

2. **Schema Errors**
   - Unknown table → Request clarification
   - Unknown column → Reject + suggest alternatives
   - Type mismatch → Correct or reject

3. **Execution Errors**
   - Connection failure → Retry + inform
   - Query timeout → Optimize or partition
   - Permission denied → Check access + inform

4. **User Errors**
   - Ambiguous query → Request clarification
   - Missing information → Ask for details
   - Invalid format → Provide example

### Error Response Format

```python
AgentResponse(
    success=False,
    error="Clear error message",
    clarification_needed="What we need from user"
)
```

## Configuration

### Environment Variables

```ini
# Databricks
DATABRICKS_SERVER_HOSTNAME=workspace.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/id
DATABRICKS_ACCESS_TOKEN=token

# Security
MAX_QUERY_LENGTH=10000
RATE_LIMIT_PER_MINUTE=60
ALLOWED_SCHEMAS=default,analytics
ENABLE_SQL_INJECTION_PROTECTION=true

# Context Retrieval
FAISS_INDEX_PATH=./data/faiss_index.faiss
DOCUMENTS_PATH=./data/documents

# Logging
LOG_LEVEL=INFO
```

## Scalability Considerations

### Current Design
- In-memory rate limiting
- Single-process architecture
- Local FAISS index

### Future Enhancements

1. **Distributed Rate Limiting**
   - Redis for shared state
   - Multi-instance support

2. **Database Schema Caching**
   - Cache schema in Redis
   - Periodic refresh
   - Invalidation on changes

3. **Horizontal Scaling**
   - Stateless design
   - Load balancer
   - Shared FAISS index

4. **Query Optimization**
   - Query result caching
   - Smart pre-fetching
   - Index optimization

## Testing Strategy

### Unit Tests
- Each component tested independently
- Mock external dependencies
- Test security rules thoroughly

### Integration Tests
- End-to-end query processing
- Real Databricks connection (optional)
- FAISS integration

### Security Tests
- SQL injection attempts
- Rate limiting
- Schema validation
- Input sanitization

### Performance Tests
- Query response time
- FAISS search speed
- Connection pooling
- Memory usage

## Monitoring and Observability

### Key Metrics

1. **Performance**
   - Query processing time
   - FAISS search latency
   - Databricks query time
   - End-to-end latency

2. **Usage**
   - Queries per minute
   - Users active
   - Most queried tables
   - Common query patterns

3. **Security**
   - Injection attempts
   - Rate limit hits
   - Failed validations
   - Schema violations

4. **Reliability**
   - Success rate
   - Error types
   - Connection failures
   - Timeout rate

### Logging

```python
# Structured logging format
{
    "timestamp": "2024-01-01T12:00:00Z",
    "level": "INFO",
    "component": "agent",
    "user_id": "user123",
    "query": "Show sales",
    "query_type": "SQL_ONLY",
    "execution_time_ms": 150,
    "success": true
}
```

## Deployment

### Prerequisites
- Python 3.8+
- Databricks workspace
- Access token with read permissions
- Virtual environment

### Deployment Steps

1. Clone repository
2. Install dependencies
3. Configure `.env` file
4. Initialize FAISS index
5. Test connection
6. Start application

### Production Checklist

- [ ] Environment variables configured
- [ ] Security settings enabled
- [ ] Logging configured
- [ ] Token rotation plan
- [ ] Monitoring set up
- [ ] Backup strategy
- [ ] Incident response plan
- [ ] Documentation complete
- [ ] Security audit passed
- [ ] Performance tested

## Future Enhancements

### Short Term
- [ ] LLM integration for better query understanding
- [ ] Multi-table join support
- [ ] Query history tracking
- [ ] Enhanced visualizations

### Medium Term
- [ ] REST API
- [ ] Web UI
- [ ] Advanced analytics
- [ ] Query templates
- [ ] User preferences

### Long Term
- [ ] Multi-language support
- [ ] Forecasting and ML
- [ ] Anomaly detection
- [ ] Natural language reporting
- [ ] Slack/Teams integration

## References

- [Databricks SQL API](https://docs.databricks.com/sql/api/index.html)
- [FAISS Documentation](https://github.com/facebookresearch/faiss)
- [Sentence Transformers](https://www.sbert.net/)
- [OWASP Security Guidelines](https://owasp.org/)
