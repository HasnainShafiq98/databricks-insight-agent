# 🔍 Databricks Insight Agent

> AI-powered analytics assistant for Databricks lakehouse with RAG-based context retrieval and intelligent SQL generation.

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Streamlit](https://img.shields.io/badge/streamlit-1.28+-red.svg)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## ✨ Features

🧠 **Intelligent Query Understanding** - Analyzes natural language to extract intent, filters, and metrics  
🔒 **Security First** - Multi-layer SQL injection protection and input validation  
🎯 **Schema-Aware SQL** - Generates queries only from known schema (no hallucinations)  
🔍 **RAG System** - FAISS-based semantic document retrieval for contextual insights  
📊 **Delta Lake Integration** - Bronze/Silver/Gold medallion architecture  
⚡ **Auto-Correction** - Detects and fixes SQL errors automatically  
🛡️ **Production Ready** - Error handling, rate limiting, and comprehensive logging

---

## 🚀 Quick Start

### 1. Setup
```bash
# Clone repository
git clone https://github.com/HasnainShafiq98/databricks-insight-agent.git
cd databricks-insight-agent

# Run automated setup
./scripts/quickstart.sh
```

### 2. Configure
```bash
# Copy environment template
cp config/.env.example .env

# Edit with your credentials
nano .env
```

### 3. Launch
```bash
# Streamlit UI (recommended)
streamlit run src/ui/app.py

# Or CLI
python src/ui/main.py

# Or run examples
python scripts/example_usage.py
```

---

## 📁 Project Structure

```
databricks-insight-agent/
├── src/                          # Source code
│   ├── core/                     # Core application logic
│   │   ├── agent.py              # Main orchestrator
│   │   └── __init__.py
│   │
│   ├── data/                     # Data layer
│   │   ├── databricks_client.py  # SQL connector
│   │   ├── data_pipeline.py      # Delta table pipeline
│   │   ├── dbfs_integration.py   # DBFS storage
│   │   └── __init__.py
│   │
│   ├── intelligence/             # AI/ML components
│   │   ├── sql_generator.py      # SQL generation
│   │   ├── sql_error_correction.py # Auto-correction
│   │   ├── context_retriever.py  # RAG system
│   │   ├── document_processor.py # Document chunking
│   │   └── __init__.py
│   │
│   ├── security/                 # Security layer
│   │   ├── security.py           # Validation & rate limiting
│   │   └── __init__.py
│   │
│   └── ui/                       # User interfaces
│       ├── app.py                # Streamlit web UI
│       ├── main.py               # CLI interface
│       └── __init__.py
│
├── tests/                        # Test suite
│   ├── test_core.py              # Core tests
│   ├── examples.py               # Example queries
│   └── __init__.py
│
├── scripts/                      # Utility scripts
│   ├── quickstart.sh             # Setup automation
│   └── example_usage.py          # Complete demos
│
├── docs/                         # Documentation
│   ├── README.md                 # This file
│   ├── SETUP_GUIDE.md            # Setup instructions
│   ├── DEVELOPMENT.md            # Development guide
│   ├── GIT_WORKFLOW.md           # Git workflow
│   ├── PROJECT_SUMMARY.md        # Implementation details
│   ├── ARCHITECTURE.md           # System architecture
│   └── SECURITY.md               # Security guidelines
│
├── config/                       # Configuration
│   └── .env.example              # Environment template
│
├── data/                         # Data storage
│   ├── csv/                      # Input CSV files
│   ├── documents/                # Knowledge base
│   ├── cache/                    # Local cache
│   └── logs/                     # Application logs
│
├── requirements.txt              # Python dependencies
├── .gitignore                    # Git ignore rules
└── .env                          # Local config (not in git)
```

---

## 🎯 Architecture

```
User Query
    ↓
Security Validation
    ↓
Intent Analysis
    ↓
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  SQL Mode    │    │ Context Mode │    │  Hybrid Mode │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                    │
       ▼                   ▼                    ▼
  SQL Generator      FAISS Search          Both Systems
       │                   │                    │
       ▼                   │                    │
  Databricks              │                    │
  Execution               │                    │
       │                   │                    │
       └───────────────────┴────────────────────┘
                           │
                           ▼
                  Insight Generation
                           │
                           ▼
                    User Response
```

---

## 💡 Usage Examples

### Basic Query
```python
"Show me total sales by region"
→ Generates SQL: SELECT region, SUM(amount) FROM sales GROUP BY region
→ Executes on Databricks
→ Returns insights
```

### Context Query
```python
"What is Customer Lifetime Value?"
→ Retrieves from knowledge base
→ Returns definition and formula
```

### Hybrid Query
```python
"Analyze our Q4 sales performance"
→ Runs SQL for Q4 data
→ Retrieves seasonality context
→ Combines both for comprehensive answer
```

---

## 🔐 Security Features

✅ **SQL Injection Prevention** - Multi-layer validation and sanitization  
✅ **Schema Enforcement** - Only allows queries on known tables/columns  
✅ **Rate Limiting** - Prevents abuse with configurable limits  
✅ **Input Validation** - Strict validation of all user inputs  
✅ **Query Length Limits** - Prevents resource exhaustion  
✅ **Audit Logging** - Comprehensive logging of all operations

---

## 📊 Data Pipeline

### Medallion Architecture

**Bronze Layer** (Raw Data)
- Ingests CSV files as-is
- Adds ingestion metadata
- Minimal validation

**Silver Layer** (Cleaned Data)
- Data quality checks
- Type normalization
- Duplicate removal
- Schema validation

**Gold Layer** (Business Metrics)
- Aggregated KPIs
- Business-ready analytics
- Optimized for queries

---

## 🛠️ Configuration

### Environment Variables

Key settings in `.env`:

```ini
# Databricks (Required)
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxx
DATABRICKS_ACCESS_TOKEN=dapi...

# Optional
OPENAI_API_KEY=sk-...                  # Enhanced query understanding
FAISS_INDEX_PATH=./data/faiss_index.faiss
MAX_QUERY_LENGTH=10000
RATE_LIMIT_PER_MINUTE=60
```

See [config/.env.example](config/.env.example) for all options.

---

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run examples
python scripts/example_usage.py
```

---

## 📚 Documentation

- **[Setup Guide](docs/SETUP_GUIDE.md)** - Complete installation and configuration
- **[Development Guide](docs/DEVELOPMENT.md)** - Architecture and dev workflow
- **[Git Workflow](docs/GIT_WORKFLOW.md)** - Version control best practices
- **[Project Summary](docs/PROJECT_SUMMARY.md)** - Implementation details
- **[Architecture](docs/ARCHITECTURE.md)** - System design
- **[Security](docs/SECURITY.md)** - Security guidelines

---

## 🤝 Contributing

We follow a structured Git workflow:

1. Create feature branch: `git checkout -b feature/your-feature`
2. Make changes with descriptive commits
3. Create pull request to `dev` branch
4. After review, merge to `dev`
5. Periodically release from `dev` to `main`

See [Git Workflow Guide](docs/GIT_WORKFLOW.md) for details.

---

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 🎓 Credits

Built with:
- [Databricks](https://databricks.com/) - Data lakehouse platform
- [FAISS](https://github.com/facebookresearch/faiss) - Vector search
- [Streamlit](https://streamlit.io/) - Web UI framework
- [Sentence Transformers](https://www.sbert.net/) - Embeddings
- [Delta Lake](https://delta.io/) - Storage layer

---

## 📞 Support

- **Documentation:** See [docs/](docs/) directory
- **Issues:** Open an issue on GitHub
- **Questions:** Check [SETUP_GUIDE.md](docs/SETUP_GUIDE.md)

---

## 🗺️ Roadmap

- [ ] Multi-table join support
- [ ] Advanced visualizations
- [ ] Query history and favorites
- [ ] Real-time data streaming
- [ ] Advanced ML query understanding
- [ ] Export to multiple formats

---

**Version:** 2.0.0  
**Status:** ✅ Production Ready  
**Last Updated:** January 11, 2026

---

Made with ❤️ by the Databricks Insight Agent Team
