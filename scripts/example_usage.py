"""
Example script demonstrating end-to-end usage of the Databricks Insight Agent.
Run this after completing setup to verify everything works.
"""

import os
import sys
from dotenv import load_dotenv
import logging

# Import agent components
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data.databricks_client import DatabricksClient
from src.intelligence.sql_generator import SchemaManager, SQLGenerator, TableSchema
from src.intelligence.context_retriever import ContextRetriever, Document
from src.intelligence.document_processor import DocumentChunker, DocumentLoader, create_knowledge_base_documents
from src.security.security import SecurityValidator, SecurityConfig, SchemaValidator, RateLimiter
from src.core.agent import DatabricksInsightAgent
from src.data.data_pipeline import DeltaTablePipeline, CSVIngestionPipeline
from src.data.dbfs_integration import DBFSStorage, FAISSManager
from src.intelligence.sql_error_correction import SQLCorrector, RetryableQueryExecutor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_example_environment():
    """
    Setup a complete example environment with sample data.
    """
    logger.info("=" * 80)
    logger.info("Databricks Insight Agent - Complete Example")
    logger.info("=" * 80)
    
    # Load environment variables
    load_dotenv()
    
    # Configuration
    config = {
        'databricks_hostname': os.getenv('DATABRICKS_SERVER_HOSTNAME'),
        'databricks_http_path': os.getenv('DATABRICKS_HTTP_PATH'),
        'databricks_token': os.getenv('DATABRICKS_ACCESS_TOKEN'),
        'faiss_index_path': os.getenv('FAISS_INDEX_PATH', './data/faiss_index.faiss'),
    }
    
    return config


def demo_1_basic_query(agent):
    """Demo 1: Simple SQL query."""
    logger.info("\n" + "=" * 80)
    logger.info("DEMO 1: Basic Query - Total Sales by Region")
    logger.info("=" * 80)
    
    query = "Show me total sales by region"
    logger.info(f"\n👤 User: {query}")
    
    response = agent.process_query(query, user_id="demo_user")
    
    logger.info(f"\n🤖 Agent Response:")
    logger.info(f"   Success: {response.success}")
    logger.info(f"   Query Type: {response.query_type.value}")
    
    if response.sql_query:
        logger.info(f"\n   Generated SQL:")
        logger.info(f"   {response.sql_query}")
    
    if response.results:
        logger.info(f"\n   Results: {len(response.results)} rows")
        for row in response.results[:3]:
            logger.info(f"   {row}")
    
    logger.info(f"\n   Insights:")
    logger.info(f"   {response.insights}")


def demo_2_filtered_query(agent):
    """Demo 2: Query with filters."""
    logger.info("\n" + "=" * 80)
    logger.info("DEMO 2: Filtered Query - Top Products")
    logger.info("=" * 80)
    
    query = "What are the top 5 products by revenue in the North region?"
    logger.info(f"\n👤 User: {query}")
    
    response = agent.process_query(query, user_id="demo_user")
    
    logger.info(f"\n🤖 Agent Response:")
    logger.info(f"   Success: {response.success}")
    
    if response.sql_query:
        logger.info(f"\n   Generated SQL:")
        logger.info(f"   {response.sql_query}")
    
    logger.info(f"\n   Insights:")
    logger.info(f"   {response.insights}")


def demo_3_context_query(agent):
    """Demo 3: Context-only query (no SQL needed)."""
    logger.info("\n" + "=" * 80)
    logger.info("DEMO 3: Context Query - KPI Definition")
    logger.info("=" * 80)
    
    query = "What is Customer Lifetime Value and how is it calculated?"
    logger.info(f"\n👤 User: {query}")
    
    response = agent.process_query(query, user_id="demo_user")
    
    logger.info(f"\n🤖 Agent Response:")
    logger.info(f"   Success: {response.success}")
    logger.info(f"   Query Type: {response.query_type.value}")
    
    if response.context:
        logger.info(f"\n   Retrieved Context:")
        logger.info(f"   {response.context[:200]}...")
    
    logger.info(f"\n   Insights:")
    logger.info(f"   {response.insights}")


def demo_4_hybrid_query(agent):
    """Demo 4: Hybrid query (SQL + Context)."""
    logger.info("\n" + "=" * 80)
    logger.info("DEMO 4: Hybrid Query - Analysis with Context")
    logger.info("=" * 80)
    
    query = "Analyze our customer acquisition trends and explain the key metrics"
    logger.info(f"\n👤 User: {query}")
    
    response = agent.process_query(query, user_id="demo_user")
    
    logger.info(f"\n🤖 Agent Response:")
    logger.info(f"   Success: {response.success}")
    logger.info(f"   Query Type: {response.query_type.value}")
    
    if response.sql_query:
        logger.info(f"\n   Generated SQL:")
        logger.info(f"   {response.sql_query}")
    
    if response.context:
        logger.info(f"\n   Used Context: Yes")
    
    logger.info(f"\n   Insights:")
    logger.info(f"   {response.insights}")


def demo_5_data_pipeline():
    """Demo 5: Data pipeline (Bronze -> Silver -> Gold)."""
    logger.info("\n" + "=" * 80)
    logger.info("DEMO 5: Data Pipeline - Delta Table Creation")
    logger.info("=" * 80)
    
    logger.info("\nThis demo shows the medallion architecture:")
    logger.info("  Bronze → Raw data ingestion")
    logger.info("  Silver → Cleaned and validated data")
    logger.info("  Gold → Business-level aggregates")
    
    logger.info("\nTo run the data pipeline:")
    logger.info("  1. Place CSV files in data/csv/")
    logger.info("  2. Run: python -c 'from data_pipeline import *; run_pipeline()'")
    logger.info("  3. Tables will be created in your Databricks catalog")


def demo_6_document_chunking():
    """Demo 6: Document processing and chunking."""
    logger.info("\n" + "=" * 80)
    logger.info("DEMO 6: Document Processing - RAG Pipeline")
    logger.info("=" * 80)
    
    # Create chunker
    chunker = DocumentChunker(chunk_size=300, chunk_overlap=50)
    
    # Sample document
    sample_doc = """
    Revenue Analysis Guidelines
    
    When analyzing revenue trends, consider the following factors:
    
    1. Seasonality: Q4 typically shows 40% higher sales due to holiday shopping.
    2. Day of Week: Weekend sales patterns differ significantly from weekdays.
    3. Regional Variations: North region consistently outperforms other regions.
    4. Product Mix: Electronics category drives 60% of total revenue.
    
    Key Metrics:
    - Average Order Value (AOV): Total revenue / Number of orders
    - Revenue per Customer: Total revenue / Unique customers
    - Growth Rate: (Current Period - Previous Period) / Previous Period
    """
    
    # Chunk the document
    chunks = chunker.chunk_text(
        text=sample_doc,
        source="revenue_guidelines",
        metadata={"category": "analysis", "type": "guidelines"}
    )
    
    logger.info(f"\n📄 Original document length: {len(sample_doc)} characters")
    logger.info(f"✂️  Created {len(chunks)} chunks")
    
    for i, chunk in enumerate(chunks[:2], 1):
        logger.info(f"\n   Chunk {i}:")
        logger.info(f"   ID: {chunk.chunk_id}")
        logger.info(f"   Length: {len(chunk.content)} characters")
        logger.info(f"   Content preview: {chunk.content[:100]}...")


def demo_7_error_correction():
    """Demo 7: SQL error correction."""
    logger.info("\n" + "=" * 80)
    logger.info("DEMO 7: SQL Error Correction - Auto-Retry Logic")
    logger.info("=" * 80)
    
    from sql_error_correction import SQLCorrector, SQLErrorAnalyzer
    
    # Setup
    schema_manager = SchemaManager()
    sales_table = TableSchema(
        name="sales",
        columns=["transaction_id", "customer_id", "product_id", "amount", "date", "region"],
        column_types={"transaction_id": "STRING", "customer_id": "STRING", 
                     "product_id": "STRING", "amount": "DECIMAL", 
                     "date": "DATE", "region": "STRING"}
    )
    schema_manager.add_table(sales_table)
    
    corrector = SQLCorrector(schema_manager)
    
    # Example 1: Column name error
    wrong_sql = "SELECT transactoin_id, amount FROM sales"
    error_msg = "column 'transactoin_id' does not exist"
    
    logger.info(f"\n❌ Wrong SQL:")
    logger.info(f"   {wrong_sql}")
    logger.info(f"\n   Error: {error_msg}")
    
    correction = corrector.correct_query(wrong_sql, error_msg)
    
    if correction:
        logger.info(f"\n✅ Corrected SQL:")
        logger.info(f"   {correction.corrected_sql}")
        logger.info(f"   Confidence: {correction.confidence:.1%}")


def demo_8_security_validation():
    """Demo 8: Security validation."""
    logger.info("\n" + "=" * 80)
    logger.info("DEMO 8: Security Validation - SQL Injection Protection")
    logger.info("=" * 80)
    
    schema_manager = SchemaManager()
    config = SecurityConfig()
    rate_limiter = RateLimiter(max_requests=60)
    schema_validator = SchemaValidator(schema_manager, config)
    validator = SecurityValidator(config, rate_limiter, schema_validator)
    
    # Test queries
    test_queries = [
        ("Legitimate query", "SELECT * FROM sales WHERE region = 'North'", True),
        ("SQL injection attempt", "'; DROP TABLE sales; --", False),
        ("XSS attempt", "<script>alert('xss')</script>", False),
        ("Too long query", "SELECT * FROM sales WHERE " + "x = 1 OR " * 1000, False),
    ]
    
    for name, query, expected_valid in test_queries:
        is_valid, error = validator.validate_query(query)
        status = "✅" if is_valid else "❌"
        logger.info(f"\n{status} {name}:")
        logger.info(f"   Query: {query[:80]}...")
        logger.info(f"   Valid: {is_valid}")
        if not is_valid:
            logger.info(f"   Error: {error}")


def main():
    """Run all demos."""
    
    # Setup
    config = setup_example_environment()
    
    # Check if Databricks credentials are configured
    if not config['databricks_hostname']:
        logger.warning("\n⚠️  Databricks credentials not configured!")
        logger.warning("Some demos will run in mock mode.")
        logger.warning("Configure .env file for full functionality.\n")
    
    # Initialize agent
    logger.info("\n🔧 Initializing agent components...")
    
    try:
        # Setup schema
        schema_manager = SchemaManager()
        
        # Add sample tables
        sales_table = TableSchema(
            name="sales",
            columns=["transaction_id", "customer_id", "product_id", "amount", "date", "region"],
            column_types={
                "transaction_id": "STRING",
                "customer_id": "STRING",
                "product_id": "STRING",
                "amount": "DECIMAL",
                "date": "DATE",
                "region": "STRING"
            },
            description="Sales transaction data"
        )
        schema_manager.add_table(sales_table)
        
        # Initialize components
        sql_generator = SQLGenerator(schema_manager)
        
        # Context retriever with knowledge base
        context_retriever = ContextRetriever(
            embedding_model="all-MiniLM-L6-v2",
            index_path=config['faiss_index_path']
        )
        
        # Add knowledge base documents
        kb_documents = create_knowledge_base_documents()
        from context_retriever import Document
        documents = [Document(content=chunk.content, metadata=chunk.metadata) 
                    for chunk in kb_documents]
        context_retriever.add_documents(documents)
        
        # Security
        security_config = SecurityConfig()
        rate_limiter = RateLimiter(max_requests=60)
        schema_validator = SchemaValidator(schema_manager, security_config)
        security_validator = SecurityValidator(security_config, rate_limiter, schema_validator)
        
        # Databricks client (may be None in mock mode)
        databricks_client = None
        if config['databricks_hostname'] and config['databricks_token']:
            databricks_client = DatabricksClient(
                server_hostname=config['databricks_hostname'],
                http_path=config['databricks_http_path'],
                access_token=config['databricks_token']
            )
        
        # Create agent
        agent = DatabricksInsightAgent(
            databricks_client=databricks_client,
            schema_manager=schema_manager,
            sql_generator=sql_generator,
            context_retriever=context_retriever,
            security_validator=security_validator
        )
        
        logger.info("✅ Agent initialized successfully!\n")
        
        # Run demos
        demo_1_basic_query(agent)
        demo_2_filtered_query(agent)
        demo_3_context_query(agent)
        demo_4_hybrid_query(agent)
        demo_5_data_pipeline()
        demo_6_document_chunking()
        demo_7_error_correction()
        demo_8_security_validation()
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ All demos completed!")
        logger.info("=" * 80)
        logger.info("\nNext steps:")
        logger.info("  1. Configure Databricks credentials in .env")
        logger.info("  2. Run: streamlit run app.py")
        logger.info("  3. Upload your CSV files for data ingestion")
        logger.info("  4. Start querying your data!\n")
        
    except Exception as e:
        logger.error(f"\n❌ Error running demos: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
