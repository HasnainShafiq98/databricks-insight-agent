"""
Main application entry point for the Databricks Insight Agent.
Provides a CLI interface for interacting with the agent.
"""

import os
import sys
import logging
from dotenv import load_dotenv
import colorlog

from databricks_client import DatabricksClient
from sql_generator import SchemaManager, SQLGenerator, TableSchema
from context_retriever import ContextRetriever, create_sample_documents
from security import SecurityValidator, SecurityConfig, SchemaValidator, RateLimiter
from agent import DatabricksInsightAgent, QueryType


def setup_logging():
    """Configure colored logging."""
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        '%(log_color)s%(levelname)-8s%(reset)s %(blue)s%(message)s',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    ))
    
    logger = colorlog.getLogger()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def load_configuration():
    """Load configuration from environment variables."""
    load_dotenv()
    
    config = {
        'databricks_hostname': os.getenv('DATABRICKS_SERVER_HOSTNAME'),
        'databricks_http_path': os.getenv('DATABRICKS_HTTP_PATH'),
        'databricks_token': os.getenv('DATABRICKS_ACCESS_TOKEN'),
        'openai_api_key': os.getenv('OPENAI_API_KEY'),
        'faiss_index_path': os.getenv('FAISS_INDEX_PATH', './data/faiss_index.faiss'),
        'max_query_length': int(os.getenv('MAX_QUERY_LENGTH', '10000')),
        'rate_limit_per_minute': int(os.getenv('RATE_LIMIT_PER_MINUTE', '60')),
        'allowed_schemas': os.getenv('ALLOWED_SCHEMAS', 'default,analytics').split(','),
        'log_level': os.getenv('LOG_LEVEL', 'INFO'),
    }
    
    return config


def initialize_schema_manager(sample_mode: bool = True):
    """
    Initialize the schema manager with table definitions.
    In production, this would load from Databricks.
    
    Args:
        sample_mode: If True, use sample schema for demonstration
    """
    schema_manager = SchemaManager()
    
    if sample_mode:
        # Add sample tables for demonstration
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
        
        customers_table = TableSchema(
            name="customers",
            columns=["customer_id", "name", "email", "registration_date", "country"],
            column_types={
                "customer_id": "STRING",
                "name": "STRING",
                "email": "STRING",
                "registration_date": "DATE",
                "country": "STRING"
            },
            description="Customer information"
        )
        
        products_table = TableSchema(
            name="products",
            columns=["product_id", "name", "category", "price", "stock_quantity"],
            column_types={
                "product_id": "STRING",
                "name": "STRING",
                "category": "STRING",
                "price": "DECIMAL",
                "stock_quantity": "INT"
            },
            description="Product catalog"
        )
        
        schema_manager.add_table(sales_table)
        schema_manager.add_table(customers_table)
        schema_manager.add_table(products_table)
    
    return schema_manager


def initialize_agent(config: dict, demo_mode: bool = False):
    """
    Initialize the Databricks Insight Agent with all components.
    
    Args:
        config: Configuration dictionary
        demo_mode: If True, use mock components for demonstration
    """
    logger = logging.getLogger(__name__)
    
    # Initialize schema manager
    schema_manager = initialize_schema_manager(sample_mode=True)
    logger.info(f"Loaded schema with {len(schema_manager.get_all_tables())} tables")
    
    # Initialize SQL generator
    sql_generator = SQLGenerator(schema_manager)
    
    # Initialize security components
    security_config = SecurityConfig(
        max_query_length=config['max_query_length'],
        rate_limit_per_minute=config['rate_limit_per_minute'],
        allowed_schemas=config['allowed_schemas']
    )
    security_validator = SecurityValidator(security_config)
    
    # Add rate limiter to security validator
    security_validator.rate_limiter = RateLimiter(config['rate_limit_per_minute'])
    security_validator.check_rate_limit = security_validator.rate_limiter.check_rate_limit
    
    # Initialize schema validator
    known_tables = {
        table_name: schema_manager.get_table_columns(table_name)
        for table_name in schema_manager.get_all_tables()
    }
    schema_validator = SchemaValidator(known_tables)
    
    # Initialize context retriever
    try:
        context_retriever = ContextRetriever(index_path=config['faiss_index_path'])
        
        # Add sample documents if index is empty
        if len(context_retriever.documents) == 0:
            logger.info("Initializing context with sample documents")
            sample_docs = create_sample_documents()
            context_retriever.add_documents(sample_docs)
            context_retriever.save_index()
    except Exception as e:
        logger.warning(f"Could not initialize context retriever: {e}")
        logger.warning("Continuing without context retrieval capability")
        context_retriever = None
    
    # Initialize Databricks client
    if not demo_mode and all([
        config['databricks_hostname'],
        config['databricks_http_path'],
        config['databricks_token']
    ]):
        databricks_client = DatabricksClient(
            server_hostname=config['databricks_hostname'],
            http_path=config['databricks_http_path'],
            access_token=config['databricks_token']
        )
        logger.info("Initialized Databricks client")
    else:
        logger.warning("Running in demo mode - Databricks client not initialized")
        databricks_client = None
    
    # Create the agent
    agent = DatabricksInsightAgent(
        databricks_client=databricks_client,
        schema_manager=schema_manager,
        sql_generator=sql_generator,
        context_retriever=context_retriever,
        security_validator=security_validator
    )
    
    return agent


def print_response(response):
    """Pretty print agent response."""
    print("\n" + "=" * 80)
    
    if not response.success:
        print("❌ Query failed")
        if response.error:
            print(f"Error: {response.error}")
        if response.clarification_needed:
            print(f"ℹ️  {response.clarification_needed}")
        print("=" * 80)
        return
    
    print("✅ Query successful")
    print(f"Query Type: {response.query_type.value}")
    
    if response.sql_query:
        print(f"\n📊 Generated SQL:")
        print(f"  {response.sql_query}")
    
    if response.results:
        print(f"\n📈 Results: {len(response.results)} record(s)")
        if len(response.results) <= 5:
            for i, row in enumerate(response.results, 1):
                print(f"  {i}. {row}")
        else:
            for i, row in enumerate(response.results[:3], 1):
                print(f"  {i}. {row}")
            print(f"  ... and {len(response.results) - 3} more records")
    
    if response.insights:
        print(f"\n💡 Insights:")
        for line in response.insights.split('\n'):
            print(f"  {line}")
    
    print("=" * 80)


def run_cli(agent):
    """Run the interactive CLI."""
    print("\n" + "=" * 80)
    print("🤖 Databricks Insight Agent")
    print("=" * 80)
    print("\nWelcome! I'm your AI analytics assistant for Databricks.")
    print("Ask me questions about your data, and I'll help you find insights.")
    print("\nCommands:")
    print("  - Type your question in natural language")
    print("  - Type 'schema' to see available tables")
    print("  - Type 'exit' or 'quit' to exit")
    print("=" * 80 + "\n")
    
    while True:
        try:
            user_input = input("You: ").strip()
            
            if not user_input:
                continue
            
            if user_input.lower() in ['exit', 'quit', 'q']:
                print("\n👋 Goodbye!")
                break
            
            if user_input.lower() == 'schema':
                print("\n📋 Available Schema:")
                print(agent.schema_manager.get_schema_summary())
                continue
            
            # Process the query
            response = agent.process_query(user_input)
            print_response(response)
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            logging.error(f"Error processing query: {e}", exc_info=True)
            print(f"\n❌ An error occurred: {e}")


def main():
    """Main application entry point."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Load configuration
    config = load_configuration()
    
    # Check if running in demo mode
    demo_mode = not all([
        config['databricks_hostname'],
        config['databricks_http_path'],
        config['databricks_token']
    ])
    
    if demo_mode:
        logger.warning("Running in DEMO mode - configure .env file for full functionality")
    
    # Initialize agent
    try:
        agent = initialize_agent(config, demo_mode=demo_mode)
    except Exception as e:
        logger.error(f"Failed to initialize agent: {e}", exc_info=True)
        sys.exit(1)
    
    # Run CLI
    run_cli(agent)


if __name__ == "__main__":
    main()
