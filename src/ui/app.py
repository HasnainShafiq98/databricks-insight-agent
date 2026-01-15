"""
Streamlit UI for Databricks Insight Agent.
Provides an interactive chat-style interface for querying Databricks data.
"""

import os
import sys
import streamlit as st
import logging
from dotenv import load_dotenv
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.data.databricks_client import DatabricksClient
from src.intelligence.sql_generator import SchemaManager, SQLGenerator, TableSchema
from src.intelligence.schema_loader import SchemaLoader, create_schema_manager_from_databricks
from src.intelligence.context_retriever import ContextRetriever
from src.intelligence.document_processor import create_knowledge_base_documents
from src.security.security import SecurityValidator, SecurityConfig, SchemaValidator, RateLimiter
from src.core.agent import DatabricksInsightAgent, QueryType


# Page configuration
st.set_page_config(
    page_title="Databricks Insight Agent",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FF3621;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .query-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    .result-card {
        background-color: #e8f4f8;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    .sql-code {
        background-color: #282c34;
        color: #abb2bf;
        padding: 1rem;
        border-radius: 0.5rem;
        font-family: 'Courier New', monospace;
        margin: 1rem 0;
    }
    .insight-box {
        background-color: #f0f9ff;
        border-left: 4px solid #0284c7;
        padding: 1rem;
        margin: 1rem 0;
    }
    .context-box {
        background-color: #fef3c7;
        border-left: 4px solid #f59e0b;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #fee2e2;
        border-left: 4px solid #dc2626;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


def setup_logging():
    """Configure logging for Streamlit."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    if 'agent' not in st.session_state:
        st.session_state.agent = None
    if 'initialized' not in st.session_state:
        st.session_state.initialized = False


def load_configuration():
    """Load configuration from environment variables."""
    load_dotenv()
    
    config = {
        'databricks_hostname': os.getenv('DATABRICKS_SERVER_HOSTNAME'),
        'databricks_http_path': os.getenv('DATABRICKS_HTTP_PATH'),
        'databricks_token': os.getenv('DATABRICKS_ACCESS_TOKEN'),
        'catalog': os.getenv('DATABRICKS_CATALOG', 'workspace'),
        'schema': os.getenv('DATABRICKS_SCHEMA', 'default'),
        'mistral_api_key': os.getenv('MISTRAL_API_KEY'),
        'mistral_model': os.getenv('MISTRAL_MODEL', 'mistral-large-latest'),
        'faiss_index_path': os.getenv('FAISS_INDEX_PATH', './data/faiss_index.faiss'),
        'max_query_length': int(os.getenv('MAX_QUERY_LENGTH', '10000')),
        'rate_limit_per_minute': int(os.getenv('RATE_LIMIT_PER_MINUTE', '60')),
        'allowed_schemas': os.getenv('ALLOWED_SCHEMAS', 'default,analytics').split(','),
    }
    
    return config


def initialize_agent(config, use_sample_data=True):
    """Initialize the Databricks Insight Agent with all components."""
    
    try:
        # Initialize Databricks client
        if config['databricks_hostname'] and config['databricks_token']:
            databricks_client = DatabricksClient(
                server_hostname=config['databricks_hostname'],
                http_path=config['databricks_http_path'],
                access_token=config['databricks_token']
            )
        else:
            st.warning("⚠️ Databricks credentials not configured. Using mock mode.")
            databricks_client = None
        
        # Initialize schema manager with auto-detection
        if databricks_client:
            # Auto-detect schema from Databricks
            schema_manager = create_schema_manager_from_databricks(
                databricks_client=databricks_client,
                catalog=config.get('catalog', 'hive_metastore'),
                schema=config.get('schema', 'default'),
                specific_tables=config.get('specific_tables', None),
                fallback_to_sample=use_sample_data
            )
        else:
            # Use sample schema when no Databricks connection
            schema_manager = create_schema_manager_from_databricks(
                databricks_client=None,
                fallback_to_sample=True
            )
        
        # Initialize SQL generator
        sql_generator = SQLGenerator(schema_manager)
        
        # Initialize context retriever
        context_retriever = ContextRetriever(
            embedding_model="all-MiniLM-L6-v2",
            index_path=config['faiss_index_path']
        )
        
        # Add sample documents if no index exists
        if use_sample_data:
            kb_chunks = create_knowledge_base_documents()
            from src.intelligence.context_retriever import Document
            documents = [Document(content=chunk.content, metadata=chunk.metadata) 
                        for chunk in kb_chunks]
            context_retriever.add_documents(documents)
        
        # Initialize security components
        security_config = SecurityConfig(
            max_query_length=config['max_query_length'],
            rate_limit_per_minute=config['rate_limit_per_minute'],
            allowed_schemas=config['allowed_schemas']
        )
        
        rate_limiter = RateLimiter(max_calls_per_minute=security_config.rate_limit_per_minute)
        
        # Build known_tables dictionary for schema validator
        known_tables = {
            table_name: schema_manager.get_table_columns(table_name)
            for table_name in schema_manager.get_all_tables()
        }
        schema_validator = SchemaValidator(known_tables)
        
        security_validator = SecurityValidator(security_config)
        
        # Initialize LLM service (optional)
        llm_service = None
        if config.get('mistral_api_key'):
            try:
                from src.intelligence.llm_service import create_llm_service
                llm_service = create_llm_service(
                    api_key=config['mistral_api_key'],
                    model=config.get('mistral_model', 'mistral-large-latest')
                )
                if llm_service:
                    st.success("✅ Mistral AI enabled for enhanced query understanding")
            except Exception as e:
                st.warning(f"⚠️ Could not initialize Mistral AI: {e}")
        
        # Create agent
        agent = DatabricksInsightAgent(
            databricks_client=databricks_client,
            schema_manager=schema_manager,
            sql_generator=sql_generator,
            context_retriever=context_retriever,
            security_validator=security_validator,
            rate_limiter=rate_limiter,
            llm_service=llm_service
        )
        
        return agent
        
    except Exception as e:
        st.error(f"❌ Failed to initialize agent: {str(e)}")
        return None


def render_sidebar():
    """Render the sidebar with configuration and schema information."""
    with st.sidebar:
        st.markdown("### 🔧 Configuration")
        
        config = load_configuration()
        
        # Connection status
        if config['databricks_hostname']:
            st.success("✅ Databricks Connected")
        else:
            st.warning("⚠️ No Databricks Connection")
        
        st.markdown("---")
        
        # Schema information
        st.markdown("### 📊 Available Tables")
        
        if st.session_state.agent:
            schema_manager = st.session_state.agent.schema_manager
            for table_name in schema_manager.get_all_tables():
                with st.expander(f"📋 {table_name}"):
                    table = schema_manager.get_table(table_name)
                    if table.description:
                        st.caption(table.description)
                    st.markdown("**Columns:**")
                    for col in table.columns:
                        col_type = table.column_types.get(col, "unknown")
                        st.text(f"  • {col} ({col_type})")
        
        st.markdown("---")
        
        # Query examples
        st.markdown("### 💡 Example Queries")
        example_queries = [
            "Show me total sales by region",
            "What are the top 5 products by revenue?",
            "How many customers registered last month?",
            "What is the average order value?",
            "Show sales trends over time"
        ]
        
        for query in example_queries:
            if st.button(query, key=f"example_{query}"):
                st.session_state.example_query = query
        
        st.markdown("---")
        
        # Clear chat button
        if st.button("🗑️ Clear Chat History"):
            st.session_state.messages = []
            st.rerun()


def render_message(message):
    """Render a chat message with proper formatting."""
    
    role = message["role"]
    
    if role == "user":
        with st.chat_message("user"):
            st.markdown(message.get("content", ""))
    else:
        with st.chat_message("assistant"):
            # Display the main insight
            if "insights" in message:
                st.markdown(f'<div class="insight-box">{message["insights"]}</div>', 
                          unsafe_allow_html=True)
            
            # Display SQL query if available
            if message.get("sql_query"):
                with st.expander("🔍 View Generated SQL"):
                    st.code(message["sql_query"], language="sql")
            
            # Display query results if available
            if message.get("results"):
                with st.expander("📊 View Query Results"):
                    import pandas as pd
                    df = pd.DataFrame(message["results"])
                    st.dataframe(df, use_container_width=True)
            
            # Display retrieved context if available
            if message.get("context"):
                with st.expander("📚 View Retrieved Context"):
                    st.markdown(f'<div class="context-box">{message["context"]}</div>', 
                              unsafe_allow_html=True)
            
            # Display error if any
            if message.get("error"):
                st.markdown(f'<div class="error-box"><strong>Error:</strong> {message["error"]}</div>', 
                          unsafe_allow_html=True)
            
            # Display clarification needed
            if message.get("clarification_needed"):
                st.info(f"❓ {message['clarification_needed']}")


def main():
    """Main Streamlit application."""
    
    setup_logging()
    initialize_session_state()
    
    # Header
    st.markdown('<div class="main-header">🔍 Databricks Insight Agent</div>', 
                unsafe_allow_html=True)
    st.markdown('<div class="sub-header">AI-powered analytics assistant for your Databricks lakehouse</div>', 
                unsafe_allow_html=True)
    
    # Initialize agent if not already done
    if not st.session_state.initialized:
        with st.spinner("🔄 Initializing agent..."):
            config = load_configuration()
            agent = initialize_agent(config, use_sample_data=True)
            
            if agent:
                st.session_state.agent = agent
                st.session_state.initialized = True
                st.success("✅ Agent initialized successfully!")
            else:
                st.error("❌ Failed to initialize agent. Please check configuration.")
                return
    
    # Render sidebar
    render_sidebar()
    
    # Display chat history
    for message in st.session_state.messages:
        render_message(message)
    
    # Handle example query selection
    if 'example_query' in st.session_state:
        user_query = st.session_state.example_query
        del st.session_state.example_query
    else:
        user_query = st.chat_input("Ask me anything about your data...")
    
    # Process new query
    if user_query:
        # Add user message
        st.session_state.messages.append({
            "role": "user",
            "content": user_query
        })
        
        # Display user message
        with st.chat_message("user"):
            st.markdown(user_query)
        
        # Process query with agent
        with st.chat_message("assistant"):
            with st.spinner("🤔 Analyzing query..."):
                response = st.session_state.agent.process_query(user_query, user_id="streamlit_user")
            
            # Create response message
            response_message = {
                "role": "assistant",
                "content": response.insights or response.error or response.clarification_needed or "",
                "insights": response.insights,
                "sql_query": response.sql_query,
                "results": response.results,
                "context": response.context,
                "error": response.error,
                "clarification_needed": response.clarification_needed
            }
            
            # Add to messages
            st.session_state.messages.append(response_message)
            
            # Render response
            render_message(response_message)
        
        # Force rerun to update chat
        st.rerun()


if __name__ == "__main__":
    main()
