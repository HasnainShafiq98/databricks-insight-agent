"""
Example usage of the Databricks Insight Agent.
Demonstrates various query types and capabilities.
"""

from main import initialize_agent, load_configuration, setup_logging


def demo_security_features(agent):
    """Demonstrate security features."""
    print("\n" + "=" * 80)
    print("DEMO: Security Features")
    print("=" * 80)
    
    # Test 1: SQL injection attempt
    print("\n1. Testing SQL injection protection...")
    malicious_query = "Show sales WHERE 1=1; DROP TABLE sales;--"
    response = agent.process_query(malicious_query)
    print(f"   Result: {response.error if not response.success else 'Blocked'}")
    
    # Test 2: Dangerous keyword
    print("\n2. Testing dangerous keyword detection...")
    dangerous_query = "DELETE FROM sales WHERE region = 'US'"
    response = agent.process_query(dangerous_query)
    print(f"   Result: {response.error if not response.success else 'Blocked'}")
    
    # Test 3: Valid query
    print("\n3. Testing valid query...")
    valid_query = "Show me sales data"
    response = agent.process_query(valid_query)
    print(f"   Result: {'Passed security checks' if response.success or response.clarification_needed else 'Failed'}")


def demo_query_understanding(agent):
    """Demonstrate query understanding capabilities."""
    print("\n" + "=" * 80)
    print("DEMO: Query Understanding")
    print("=" * 80)
    
    queries = [
        "Show me all sales",
        "What is the sales table?",
        "Get customer information and explain the schema",
        "Calculate total revenue by region"
    ]
    
    for i, query in enumerate(queries, 1):
        print(f"\n{i}. Query: '{query}'")
        analysis = agent.analyze_query(query)
        print(f"   Type: {analysis.query_type.value}")
        print(f"   Target tables: {analysis.target_tables}")
        print(f"   Confidence: {analysis.confidence}")


def demo_sql_generation(agent):
    """Demonstrate SQL generation from schema."""
    print("\n" + "=" * 80)
    print("DEMO: SQL Generation")
    print("=" * 80)
    
    # Test 1: Simple SELECT
    print("\n1. Simple select all from sales:")
    sql = agent.sql_generator.generate_sql(table_name="sales")
    print(f"   SQL: {sql}")
    
    # Test 2: With filters
    print("\n2. With filters:")
    sql = agent.sql_generator.generate_sql(
        table_name="sales",
        filters={"region": "US"}
    )
    print(f"   SQL: {sql}")
    
    # Test 3: With aggregation
    print("\n3. With aggregation:")
    sql = agent.sql_generator.generate_sql(
        table_name="sales",
        aggregations={"amount": "SUM"},
        group_by=["region"]
    )
    print(f"   SQL: {sql}")
    
    # Test 4: Invalid column (should fail)
    print("\n4. Invalid column (should fail):")
    sql = agent.sql_generator.generate_sql(
        table_name="sales",
        columns=["nonexistent_column"]
    )
    print(f"   SQL: {sql if sql else 'Failed - column not in schema ✓'}")


def demo_context_retrieval(agent):
    """Demonstrate FAISS context retrieval."""
    print("\n" + "=" * 80)
    print("DEMO: Context Retrieval")
    print("=" * 80)
    
    if not agent.context_retriever:
        print("Context retriever not available")
        return
    
    queries = [
        "sales table",
        "customer information",
        "how to calculate total sales"
    ]
    
    for i, query in enumerate(queries, 1):
        print(f"\n{i}. Query: '{query}'")
        results = agent.context_retriever.search(query, top_k=2)
        for j, (doc, score) in enumerate(results, 1):
            print(f"   Match {j} (score: {score:.4f}):")
            print(f"   {doc.content[:100]}...")


def demo_end_to_end_queries(agent):
    """Demonstrate end-to-end query processing."""
    print("\n" + "=" * 80)
    print("DEMO: End-to-End Query Processing")
    print("=" * 80)
    
    queries = [
        "Show me the sales table schema",
        "List all products",
        "Get customer data from USA",
    ]
    
    for i, query in enumerate(queries, 1):
        print(f"\n{i}. Processing: '{query}'")
        response = agent.process_query(query)
        
        if response.success:
            print(f"   ✓ Success")
            if response.sql_query:
                print(f"   SQL: {response.sql_query}")
        elif response.clarification_needed:
            print(f"   ℹ️  Clarification: {response.clarification_needed}")
        else:
            print(f"   ✗ Error: {response.error}")


def demo_schema_validation(agent):
    """Demonstrate schema validation."""
    print("\n" + "=" * 80)
    print("DEMO: Schema Validation")
    print("=" * 80)
    
    print("\n1. Available tables:")
    for table in agent.schema_manager.get_all_tables():
        print(f"   - {table}")
    
    print("\n2. Sales table schema:")
    table = agent.schema_manager.get_table("sales")
    if table:
        for col in table.columns:
            col_type = table.column_types.get(col, "unknown")
            print(f"   - {col}: {col_type}")
    
    print("\n3. Column validation:")
    print(f"   'amount' in sales: {agent.schema_manager.column_exists('sales', 'amount')}")
    print(f"   'invalid_col' in sales: {agent.schema_manager.column_exists('sales', 'invalid_col')}")


def main():
    """Run all demos."""
    setup_logging()
    
    print("\n" + "=" * 80)
    print("🤖 Databricks Insight Agent - Demo")
    print("=" * 80)
    print("\nThis demo showcases the key features of the agent.")
    print("Note: Some features require Databricks connection.")
    
    # Load configuration and initialize agent
    config = load_configuration()
    agent = initialize_agent(config, demo_mode=True)
    
    # Run demos
    demo_schema_validation(agent)
    demo_security_features(agent)
    demo_query_understanding(agent)
    demo_sql_generation(agent)
    demo_context_retrieval(agent)
    demo_end_to_end_queries(agent)
    
    print("\n" + "=" * 80)
    print("✅ Demo completed!")
    print("=" * 80)


if __name__ == "__main__":
    main()
