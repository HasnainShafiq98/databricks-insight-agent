"""
Simple tests for the Databricks Insight Agent core functionality.
These tests don't require external dependencies or network access.
"""

import sys
sys.path.insert(0, '/home/runner/work/databricks-insight-agent/databricks-insight-agent')

from sql_generator import SchemaManager, SQLGenerator, TableSchema
from security import SecurityValidator, SecurityConfig, SchemaValidator, RateLimiter


def test_schema_manager():
    """Test schema manager functionality."""
    print("\n=== Testing Schema Manager ===")
    
    schema_manager = SchemaManager()
    
    # Add a sample table
    sales_table = TableSchema(
        name="sales",
        columns=["transaction_id", "customer_id", "amount", "date"],
        column_types={
            "transaction_id": "STRING",
            "customer_id": "STRING",
            "amount": "DECIMAL",
            "date": "DATE"
        }
    )
    
    schema_manager.add_table(sales_table)
    
    # Test retrieval
    assert "sales" in schema_manager.get_all_tables(), "Failed to add table"
    assert schema_manager.column_exists("sales", "amount"), "Column not found"
    assert not schema_manager.column_exists("sales", "invalid_col"), "Invalid column found"
    
    print("✓ Schema manager working correctly")


def test_sql_generator():
    """Test SQL generation."""
    print("\n=== Testing SQL Generator ===")
    
    schema_manager = SchemaManager()
    sales_table = TableSchema(
        name="sales",
        columns=["transaction_id", "customer_id", "amount", "date", "region"],
        column_types={
            "transaction_id": "STRING",
            "customer_id": "STRING",
            "amount": "DECIMAL",
            "date": "DATE",
            "region": "STRING"
        }
    )
    schema_manager.add_table(sales_table)
    
    sql_gen = SQLGenerator(schema_manager)
    
    # Test 1: Simple SELECT
    sql = sql_gen.generate_sql(table_name="sales")
    assert sql == "SELECT transaction_id, customer_id, amount, date, region FROM sales", f"Unexpected SQL: {sql}"
    print(f"✓ Simple SELECT: {sql}")
    
    # Test 2: SELECT with filter
    sql = sql_gen.generate_sql(
        table_name="sales",
        filters={"region": "US"}
    )
    assert "WHERE" in sql and "region = 'US'" in sql, f"Unexpected SQL: {sql}"
    print(f"✓ SELECT with filter: {sql}")
    
    # Test 3: SELECT with aggregation
    sql = sql_gen.generate_sql(
        table_name="sales",
        aggregations={"amount": "SUM"},
        group_by=["region"]
    )
    assert "SUM(amount)" in sql and "GROUP BY region" in sql, f"Unexpected SQL: {sql}"
    print(f"✓ SELECT with aggregation: {sql}")
    
    # Test 4: Invalid column should fail
    sql = sql_gen.generate_sql(
        table_name="sales",
        columns=["invalid_column"]
    )
    assert sql is None, "Should have rejected invalid column"
    print("✓ Invalid column rejected")
    
    # Test 5: SELECT with limit
    sql = sql_gen.generate_sql(
        table_name="sales",
        limit=10
    )
    assert "LIMIT 10" in sql, f"Unexpected SQL: {sql}"
    print(f"✓ SELECT with limit: {sql}")


def test_security_validator():
    """Test security validation."""
    print("\n=== Testing Security Validator ===")
    
    config = SecurityConfig()
    validator = SecurityValidator(config)
    
    # Test 1: Valid query
    is_valid, error = validator.validate_query("Show me sales data")
    assert is_valid, f"Valid query rejected: {error}"
    print("✓ Valid query accepted")
    
    # Test 2: SQL injection attempt
    is_valid, error = validator.validate_query("SELECT * FROM sales; DROP TABLE sales;--")
    assert not is_valid, "SQL injection not detected"
    print(f"✓ SQL injection blocked: {error}")
    
    # Test 3: Dangerous keyword
    is_valid, error = validator.validate_query("DELETE FROM sales")
    assert not is_valid, "Dangerous keyword not detected"
    print(f"✓ Dangerous keyword blocked: {error}")
    
    # Test 4: Too long query
    long_query = "a" * 20000
    is_valid, error = validator.validate_query(long_query)
    assert not is_valid, "Long query not detected"
    print(f"✓ Long query blocked: {error}")
    
    # Test 5: Empty query
    is_valid, error = validator.validate_query("")
    assert not is_valid, "Empty query not detected"
    print(f"✓ Empty query blocked: {error}")


def test_sql_validation():
    """Test SQL validation."""
    print("\n=== Testing SQL Validation ===")
    
    config = SecurityConfig()
    validator = SecurityValidator(config)
    
    # Test 1: Valid SELECT
    sql = "SELECT customer_id, amount FROM sales"
    is_valid, error = validator.validate_sql(sql)
    assert is_valid, f"Valid SQL rejected: {error}"
    print(f"✓ Valid SQL accepted: {sql}")
    
    # Test 2: Non-SELECT query (should fail)
    sql = "INSERT INTO sales VALUES (1, 2, 3)"
    is_valid, error = validator.validate_sql(sql)
    assert not is_valid, "Non-SELECT query not detected"
    print(f"✓ Non-SELECT blocked: {error}")


def test_schema_validator():
    """Test schema validator."""
    print("\n=== Testing Schema Validator ===")
    
    known_tables = {
        "sales": ["transaction_id", "customer_id", "amount"],
        "customers": ["customer_id", "name", "email"]
    }
    
    validator = SchemaValidator(known_tables)
    
    # Test 1: Known columns
    sql = "SELECT customer_id, amount FROM sales"
    is_valid, unknown = validator.validate_columns(sql)
    assert is_valid, f"Valid columns rejected: {unknown}"
    print("✓ Known columns accepted")
    
    # Test 2: Get table columns
    columns = validator.get_table_columns("sales")
    assert columns == ["transaction_id", "customer_id", "amount"], f"Unexpected columns: {columns}"
    print(f"✓ Table columns retrieved: {columns}")


def test_rate_limiter():
    """Test rate limiter."""
    print("\n=== Testing Rate Limiter ===")
    
    limiter = RateLimiter(max_calls_per_minute=3)
    
    # Test 1: First 3 calls should succeed
    for i in range(3):
        is_allowed, error = limiter.check_rate_limit("user1")
        assert is_allowed, f"Call {i+1} rejected: {error}"
    print("✓ First 3 calls allowed")
    
    # Test 2: 4th call should be rejected
    is_allowed, error = limiter.check_rate_limit("user1")
    assert not is_allowed, "Rate limit not enforced"
    print(f"✓ Rate limit enforced: {error}")
    
    # Test 3: Different user should be allowed
    is_allowed, error = limiter.check_rate_limit("user2")
    assert is_allowed, f"Different user rejected: {error}"
    print("✓ Different user allowed")


def test_input_sanitization():
    """Test input sanitization."""
    print("\n=== Testing Input Sanitization ===")
    
    config = SecurityConfig()
    validator = SecurityValidator(config)
    
    # Test 1: Remove null bytes
    sanitized = validator.sanitize_input("test\x00data")
    assert "\x00" not in sanitized, "Null byte not removed"
    print("✓ Null bytes removed")
    
    # Test 2: Normalize whitespace
    sanitized = validator.sanitize_input("test    data   with   spaces")
    assert "    " not in sanitized, "Excess whitespace not normalized"
    print(f"✓ Whitespace normalized: '{sanitized}'")


def run_all_tests():
    """Run all tests."""
    print("\n" + "=" * 80)
    print("Running Databricks Insight Agent Tests")
    print("=" * 80)
    
    try:
        test_schema_manager()
        test_sql_generator()
        test_security_validator()
        test_sql_validation()
        test_schema_validator()
        test_rate_limiter()
        test_input_sanitization()
        
        print("\n" + "=" * 80)
        print("✅ All tests passed!")
        print("=" * 80)
        return True
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
