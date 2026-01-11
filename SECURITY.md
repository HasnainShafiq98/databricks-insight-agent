# Security and Protection Rules

This document details the comprehensive security and protection mechanisms implemented in the Databricks Insight Agent.

## Table of Contents
1. [Overview](#overview)
2. [Input Validation](#input-validation)
3. [SQL Injection Prevention](#sql-injection-prevention)
4. [Schema-Based SQL Generation](#schema-based-sql-generation)
5. [SQL Validation](#sql-validation)
6. [Rate Limiting](#rate-limiting)
7. [Access Control](#access-control)
8. [Audit Logging](#audit-logging)
9. [Best Practices](#best-practices)

## Overview

The Databricks Insight Agent implements defense-in-depth security with multiple layers of protection:

```
User Input
    ↓
[1] Input Validation & Sanitization
    ↓
[2] Rate Limiting
    ↓
[3] Query Analysis
    ↓
[4] Schema-Based SQL Generation (prevents hallucination)
    ↓
[5] SQL Validation (injection prevention)
    ↓
[6] Access Control (schema whitelist)
    ↓
[7] Query Execution
    ↓
[8] Audit Logging
```

## Input Validation

### Purpose
Validate and sanitize all user input before processing to prevent malicious queries.

### Rules

#### 1. Query Length Limit
- **Default**: 10,000 characters
- **Configurable**: Via `MAX_QUERY_LENGTH` environment variable
- **Rationale**: Prevents denial-of-service attacks with extremely long queries

```python
# Configuration
MAX_QUERY_LENGTH=10000
```

#### 2. Empty Query Detection
- Rejects empty or whitespace-only queries
- Provides clear error message to user

#### 3. Null Byte Removal
- Automatically strips null bytes (`\x00`) from input
- Prevents null byte injection attacks

#### 4. Whitespace Normalization
- Collapses multiple spaces into single spaces
- Removes leading/trailing whitespace
- Helps with pattern detection

### Implementation

```python
class SecurityValidator:
    def validate_query(self, query: str) -> tuple[bool, Optional[str]]:
        # Length check
        if len(query) > self.config.max_query_length:
            return False, "Query exceeds maximum length"
        
        # Empty check
        if not query.strip():
            return False, "Query cannot be empty"
        
        # Additional validations...
```

## SQL Injection Prevention

### Purpose
Prevent SQL injection attacks through pattern detection and keyword blocking.

### Detection Patterns

The system detects and blocks the following SQL injection patterns:

#### 1. Comment-Based Injection
- Pattern: `;\s*--` (semicolon followed by SQL comment)
- Example: `'; DROP TABLE users;--`
- **Status**: ❌ BLOCKED

#### 2. Query Chaining
- Pattern: `;\s*$` (semicolon at end of query)
- Example: `SELECT * FROM sales; SELECT * FROM passwords`
- **Status**: ❌ BLOCKED

#### 3. Union-Based Injection
- Pattern: `UNION\s+SELECT`
- Example: `' UNION SELECT password FROM users--`
- **Status**: ❌ BLOCKED

#### 4. Tautology-Based Injection
- Pattern: `OR\s+1\s*=\s*1`
- Example: `WHERE id = 1 OR 1=1`
- **Status**: ❌ BLOCKED

#### 5. Quote-Based Injection
- Pattern: `'\s*OR\s*'`
- Example: `' OR '1'='1`
- **Status**: ❌ BLOCKED

#### 6. SQL Comments
- Patterns: `--\s*$`, `/\*.*\*/`
- Example: `SELECT * FROM sales -- malicious comment`
- **Status**: ❌ BLOCKED

#### 7. Stored Procedures
- Patterns: `xp_`, `sp_`
- Example: `xp_cmdshell`, `sp_executesql`
- **Status**: ❌ BLOCKED

### Dangerous Keywords

The following SQL keywords are **forbidden** in user queries:

- `DROP` - Cannot drop tables
- `DELETE` - Cannot delete records
- `TRUNCATE` - Cannot truncate tables
- `ALTER` - Cannot modify schema
- `CREATE` - Cannot create objects
- `INSERT` - Cannot insert data
- `UPDATE` - Cannot update records
- `GRANT` - Cannot grant permissions
- `REVOKE` - Cannot revoke permissions
- `EXEC`/`EXECUTE` - Cannot execute procedures

### Implementation

```python
dangerous_sql_keywords = [
    "DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE", 
    "INSERT", "UPDATE", "GRANT", "REVOKE", "EXEC", "EXECUTE"
]

sql_injection_patterns = [
    r";\s*--",           # Comment after semicolon
    r";\s*$",            # Query chaining
    r"UNION\s+SELECT",   # Union-based injection
    r"OR\s+1\s*=\s*1",   # Always true condition
    r"'\s*OR\s*'",       # Quote-based injection
    r"--\s*$",           # SQL comment at end
    r"/\*.*\*/",         # Multi-line comments
    r"xp_",              # Extended stored procedures
    r"sp_",              # System stored procedures
]
```

## Schema-Based SQL Generation

### Purpose
**Prevent column hallucination** by generating SQL only from known schema.

### How It Works

1. **Schema Registry**: Maintains a registry of all known tables and columns
2. **Column Validation**: Every column referenced must exist in schema
3. **Type Safety**: Column types are tracked and validated
4. **Automatic Rejection**: Queries referencing unknown columns are rejected

### Benefits

- ✅ **No Hallucination**: Cannot generate SQL for non-existent columns
- ✅ **Type Safety**: Ensures correct data types
- ✅ **Schema Compliance**: All queries conform to actual database schema
- ✅ **Clear Errors**: Users get helpful error messages about missing columns

### Example

```python
# Schema definition
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

# Valid query - all columns exist
sql = generator.generate_sql(
    table_name="sales",
    columns=["customer_id", "amount"]
)
# Result: SELECT customer_id, amount FROM sales ✅

# Invalid query - column doesn't exist
sql = generator.generate_sql(
    table_name="sales",
    columns=["nonexistent_column"]
)
# Result: None (rejected) ❌
# Error: Column nonexistent_column not found in table sales
```

## SQL Validation

### Purpose
Validate generated SQL before execution to ensure it's safe.

### Validation Steps

#### 1. Syntax Validation
- Uses `sqlparse` library to parse SQL
- Ensures well-formed SQL syntax
- Detects syntax errors before execution

#### 2. Statement Type Check
- **Allowed**: `SELECT` statements only
- **Blocked**: All other statement types (INSERT, UPDATE, DELETE, etc.)
- Ensures read-only access

#### 3. Schema Whitelist
- Validates that only allowed schemas are accessed
- Default: `default`, `analytics`
- Configurable via `ALLOWED_SCHEMAS` environment variable

### Implementation

```python
def validate_sql(self, sql: str, allowed_schemas: Set[str]) -> tuple[bool, str]:
    # Parse SQL
    parsed = sqlparse.parse(sql)
    
    # Check statement type
    stmt = parsed[0]
    if stmt.get_type() != 'SELECT':
        return False, "Only SELECT statements are allowed"
    
    # Validate schemas
    # ... schema validation logic ...
    
    return True, None
```

## Rate Limiting

### Purpose
Prevent abuse through excessive API calls.

### Configuration

```ini
# .env file
RATE_LIMIT_PER_MINUTE=60
```

### Features

- **Per-User Tracking**: Separate limits for each user
- **Sliding Window**: Uses 60-second rolling window
- **In-Memory**: Fast, efficient tracking
- **Configurable**: Adjust limits based on needs

### Implementation

```python
class RateLimiter:
    def __init__(self, max_calls_per_minute: int):
        self.max_calls = max_calls_per_minute
        self.calls = []  # (user_id, timestamp) pairs
    
    def check_rate_limit(self, user_id: str) -> tuple[bool, str]:
        # Remove old calls (>60 seconds)
        current_time = time.time()
        self.calls = [
            (uid, ts) for uid, ts in self.calls 
            if current_time - ts < 60
        ]
        
        # Count user's recent calls
        user_calls = sum(1 for uid, _ in self.calls if uid == user_id)
        
        if user_calls >= self.max_calls:
            return False, "Rate limit exceeded"
        
        self.calls.append((user_id, current_time))
        return True, None
```

## Access Control

### Purpose
Control which databases and schemas users can access.

### Schema Whitelist

Only explicitly allowed schemas can be queried:

```ini
# .env file
ALLOWED_SCHEMAS=default,analytics,reporting
```

### Token-Based Authentication

Databricks access requires valid access token:

```ini
DATABRICKS_ACCESS_TOKEN=your-token-here
```

### Benefits

- ✅ **Principle of Least Privilege**: Only necessary access granted
- ✅ **Audit Trail**: All access uses authenticated tokens
- ✅ **Revocable**: Tokens can be revoked at any time
- ✅ **Time-Limited**: Tokens expire automatically

## Audit Logging

### Purpose
Track all queries and security events for compliance and debugging.

### Logged Events

#### 1. Security Violations
- SQL injection attempts
- Dangerous keyword usage
- Rate limit violations
- Invalid column references

#### 2. Query Processing
- All user queries
- Generated SQL
- Query results (count)
- Processing time

#### 3. Errors
- Connection failures
- Query execution errors
- Validation failures

### Log Format

```python
# Example log entries
INFO - Processing query: Show me sales data
INFO - Generated SQL: SELECT * FROM sales LIMIT 100
WARNING - Potential SQL injection detected: ;\s*--
ERROR - Column invalid_col not found in table sales
INFO - Query executed successfully, returned 42 rows
```

### Log Levels

- **INFO**: Normal operations
- **WARNING**: Security concerns, but handled
- **ERROR**: Failures and rejections
- **CRITICAL**: System failures

## Best Practices

### For Users

1. **Use Natural Language**: Describe what you want, don't write SQL
2. **Be Specific**: Include table names and clear criteria
3. **Start Simple**: Test with small queries first
4. **Check Schema**: Use 'schema' command to see available tables

### For Administrators

1. **Rotate Tokens**: Change Databricks tokens regularly
2. **Monitor Logs**: Review security logs daily
3. **Adjust Rate Limits**: Based on actual usage patterns
4. **Whitelist Minimal Schemas**: Only expose necessary databases
5. **Keep Schema Updated**: Ensure schema registry matches database

### For Developers

1. **Never Bypass Validation**: Always use provided validators
2. **Log Security Events**: Comprehensive logging is critical
3. **Test Edge Cases**: Try to break your own security
4. **Update Patterns**: Add new attack patterns as discovered
5. **Review Regularly**: Security is not set-and-forget

## Security Checklist

Before deploying to production:

- [ ] Set strong `DATABRICKS_ACCESS_TOKEN`
- [ ] Configure appropriate `RATE_LIMIT_PER_MINUTE`
- [ ] Set `MAX_QUERY_LENGTH` based on needs
- [ ] Whitelist only necessary schemas in `ALLOWED_SCHEMAS`
- [ ] Enable `ENABLE_SQL_INJECTION_PROTECTION=true`
- [ ] Set `LOG_LEVEL=INFO` or higher
- [ ] Review and update dangerous keyword list
- [ ] Test all SQL injection patterns
- [ ] Set up log monitoring and alerts
- [ ] Document incident response procedures
- [ ] Plan for token rotation
- [ ] Configure backup and disaster recovery

## Threat Model

### Protected Against

✅ SQL Injection (multiple variants)
✅ Column hallucination
✅ Data modification (INSERT, UPDATE, DELETE)
✅ Schema changes (DROP, ALTER, CREATE)
✅ Query chaining
✅ Stored procedure execution
✅ Rate limiting / DoS
✅ Access to unauthorized schemas
✅ Null byte injection
✅ Excessively long queries

### Not Protected Against

⚠️ **Compromised credentials**: If token is stolen, attacker has same access
⚠️ **Social engineering**: Cannot prevent authorized users from requesting sensitive data
⚠️ **Network attacks**: DDoS, man-in-the-middle (use HTTPS and network security)
⚠️ **Application vulnerabilities**: In Databricks itself
⚠️ **Insider threats**: Authorized users with malicious intent

### Recommendations

- Use HTTPS for all connections
- Implement network security (firewalls, VPN)
- Rotate tokens frequently
- Monitor for suspicious patterns
- Implement user training
- Use principle of least privilege
- Regular security audits

## References

- [OWASP SQL Injection Prevention](https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html)
- [Databricks Security Best Practices](https://docs.databricks.com/security/index.html)
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [CWE-89: SQL Injection](https://cwe.mitre.org/data/definitions/89.html)

## Support

For security issues or questions:
- Open a confidential security issue
- Contact the security team
- Review logs for suspicious activity

**Remember**: Security is everyone's responsibility!
