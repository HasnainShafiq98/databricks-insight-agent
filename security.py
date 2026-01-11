"""
Security and protection rules for the Databricks Insight Agent.
Implements input validation, SQL injection prevention, and access control.
"""

import re
import sqlparse
from typing import List, Optional, Set
from pydantic import BaseModel, validator
import logging

logger = logging.getLogger(__name__)


class SecurityConfig(BaseModel):
    """Security configuration model."""
    max_query_length: int = 10000
    rate_limit_per_minute: int = 60
    allowed_schemas: List[str] = ["default", "analytics"]
    enable_sql_injection_protection: bool = True
    
    # SQL keywords that should not appear in user queries
    dangerous_sql_keywords: List[str] = [
        "DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE", 
        "INSERT", "UPDATE", "GRANT", "REVOKE", "EXEC", "EXECUTE"
    ]
    
    # Patterns that indicate potential SQL injection
    sql_injection_patterns: List[str] = [
        r";\s*--",  # Comment after semicolon
        r";\s*$",   # Query chaining
        r"UNION\s+SELECT",  # Union-based injection
        r"OR\s+1\s*=\s*1",  # Always true condition
        r"'\s*OR\s*'",      # Quote-based injection
        r"--\s*$",          # SQL comment at end
        r"/\*.*\*/",        # Multi-line comments
        r"xp_",             # Extended stored procedures
        r"sp_",             # System stored procedures
    ]


class SecurityValidator:
    """Validates and sanitizes user input for security."""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
        self._compiled_patterns = [
            re.compile(pattern, re.IGNORECASE) 
            for pattern in config.sql_injection_patterns
        ]
    
    def validate_query(self, query: str) -> tuple[bool, Optional[str]]:
        """
        Validate user query for security issues.
        
        Args:
            query: User input query string
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check query length
        if len(query) > self.config.max_query_length:
            return False, f"Query exceeds maximum length of {self.config.max_query_length} characters"
        
        # Check for empty query
        if not query.strip():
            return False, "Query cannot be empty"
        
        # Check for SQL injection patterns
        for pattern in self._compiled_patterns:
            if pattern.search(query):
                logger.warning(f"Potential SQL injection detected: {pattern.pattern}")
                return False, "Query contains potentially dangerous SQL patterns"
        
        # Check for dangerous SQL keywords in user input
        query_upper = query.upper()
        for keyword in self.config.dangerous_sql_keywords:
            if re.search(rf"\b{keyword}\b", query_upper):
                logger.warning(f"Dangerous SQL keyword detected: {keyword}")
                return False, f"Query contains forbidden SQL keyword: {keyword}"
        
        return True, None
    
    def validate_sql(self, sql: str, allowed_schemas: Optional[Set[str]] = None) -> tuple[bool, Optional[str]]:
        """
        Validate generated SQL before execution.
        
        Args:
            sql: SQL query to validate
            allowed_schemas: Set of allowed schema names
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if allowed_schemas is None:
            allowed_schemas = set(self.config.allowed_schemas)
        
        try:
            # Parse SQL to validate syntax
            parsed = sqlparse.parse(sql)
            if not parsed:
                return False, "Invalid SQL syntax"
            
            # Check that it's a SELECT statement
            stmt = parsed[0]
            if stmt.get_type() != 'SELECT':
                return False, "Only SELECT statements are allowed"
            
            # Extract and validate schema references
            sql_upper = sql.upper()
            for schema in allowed_schemas:
                if schema.upper() in sql_upper:
                    # Found at least one allowed schema
                    return True, None
            
            # If no schema found in allowed list, might be using default
            # This is a simplified check; more sophisticated parsing could be added
            logger.info("SQL validation passed")
            return True, None
            
        except Exception as e:
            logger.error(f"SQL validation error: {e}")
            return False, f"SQL validation failed: {str(e)}"
    
    def sanitize_input(self, user_input: str) -> str:
        """
        Sanitize user input by removing potentially dangerous characters.
        
        Args:
            user_input: Raw user input
            
        Returns:
            Sanitized input string
        """
        # Remove null bytes
        sanitized = user_input.replace('\x00', '')
        
        # Remove excessive whitespace
        sanitized = ' '.join(sanitized.split())
        
        # Limit length
        sanitized = sanitized[:self.config.max_query_length]
        
        return sanitized


class SchemaValidator:
    """Validates that SQL only references known schema elements."""
    
    def __init__(self, known_tables: dict[str, List[str]]):
        """
        Initialize schema validator.
        
        Args:
            known_tables: Dictionary mapping table names to list of column names
        """
        self.known_tables = known_tables
        self.known_columns = set()
        for columns in known_tables.values():
            self.known_columns.update(columns)
    
    def validate_columns(self, sql: str) -> tuple[bool, List[str]]:
        """
        Check if SQL references only known columns.
        
        Args:
            sql: SQL query to validate
            
        Returns:
            Tuple of (is_valid, list of unknown columns if any)
        """
        # Extract potential column names from SQL
        # This is a simplified extraction; a full SQL parser would be better
        words = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', sql)
        
        # SQL keywords to ignore
        sql_keywords = {
            'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN',
            'LIKE', 'AS', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON',
            'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT', 'OFFSET', 'COUNT',
            'SUM', 'AVG', 'MIN', 'MAX', 'DISTINCT', 'NULL', 'IS', 'TRUE', 'FALSE'
        }
        
        unknown_columns = []
        for word in words:
            word_upper = word.upper()
            if word_upper not in sql_keywords:
                # Check if it's a known table or column
                if word not in self.known_tables and word not in self.known_columns:
                    unknown_columns.append(word)
        
        if unknown_columns:
            logger.warning(f"Unknown columns detected: {unknown_columns}")
            return False, unknown_columns
        
        return True, []
    
    def get_table_columns(self, table_name: str) -> Optional[List[str]]:
        """Get list of columns for a given table."""
        return self.known_tables.get(table_name)


class RateLimiter:
    """Simple in-memory rate limiter for API calls."""
    
    def __init__(self, max_calls_per_minute: int):
        self.max_calls = max_calls_per_minute
        self.calls = []
    
    def check_rate_limit(self, user_id: str = "default") -> tuple[bool, Optional[str]]:
        """
        Check if the user is within rate limits.
        
        Args:
            user_id: Identifier for the user
            
        Returns:
            Tuple of (is_allowed, error_message)
        """
        import time
        current_time = time.time()
        
        # Remove calls older than 1 minute
        self.calls = [
            (uid, timestamp) 
            for uid, timestamp in self.calls 
            if current_time - timestamp < 60
        ]
        
        # Count calls for this user in the last minute
        user_calls = sum(1 for uid, _ in self.calls if uid == user_id)
        
        if user_calls >= self.max_calls:
            return False, f"Rate limit exceeded. Maximum {self.max_calls} calls per minute."
        
        # Record this call
        self.calls.append((user_id, current_time))
        return True, None
