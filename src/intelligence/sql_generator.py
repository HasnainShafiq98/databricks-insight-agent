"""
SQL generation module for the Databricks Insight Agent.
Generates safe SQL queries from known schema without hallucinating columns.
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TableSchema:
    """Represents a table schema."""
    name: str
    columns: List[str]
    column_types: Dict[str, str]
    description: Optional[str] = None


class SchemaManager:
    """Manages database schema information."""
    
    def __init__(self):
        self.tables: Dict[str, TableSchema] = {}
    
    def add_table(self, table: TableSchema):
        """Add a table schema."""
        self.tables[table.name] = table
        logger.info(f"Added table schema: {table.name}")
    
    def get_table(self, table_name: str) -> Optional[TableSchema]:
        """Get table schema by name."""
        return self.tables.get(table_name)
    
    def get_all_tables(self) -> List[str]:
        """Get list of all table names."""
        return list(self.tables.keys())
    
    def get_table_columns(self, table_name: str) -> Optional[List[str]]:
        """Get columns for a specific table."""
        table = self.get_table(table_name)
        return table.columns if table else None
    
    def column_exists(self, table_name: str, column_name: str) -> bool:
        """Check if a column exists in a table."""
        table = self.get_table(table_name)
        if not table:
            return False
        return column_name in table.columns
    
    def get_schema_summary(self) -> str:
        """Get a human-readable summary of the schema."""
        if not self.tables:
            return "No tables in schema"
        
        summary = "Available tables and columns:\n"
        for table_name, table in self.tables.items():
            summary += f"\nTable: {table_name}\n"
            if table.description:
                summary += f"  Description: {table.description}\n"
            summary += "  Columns:\n"
            for col in table.columns:
                col_type = table.column_types.get(col, "unknown")
                summary += f"    - {col} ({col_type})\n"
        return summary


class SQLGenerator:
    """Generates safe SQL queries from user intent and schema."""
    
    def __init__(self, schema_manager: SchemaManager):
        self.schema_manager = schema_manager
    
    def generate_sql(
        self, 
        table_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        aggregations: Optional[Dict[str, str]] = None,
        group_by: Optional[List[str]] = None,
        order_by: Optional[List[tuple[str, str]]] = None,
        limit: Optional[int] = None
    ) -> Optional[str]:
        """
        Generate a safe SQL query based on provided parameters.
        
        Args:
            table_name: Name of the table to query
            columns: List of columns to select (None = all columns)
            filters: Dictionary of column: value filters
            aggregations: Dictionary of column: aggregation_function
            group_by: List of columns to group by
            order_by: List of (column, direction) tuples
            limit: Maximum number of rows to return
            
        Returns:
            SQL query string or None if invalid
        """
        # Validate table exists
        table = self.schema_manager.get_table(table_name)
        if not table:
            logger.error(f"Table {table_name} not found in schema")
            return None
        
        # Validate and prepare columns
        if columns:
            # Verify all requested columns exist
            for col in columns:
                if not self.schema_manager.column_exists(table_name, col):
                    logger.error(f"Column {col} not found in table {table_name}")
                    return None
            select_cols = columns
        else:
            select_cols = table.columns
        
        # Build SELECT clause
        select_parts = []
        if aggregations:
            for col, agg_func in aggregations.items():
                if not self.schema_manager.column_exists(table_name, col):
                    logger.error(f"Column {col} not found in table {table_name}")
                    return None
                
                # Validate aggregation function
                valid_agg_funcs = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
                if agg_func.upper() not in valid_agg_funcs:
                    logger.error(f"Invalid aggregation function: {agg_func}")
                    return None
                
                select_parts.append(f"{agg_func.upper()}({col}) as {col}_{agg_func.lower()}")
        else:
            select_parts = select_cols
        
        sql = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        
        # Build WHERE clause
        if filters:
            where_parts = []
            for col, value in filters.items():
                if not self.schema_manager.column_exists(table_name, col):
                    logger.error(f"Column {col} not found in table {table_name}")
                    return None
                
                # Handle different value types
                if isinstance(value, str):
                    # Escape single quotes in string values
                    escaped_value = value.replace("'", "''")
                    where_parts.append(f"{col} = '{escaped_value}'")
                elif isinstance(value, (int, float)):
                    where_parts.append(f"{col} = {value}")
                elif isinstance(value, list):
                    # IN clause
                    if all(isinstance(v, str) for v in value):
                        values_str = ', '.join([f"'{v.replace(chr(39), chr(39)+chr(39))}'" for v in value])
                    else:
                        values_str = ', '.join([str(v) for v in value])
                    where_parts.append(f"{col} IN ({values_str})")
                elif value is None:
                    where_parts.append(f"{col} IS NULL")
            
            if where_parts:
                sql += " WHERE " + " AND ".join(where_parts)
        
        # Build GROUP BY clause
        if group_by:
            for col in group_by:
                if not self.schema_manager.column_exists(table_name, col):
                    logger.error(f"Column {col} not found in table {table_name}")
                    return None
            sql += f" GROUP BY {', '.join(group_by)}"
        
        # Build ORDER BY clause
        if order_by:
            order_parts = []
            for col, direction in order_by:
                if not self.schema_manager.column_exists(table_name, col):
                    logger.error(f"Column {col} not found in table {table_name}")
                    return None
                
                direction = direction.upper()
                if direction not in ['ASC', 'DESC']:
                    logger.error(f"Invalid order direction: {direction}")
                    return None
                
                order_parts.append(f"{col} {direction}")
            
            if order_parts:
                sql += f" ORDER BY {', '.join(order_parts)}"
        
        # Add LIMIT clause
        if limit:
            if not isinstance(limit, int) or limit <= 0:
                logger.error(f"Invalid limit value: {limit}")
                return None
            sql += f" LIMIT {limit}"
        
        logger.info(f"Generated SQL: {sql}")
        return sql
    
    def parse_query_intent(self, user_query: str, context: Optional[str] = None) -> Dict[str, Any]:
        """
        Parse user query to extract SQL generation parameters.
        This is a simplified version. In production, use an LLM for better understanding.
        
        Args:
            user_query: Natural language query from user
            context: Additional context from FAISS retrieval
            
        Returns:
            Dictionary with SQL generation parameters
        """
        intent = {
            'table_name': None,
            'columns': None,
            'filters': {},
            'aggregations': None,
            'group_by': None,
            'order_by': None,
            'limit': None
        }
        
        query_lower = user_query.lower()
        
        # Detect table name from query and available tables
        for table_name in self.schema_manager.get_all_tables():
            if table_name.lower() in query_lower:
                intent['table_name'] = table_name
                break
        
        # Detect aggregations
        if any(word in query_lower for word in ['count', 'total', 'sum', 'average', 'avg']):
            intent['aggregations'] = {}
            if 'count' in query_lower or 'total' in query_lower:
                # Default to counting all
                intent['aggregations']['*'] = 'COUNT'
        
        # Detect ordering
        if 'top' in query_lower or 'highest' in query_lower or 'largest' in query_lower:
            intent['order_by'] = []
            intent['order_by'].append(('value', 'DESC'))  # Generic, needs refinement
        
        # Detect limit
        import re
        limit_match = re.search(r'(?:top|first|limit)\s+(\d+)', query_lower)
        if limit_match:
            intent['limit'] = int(limit_match.group(1))
        elif 'top' in query_lower and not limit_match:
            intent['limit'] = 10  # Default top N
        
        return intent
