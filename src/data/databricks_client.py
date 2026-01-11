"""
Databricks client for executing queries and managing connections.
"""

import logging
from typing import List, Dict, Any, Optional
from databricks import sql
import os

logger = logging.getLogger(__name__)


class DatabricksClient:
    """Client for interacting with Databricks SQL warehouse."""
    
    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: str
    ):
        """
        Initialize Databricks client.
        
        Args:
            server_hostname: Databricks workspace hostname
            http_path: SQL warehouse HTTP path
            access_token: Personal access token for authentication
        """
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.connection = None
    
    def connect(self):
        """Establish connection to Databricks."""
        try:
            self.connection = sql.connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self.access_token
            )
            logger.info("Successfully connected to Databricks")
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {e}")
            raise
    
    def disconnect(self):
        """Close the Databricks connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Databricks")
    
    def execute_query(self, sql_query: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results.
        
        Args:
            sql_query: SQL query to execute
            
        Returns:
            List of dictionaries representing rows
        """
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql_query)
            
            # Fetch column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            results = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = row[i]
                results.append(row_dict)
            
            cursor.close()
            logger.info(f"Query executed successfully, returned {len(results)} rows")
            return results
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def get_table_schema(self, table_name: str, catalog: str = "hive_metastore", schema: str = "default") -> Dict[str, str]:
        """
        Get the schema (columns and types) for a table.
        
        Args:
            table_name: Name of the table
            catalog: Catalog name (default: hive_metastore)
            schema: Schema name (default: default)
            
        Returns:
            Dictionary mapping column names to types
        """
        query = f"DESCRIBE {catalog}.{schema}.{table_name}"
        try:
            results = self.execute_query(query)
            schema_dict = {}
            for row in results:
                # DESCRIBE returns col_name, data_type, comment
                col_name = row.get('col_name', row.get('column_name'))
                data_type = row.get('data_type', row.get('type'))
                if col_name and data_type:
                    schema_dict[col_name] = data_type
            return schema_dict
        except Exception as e:
            logger.error(f"Error getting table schema: {e}")
            return {}
    
    def list_tables(self, catalog: str = "hive_metastore", schema: str = "default") -> List[str]:
        """
        List all tables in a schema.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            
        Returns:
            List of table names
        """
        query = f"SHOW TABLES IN {catalog}.{schema}"
        try:
            results = self.execute_query(query)
            tables = [row.get('tableName', row.get('table_name', '')) for row in results]
            return [t for t in tables if t]
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []
    
    def test_connection(self) -> bool:
        """
        Test the Databricks connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.connect()
            # Simple test query
            self.execute_query("SELECT 1 as test")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
        finally:
            self.disconnect()
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
