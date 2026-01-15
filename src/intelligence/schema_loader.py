"""
Schema auto-detection module for Databricks tables.
Automatically loads table schemas from Databricks without manual definition.
"""

import logging
from typing import List, Optional, Dict
from .sql_generator import TableSchema, SchemaManager

logger = logging.getLogger(__name__)


class SchemaLoader:
    """
    Automatically loads table schemas from Databricks.
    """
    
    def __init__(self, databricks_client):
        """
        Initialize schema loader.
        
        Args:
            databricks_client: DatabricksClient instance
        """
        self.databricks_client = databricks_client
    
    def load_table_schema(
        self, 
        table_name: str,
        catalog: str = "hive_metastore",
        schema: str = "default"
    ) -> Optional[TableSchema]:
        """
        Load schema for a specific table from Databricks.
        
        Args:
            table_name: Name of the table
            catalog: Catalog name (default: hive_metastore)
            schema: Schema/database name (default: default)
            
        Returns:
            TableSchema object or None if table doesn't exist
        """
        try:
            full_table_name = f"{catalog}.{schema}.{table_name}"
            
            # Query to get column information
            describe_query = f"DESCRIBE TABLE {full_table_name}"
            
            logger.info(f"Loading schema for table: {full_table_name}")
            result = self.databricks_client.execute_query(describe_query)
            
            if not result:
                logger.warning(f"No schema information found for table: {full_table_name}")
                return None
            
            columns = []
            column_types = {}
            
            # Parse DESCRIBE TABLE results
            for row in result:
                # Skip partition information and metadata rows
                if row.get('col_name', '').startswith('#') or row.get('col_name', '') == '':
                    continue
                    
                col_name = row.get('col_name', '').strip()
                col_type = row.get('data_type', 'STRING').strip().upper()
                
                # Skip empty rows and comment rows
                if not col_name or col_name.lower() in ['', 'col_name']:
                    continue
                
                columns.append(col_name)
                column_types[col_name] = col_type
            
            if not columns:
                logger.warning(f"No columns found for table: {full_table_name}")
                return None
            
            # Get table comment/description if available
            try:
                comment_query = f"SHOW TBLPROPERTIES {full_table_name}"
                comment_result = self.databricks_client.execute_query(comment_query)
                description = None
                for row in comment_result:
                    if row.get('key') == 'comment':
                        description = row.get('value')
                        break
            except Exception as e:
                logger.debug(f"Could not retrieve table comment: {e}")
                description = f"Auto-detected schema for {table_name}"
            
            table_schema = TableSchema(
                name=table_name,
                columns=columns,
                column_types=column_types,
                description=description or f"Auto-detected schema for {table_name}"
            )
            
            logger.info(f"✅ Successfully loaded schema for {table_name} ({len(columns)} columns)")
            return table_schema
            
        except Exception as e:
            logger.error(f"Failed to load schema for {table_name}: {e}")
            return None
    
    def load_all_tables(
        self,
        catalog: str = "hive_metastore",
        schema: str = "default",
        table_filter: Optional[List[str]] = None
    ) -> List[TableSchema]:
        """
        Load schemas for all tables in a schema/database.
        
        Args:
            catalog: Catalog name
            schema: Schema/database name
            table_filter: Optional list of specific table names to load
            
        Returns:
            List of TableSchema objects
        """
        try:
            # Get list of all tables
            show_tables_query = f"SHOW TABLES IN {catalog}.{schema}"
            logger.info(f"Discovering tables in {catalog}.{schema}")
            
            tables_result = self.databricks_client.execute_query(show_tables_query)
            
            if not tables_result:
                logger.warning(f"No tables found in {catalog}.{schema}")
                return []
            
            table_schemas = []
            
            for row in tables_result:
                table_name = row.get('tableName') or row.get('name')
                
                if not table_name:
                    continue
                
                # Apply filter if specified
                if table_filter and table_name not in table_filter:
                    continue
                
                # Load schema for this table
                table_schema = self.load_table_schema(table_name, catalog, schema)
                
                if table_schema:
                    table_schemas.append(table_schema)
            
            logger.info(f"✅ Loaded {len(table_schemas)} table schemas from {catalog}.{schema}")
            return table_schemas
            
        except Exception as e:
            logger.error(f"Failed to load tables from {catalog}.{schema}: {e}")
            return []
    
    def auto_populate_schema_manager(
        self,
        schema_manager: SchemaManager,
        catalog: str = "hive_metastore",
        schema: str = "default",
        table_filter: Optional[List[str]] = None
    ) -> int:
        """
        Automatically populate a SchemaManager with all tables from Databricks.
        
        Args:
            schema_manager: SchemaManager instance to populate
            catalog: Catalog name
            schema: Schema/database name
            table_filter: Optional list of specific table names to load
            
        Returns:
            Number of tables successfully loaded
        """
        table_schemas = self.load_all_tables(catalog, schema, table_filter)
        
        count = 0
        for table_schema in table_schemas:
            schema_manager.add_table(table_schema)
            count += 1
        
        logger.info(f"✅ Auto-populated schema manager with {count} tables")
        return count


def create_schema_manager_from_databricks(
    databricks_client,
    catalog: str = "hive_metastore",
    schema: str = "default",
    specific_tables: Optional[List[str]] = None,
    fallback_to_sample: bool = True
) -> SchemaManager:
    """
    Create and populate a SchemaManager from Databricks.
    
    Args:
        databricks_client: DatabricksClient instance
        catalog: Catalog name
        schema: Schema/database name
        specific_tables: Optional list of specific table names to load
        fallback_to_sample: If True, use sample schema if Databricks connection fails
        
    Returns:
        Populated SchemaManager instance
    """
    schema_manager = SchemaManager()
    
    if databricks_client is None:
        logger.warning("No Databricks client provided")
        if fallback_to_sample:
            logger.info("Using sample schema data")
            _add_sample_schemas(schema_manager)
        return schema_manager
    
    try:
        loader = SchemaLoader(databricks_client)
        count = loader.auto_populate_schema_manager(
            schema_manager,
            catalog=catalog,
            schema=schema,
            table_filter=specific_tables
        )
        
        if count == 0 and fallback_to_sample:
            logger.warning("No tables loaded from Databricks, using sample schema")
            _add_sample_schemas(schema_manager)
        
    except Exception as e:
        logger.error(f"Error loading schema from Databricks: {e}")
        if fallback_to_sample:
            logger.info("Falling back to sample schema")
            _add_sample_schemas(schema_manager)
    
    return schema_manager


def _add_sample_schemas(schema_manager: SchemaManager):
    """Add sample table schemas for demonstration."""
    
    # Sample sales table
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
        description="Sample sales transaction data"
    )
    
    # Sample customers table
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
        description="Sample customer information"
    )
    
    # Sample products table
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
        description="Sample product catalog"
    )
    
    schema_manager.add_table(sales_table)
    schema_manager.add_table(customers_table)
    schema_manager.add_table(products_table)
    
    logger.info("✅ Added 3 sample table schemas")
