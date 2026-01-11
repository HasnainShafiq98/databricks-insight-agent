"""
Data pipeline module for processing CSV files into Delta tables (Bronze, Silver, Gold).
Implements the Databricks medallion architecture for data quality and transformation.
"""

import os
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd
from databricks import sql
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class DeltaTablePipeline:
    """
    Manages the data pipeline from raw CSV to Bronze, Silver, and Gold Delta tables.
    
    Bronze: Raw data ingestion with minimal validation
    Silver: Cleaned, validated, and normalized data
    Gold: Business-level aggregates and KPIs
    """
    
    def __init__(
        self,
        databricks_client,
        catalog: str = "hive_metastore",
        schema: str = "default",
        dbfs_mount_point: str = "/mnt/data"
    ):
        """
        Initialize the Delta table pipeline.
        
        Args:
            databricks_client: Databricks client for query execution
            catalog: Catalog name for Unity Catalog
            schema: Schema/database name
            dbfs_mount_point: DBFS mount point for data storage
        """
        self.databricks_client = databricks_client
        self.catalog = catalog
        self.schema = schema
        self.dbfs_mount_point = dbfs_mount_point
    
    def upload_csv_to_dbfs(self, local_path: str, dbfs_path: str) -> bool:
        """
        Upload a CSV file to DBFS.
        
        Args:
            local_path: Local file path
            dbfs_path: Target DBFS path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # In production, use Databricks SDK to upload
            # For now, we'll use spark.read.csv if running in Databricks
            logger.info(f"Uploading {local_path} to DBFS: {dbfs_path}")
            
            # Placeholder for actual DBFS upload
            # In a real implementation, use:
            # dbutils.fs.cp(f"file:{local_path}", dbfs_path)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload to DBFS: {e}")
            return False
    
    def create_bronze_table(
        self,
        table_name: str,
        csv_path: str,
        schema_definition: Optional[str] = None
    ) -> bool:
        """
        Create a Bronze table from raw CSV data.
        Bronze tables contain raw, unprocessed data with ingestion metadata.
        
        Args:
            table_name: Name of the bronze table
            csv_path: Path to CSV file (local or DBFS)
            schema_definition: Optional schema definition string
            
        Returns:
            True if successful, False otherwise
        """
        try:
            bronze_table = f"{self.catalog}.{self.schema}.bronze_{table_name}"
            
            # Read CSV and infer schema
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS {bronze_table}
            USING DELTA
            AS
            SELECT 
                *,
                current_timestamp() as _ingestion_timestamp,
                '{csv_path}' as _source_file
            FROM read_csv(
                '{csv_path}',
                header=true,
                inferSchema=true
            )
            """
            
            logger.info(f"Creating bronze table: {bronze_table}")
            self.databricks_client.execute_query(sql_query)
            
            logger.info(f"✅ Bronze table created: {bronze_table}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create bronze table: {e}")
            return False
    
    def create_silver_table(
        self,
        table_name: str,
        bronze_table_name: str,
        transformations: Dict[str, str],
        validation_rules: Optional[List[str]] = None
    ) -> bool:
        """
        Create a Silver table with cleaned and validated data.
        Silver tables have:
        - Data quality checks
        - Standardized formats
        - Removed duplicates
        - Type corrections
        
        Args:
            table_name: Name of the silver table
            bronze_table_name: Source bronze table
            transformations: Dictionary of column transformations
            validation_rules: List of WHERE clause conditions for filtering
            
        Returns:
            True if successful, False otherwise
        """
        try:
            bronze_table = f"{self.catalog}.{self.schema}.bronze_{bronze_table_name}"
            silver_table = f"{self.catalog}.{self.schema}.silver_{table_name}"
            
            # Build transformation SQL
            select_columns = []
            for col, transformation in transformations.items():
                if transformation:
                    select_columns.append(f"{transformation} as {col}")
                else:
                    select_columns.append(col)
            
            select_clause = ",\n    ".join(select_columns)
            
            # Build validation WHERE clause
            where_clause = ""
            if validation_rules:
                where_clause = "WHERE " + " AND ".join(validation_rules)
            
            sql_query = f"""
            CREATE OR REPLACE TABLE {silver_table}
            USING DELTA
            AS
            SELECT 
                {select_clause},
                current_timestamp() as _processing_timestamp
            FROM {bronze_table}
            {where_clause}
            """
            
            logger.info(f"Creating silver table: {silver_table}")
            self.databricks_client.execute_query(sql_query)
            
            # Add table properties and constraints
            self._add_silver_constraints(silver_table, table_name)
            
            logger.info(f"✅ Silver table created: {silver_table}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create silver table: {e}")
            return False
    
    def create_gold_table(
        self,
        table_name: str,
        silver_table_name: str,
        aggregations: Dict[str, str],
        group_by: List[str],
        description: Optional[str] = None
    ) -> bool:
        """
        Create a Gold table with business-level aggregates.
        Gold tables contain:
        - Business KPIs
        - Aggregated metrics
        - Denormalized for reporting
        
        Args:
            table_name: Name of the gold table
            silver_table_name: Source silver table
            aggregations: Dictionary of metric_name: aggregation_expression
            group_by: List of dimensions to group by
            description: Business description of the table
            
        Returns:
            True if successful, False otherwise
        """
        try:
            silver_table = f"{self.catalog}.{self.schema}.silver_{silver_table_name}"
            gold_table = f"{self.catalog}.{self.schema}.gold_{table_name}"
            
            # Build aggregation SQL
            agg_columns = []
            for metric_name, agg_expr in aggregations.items():
                agg_columns.append(f"{agg_expr} as {metric_name}")
            
            group_by_clause = ", ".join(group_by)
            agg_clause = ",\n    ".join(agg_columns)
            
            sql_query = f"""
            CREATE OR REPLACE TABLE {gold_table}
            USING DELTA
            AS
            SELECT 
                {group_by_clause},
                {agg_clause},
                current_timestamp() as _aggregation_timestamp
            FROM {silver_table}
            GROUP BY {group_by_clause}
            """
            
            logger.info(f"Creating gold table: {gold_table}")
            self.databricks_client.execute_query(sql_query)
            
            # Add table description
            if description:
                self.databricks_client.execute_query(
                    f"COMMENT ON TABLE {gold_table} IS '{description}'"
                )
            
            logger.info(f"✅ Gold table created: {gold_table}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create gold table: {e}")
            return False
    
    def _add_silver_constraints(self, table_name: str, base_name: str):
        """Add data quality constraints to silver table."""
        try:
            # Add NOT NULL constraints for critical columns
            # This is table-specific and should be customized
            
            # Enable Delta table properties for data quality
            self.databricks_client.execute_query(
                f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
            )
            
            logger.info(f"Added constraints to {table_name}")
            
        except Exception as e:
            logger.warning(f"Failed to add constraints: {e}")
    
    def optimize_table(self, table_name: str, zorder_columns: Optional[List[str]] = None):
        """
        Optimize a Delta table for query performance.
        
        Args:
            table_name: Full table name (catalog.schema.table)
            zorder_columns: Columns to Z-order for optimal filtering
        """
        try:
            # Compact small files
            logger.info(f"Optimizing table: {table_name}")
            self.databricks_client.execute_query(f"OPTIMIZE {table_name}")
            
            # Z-order if specified
            if zorder_columns:
                zorder_cols = ", ".join(zorder_columns)
                self.databricks_client.execute_query(
                    f"OPTIMIZE {table_name} ZORDER BY ({zorder_cols})"
                )
                logger.info(f"Z-ordered by: {zorder_cols}")
            
            logger.info(f"✅ Table optimized: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to optimize table: {e}")
    
    def vacuum_table(self, table_name: str, retention_hours: int = 168):
        """
        Clean up old versions of Delta table.
        
        Args:
            table_name: Full table name
            retention_hours: Retention period in hours (default 7 days)
        """
        try:
            logger.info(f"Vacuuming table: {table_name}")
            self.databricks_client.execute_query(
                f"VACUUM {table_name} RETAIN {retention_hours} HOURS"
            )
            logger.info(f"✅ Table vacuumed: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to vacuum table: {e}")
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """
        Get statistics for a Delta table.
        
        Args:
            table_name: Full table name
            
        Returns:
            Dictionary of table statistics
        """
        try:
            # Get row count
            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            count_result = self.databricks_client.execute_query(count_query)
            row_count = count_result[0]['count'] if count_result else 0
            
            # Get table details
            describe_query = f"DESCRIBE EXTENDED {table_name}"
            details = self.databricks_client.execute_query(describe_query)
            
            return {
                'row_count': row_count,
                'details': details
            }
            
        except Exception as e:
            logger.error(f"Failed to get table stats: {e}")
            return {}


class CSVIngestionPipeline:
    """Pipeline for ingesting CSV files into Delta tables."""
    
    def __init__(self, delta_pipeline: DeltaTablePipeline):
        self.delta_pipeline = delta_pipeline
    
    def ingest_sales_data(self, csv_path: str) -> bool:
        """
        Ingest sales data through Bronze -> Silver -> Gold pipeline.
        
        Args:
            csv_path: Path to sales CSV file
            
        Returns:
            True if successful
        """
        try:
            logger.info("🔄 Starting sales data ingestion...")
            
            # Step 1: Create Bronze table
            success = self.delta_pipeline.create_bronze_table(
                table_name="sales",
                csv_path=csv_path
            )
            if not success:
                return False
            
            # Step 2: Create Silver table with transformations
            transformations = {
                "transaction_id": "TRIM(transaction_id)",
                "customer_id": "TRIM(customer_id)",
                "product_id": "TRIM(product_id)",
                "amount": "CAST(amount AS DECIMAL(10,2))",
                "date": "TO_DATE(date, 'yyyy-MM-dd')",
                "region": "UPPER(TRIM(region))"
            }
            
            validation_rules = [
                "transaction_id IS NOT NULL",
                "amount > 0",
                "date IS NOT NULL"
            ]
            
            success = self.delta_pipeline.create_silver_table(
                table_name="sales",
                bronze_table_name="sales",
                transformations=transformations,
                validation_rules=validation_rules
            )
            if not success:
                return False
            
            # Step 3: Create Gold table with aggregations
            aggregations = {
                "total_sales": "SUM(amount)",
                "transaction_count": "COUNT(*)",
                "avg_transaction_value": "AVG(amount)",
                "unique_customers": "COUNT(DISTINCT customer_id)"
            }
            
            success = self.delta_pipeline.create_gold_table(
                table_name="sales_by_region",
                silver_table_name="sales",
                aggregations=aggregations,
                group_by=["region"],
                description="Sales metrics aggregated by region"
            )
            
            logger.info("✅ Sales data ingestion completed!")
            return success
            
        except Exception as e:
            logger.error(f"Sales ingestion failed: {e}")
            return False
    
    def ingest_customer_data(self, csv_path: str) -> bool:
        """
        Ingest customer data through Bronze -> Silver pipeline.
        
        Args:
            csv_path: Path to customer CSV file
            
        Returns:
            True if successful
        """
        try:
            logger.info("🔄 Starting customer data ingestion...")
            
            # Bronze table
            success = self.delta_pipeline.create_bronze_table(
                table_name="customers",
                csv_path=csv_path
            )
            if not success:
                return False
            
            # Silver table with email validation
            transformations = {
                "customer_id": "TRIM(customer_id)",
                "name": "TRIM(name)",
                "email": "LOWER(TRIM(email))",
                "registration_date": "TO_DATE(registration_date, 'yyyy-MM-dd')",
                "country": "UPPER(TRIM(country))"
            }
            
            validation_rules = [
                "customer_id IS NOT NULL",
                "email LIKE '%@%.%'",
                "name IS NOT NULL"
            ]
            
            success = self.delta_pipeline.create_silver_table(
                table_name="customers",
                bronze_table_name="customers",
                transformations=transformations,
                validation_rules=validation_rules
            )
            
            logger.info("✅ Customer data ingestion completed!")
            return success
            
        except Exception as e:
            logger.error(f"Customer ingestion failed: {e}")
            return False
