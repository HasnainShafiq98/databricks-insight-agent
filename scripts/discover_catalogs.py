"""
Discover available catalogs and schemas in your Databricks workspace.
Use this to find the correct catalog name for Unity Catalog.
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from src.data.databricks_client import DatabricksClient
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def main():
    """Discover catalogs and schemas."""
    
    print("=" * 80)
    print("🔍 DATABRICKS CATALOG & SCHEMA DISCOVERY")
    print("=" * 80)
    
    load_dotenv()
    
    server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
    http_path = os.getenv('DATABRICKS_HTTP_PATH')
    access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
    
    if not all([server_hostname, http_path, access_token]):
        print("\n❌ Missing Databricks credentials in .env file")
        return
    
    print(f"\n📡 Connecting to: {server_hostname}")
    
    try:
        client = DatabricksClient(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )
        
        print("✅ Connected!\n")
        
        # Discover catalogs
        print("=" * 80)
        print("STEP 1: Discovering Catalogs")
        print("=" * 80)
        
        try:
            catalogs = client.execute_query("SHOW CATALOGS")
            
            if catalogs:
                print(f"\n✅ Found {len(catalogs)} catalog(s):\n")
                for i, cat in enumerate(catalogs, 1):
                    catalog_name = cat.get('catalog') or cat.get('name') or cat.get('namespace')
                    print(f"   {i}. {catalog_name}")
                    
                # Try to get schemas from each catalog
                print("\n" + "=" * 80)
                print("STEP 2: Discovering Schemas in Each Catalog")
                print("=" * 80)
                
                for cat in catalogs:
                    catalog_name = cat.get('catalog') or cat.get('name') or cat.get('namespace')
                    print(f"\n📁 Catalog: {catalog_name}")
                    print("-" * 40)
                    
                    try:
                        schemas = client.execute_query(f"SHOW SCHEMAS IN {catalog_name}")
                        if schemas:
                            for schema in schemas[:10]:  # Show first 10
                                schema_name = schema.get('databaseName') or schema.get('namespace')
                                print(f"   └─ {schema_name}")
                            if len(schemas) > 10:
                                print(f"   ... and {len(schemas) - 10} more schemas")
                    except Exception as e:
                        print(f"   ⚠️  Could not list schemas: {str(e)[:100]}")
                
                # Try to find tables in main catalog
                print("\n" + "=" * 80)
                print("STEP 3: Looking for Tables")
                print("=" * 80)
                
                for cat in catalogs:
                    catalog_name = cat.get('catalog') or cat.get('name') or cat.get('namespace')
                    
                    # Common schema names to try
                    common_schemas = ['default', 'main', 'workspace']
                    
                    for schema_name in common_schemas:
                        try:
                            print(f"\n🔍 Checking {catalog_name}.{schema_name}...")
                            tables = client.execute_query(f"SHOW TABLES IN {catalog_name}.{schema_name}")
                            
                            if tables:
                                print(f"✅ Found {len(tables)} table(s):")
                                for table in tables[:10]:
                                    table_name = table.get('tableName') or table.get('name')
                                    print(f"   • {table_name}")
                                if len(tables) > 10:
                                    print(f"   ... and {len(tables) - 10} more tables")
                                
                                print(f"\n🎯 USE THIS CONFIGURATION:")
                                print(f"   CATALOG={catalog_name}")
                                print(f"   SCHEMA={schema_name}")
                                return
                        except:
                            continue
                
                print("\n⚠️  No tables found in common schemas")
                print("   Please check where you created your 'superstore' table")
                
            else:
                print("\n⚠️  No catalogs found - your workspace might use a different setup")
                
        except Exception as e:
            print(f"\n❌ Error discovering catalogs: {e}")
            print("\n💡 Trying alternative discovery method...")
            
            # Try SELECT CURRENT_CATALOG() and CURRENT_SCHEMA()
            try:
                current = client.execute_query("SELECT CURRENT_CATALOG() as catalog, CURRENT_SCHEMA() as schema")
                if current:
                    cat = current[0].get('catalog')
                    sch = current[0].get('schema')
                    print(f"\n✅ Current session settings:")
                    print(f"   Catalog: {cat}")
                    print(f"   Schema: {sch}")
                    
                    print(f"\n🎯 Try using:")
                    print(f"   CATALOG={cat}")
                    print(f"   SCHEMA={sch}")
            except Exception as e2:
                print(f"❌ {e2}")
        
    except Exception as e:
        print(f"\n❌ Connection error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
