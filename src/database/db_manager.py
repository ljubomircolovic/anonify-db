import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os

class DBManager:
    """
    Handles database connections and operations for reading source data 
    and writing anonymized results back to PostgreSQL.
    """
    def __init__(self, db_url=None):
        # Use environment variable or fallback to default Docker Compose settings
        self.db_url = db_url or os.getenv(
            "DATABASE_URL", 
            "postgresql://user:password@db:5432/anonify_db"
        )
        self.engine = create_engine(self.db_url)

    def get_all_schemas(self):
        """Returns a list of all available schemas in the database."""
        inspector = inspect(self.engine)
        return inspector.get_schema_names()

    def get_tables_in_schema(self, schema='public'):
        """Returns a list of all table names within a specific schema."""
        inspector = inspect(self.engine)
        return inspector.get_table_names(schema=schema)

    def read_table(self, table_name, schema='public'):
        """Reads a database table into a Pandas DataFrame for processing."""
        query = f"SELECT * FROM {schema}.{table_name}"
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            print(f"Error reading table {schema}.{table_name}: {e}")
            return None

    def save_anonymized_table(self, df, table_name, target_schema='anon'):
        """
        Creates the target schema if it doesn't exist and writes 
        the anonymized DataFrame to a new table.
        """
        try:
            with self.engine.connect() as conn:
                # Ensure the target schema exists before writing
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))
                conn.commit()
            
            # Write data to the new schema
            df.to_sql(
                table_name, 
                self.engine, 
                schema=target_schema, 
                if_exists='replace', 
                index=False
            )
            return True
        except Exception as e:
            print(f"Error saving to {target_schema}.{table_name}: {e}")
            return False