import pandas as pd
from sqlalchemy import create_engine, text
import os

# Use the same connection string from your DBManager
db_url = "postgresql://user:password@localhost:5433/anonify_db"
engine = create_engine(db_url)

def setup_data():
    # 1. Create a Schema for our "Production" data
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS production"))
        conn.commit()
    
    # 2. Generate some dummy PII data
    data = {
        'employee_id': [1, 2, 3],
        'full_name': ['Ljubomir Colovic', 'John Doe', 'Jane Smith'],
        'email': ['ljubomir@example.com', 'john@doe.com', 'jane@smith.com'],
        'salary': [120000, 95000, 105000],
        'department': ['Engineering', 'Sales', 'HR']
    }
    df = pd.DataFrame(data)
    
    # 3. Write to the database
    df.to_sql('employees', engine, schema='production', if_exists='replace', index=False)
    print("Success: Schema 'production' and table 'employees' created!")

if __name__ == "__main__":
    setup_data()