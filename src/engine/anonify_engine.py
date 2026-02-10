import yaml
import os
from faker import Faker
from unidecode import unidecode

# Initialize paths for configuration
current_dir = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(current_dir, '../../config/settings.yaml')

def load_config():
    """Load application settings from the YAML configuration file."""
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

config = load_config()

def get_deterministic_name(user_id):
    """
    Generate a consistent fake name based on user_id.
    Ensures the same input ID always results in the same fake name.
    """
    locale = config['anonymization']['locale']
    local_fake = Faker(locale)
    local_fake.seed_instance(user_id)

    raw_name = local_fake.name()

    # Convert to ASCII if configured to avoid encoding issues
    if config['anonymization'].get('use_ascii', True):
        return unidecode(raw_name)
    return raw_name

def get_salary_bucket(salary_str):
    """Categorize exact salary strings into predefined ranges for analytical privacy."""
    try:
        # Extract digits from string (e.g., '120k' -> 120)
        amount = int(''.join(filter(str.isdigit, salary_str)))

        if amount < 50: return "< 50k"
        if amount < 100: return "50k - 100k"
        if amount < 150: return "100k - 150k"
        return "150k+"
    except (ValueError, TypeError):
        return "[INVALID DATA]"

def process_data(conn, setup_target_table):
    """
    Core transformation engine. Maps source columns to target columns
    using methods defined in the configuration file.
    """
    cur = conn.cursor()
    setup_target_table(cur)

    source_table = config['database']['source_table']
    target_table = config['database']['target_table']
    mappings = config['mappings']

    # Dynamically build the SELECT query based on mappings
    source_cols = ["id"] + [m['source'] for m in mappings]
    query = f"SELECT {', '.join(source_cols)} FROM {source_table};"

    cur.execute(query)
    rows = cur.fetchall()

    print(f"\n{'--- ANONIFY DB | DYNAMIC TRANSFORMATION ---':^65}")

    for row in rows:
        data = dict(zip(source_cols, row))
        user_id = data['id']
        masked_values = {"id": user_id}

        for m in mappings:
            col_source = m['source']
            col_target = m['target']
            method = m['method']

            if method == "fake_name":
                masked_values[col_target] = get_deterministic_name(user_id)
            elif method == "fake_email":
                # Create email using the deterministic name to maintain consistency
                clean_name = get_deterministic_name(user_id).lower().replace(' ', '.')
                masked_values[col_target] = f"{clean_name}@example.com"
            elif method == "salary_bucket":
                raw_salary = data[col_source]
                masked_values[col_target] = get_salary_bucket(raw_salary)
            elif method == "mask_constant":
                masked_values[col_target] = m.get('value', '[MASKED]')

        # Execute dynamic INSERT with placeholders
        cols_to_insert = list(masked_values.keys())
        placeholders = [f"%({c})s" for c in cols_to_insert]

        insert_query = f"""
            INSERT INTO {target_table} ({', '.join(cols_to_insert)})
            VALUES ({', '.join(placeholders)});
        """
        cur.execute(insert_query, masked_values)

    conn.commit()
    cur.close()