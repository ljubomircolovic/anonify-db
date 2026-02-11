import yaml
import os
import logging
from faker import Faker
from unidecode import unidecode

# Get the logger inherited from main
logger = logging.getLogger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(current_dir, '../../config/settings.yaml')

def load_config():
    """Load application settings from the YAML configuration file."""
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

config = load_config()

def get_deterministic_name(user_id):
    """Generate a consistent fake name based on user_id."""
    locale = config['anonymization']['locale']
    local_fake = Faker(locale)
    local_fake.seed_instance(user_id)

    raw_name = local_fake.name()

    if config['anonymization'].get('use_ascii', True):
        return unidecode(raw_name)
    return raw_name

def get_salary_bucket(salary_str):
    """Categorize exact salary strings into ranges, handling EU and US formats."""
    try:
        if not salary_str or salary_str == 'None':
            return "[NO DATA]"

        # ?istimo sve što nije broj (uklanjamo ., €, USD, k, razmake...)
        # "125.000 €" -> "125000"
        # "160k" -> "160" (ovde moramo paziti na 'k')

        clean_numeric = ''.join(filter(str.isdigit, salary_str))

        if not clean_numeric:
            return "[INVALID FORMAT]"

        amount = int(clean_numeric)

        # Ako je neko napisao "160" (misle?i na k), a mi o?ekujemo pun iznos
        if amount < 1000:
            amount = amount * 1000

        if amount < 50000: return "< 50k"
        if amount < 100000: return "50k - 100k"
        if amount < 150000: return "100k - 150k"
        return "150k+"
    except (ValueError, TypeError):
        return "[ERROR]"

def process_data(conn, setup_target_table):
    """Core transformation engine with integrated logging."""
    cur = conn.cursor()
    setup_target_table(cur)

    source_table = config['database']['source_table']
    target_table = config['database']['target_table']
    mappings = config['mappings']

    source_cols = ["id"] + [m['source'] for m in mappings]
    query = f"SELECT {', '.join(source_cols)} FROM {source_table};"

    cur.execute(query)
    rows = cur.fetchall()

    logger.info(f"Transforming data from '{source_table}' to '{target_table}'...")

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
                clean_name = get_deterministic_name(user_id).lower().replace(' ', '.')
                masked_values[col_target] = f"{clean_name}@example.com"
            elif method == "salary_bucket":
                raw_salary = data[col_source]
                masked_values[col_target] = get_salary_bucket(raw_salary)

        cols_to_insert = list(masked_values.keys())
        placeholders = [f"%({c})s" for c in cols_to_insert]

        insert_query = f"""
            INSERT INTO {target_table} ({', '.join(cols_to_insert)})
            VALUES ({', '.join(placeholders)});
        """
        cur.execute(insert_query, masked_values)

    conn.commit()
    cur.close()
    logger.info("Transformation completed successfully.")