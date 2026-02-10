import yaml
import os
from faker import Faker
from unidecode import unidecode

# 1. U?itavanje konfiguracije
current_dir = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(current_dir, '../../config/settings.yaml')

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

config = load_config()

# 2. Funkcija koja ti je nedostajala
def get_deterministic_name(user_id):
    """Generise stabilno ime na osnovu ID-a koristeci locale iz YAML-a."""
    locale = config['anonymization']['locale']
    local_fake = Faker(locale)
    local_fake.seed_instance(user_id)

    raw_name = local_fake.name()

    # Ako je u YAML-u use_ascii: true, ?istimo ime
    if config['anonymization'].get('use_ascii', True):
        return unidecode(raw_name)
    return raw_name

# 3. Glavna logika za procesiranje
def process_data(conn, setup_target_table):
    cur = conn.cursor()
    setup_target_table(cur)

    source_table = config['database']['source_table']
    target_table = config['database']['target_table']
    mappings = config['mappings']

    # Dinami?ki pravimo listu kolona za SELECT
    source_cols = ["id"] + [m['source'] for m in mappings]
    query = f"SELECT {', '.join(source_cols)} FROM {source_table};"

    cur.execute(query)
    rows = cur.fetchall()

    print(f"\n{'--- ANONIFY DB | DYNAMIC MAPPING MODE ---':^65}")

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
                # Koristimo isto deterministi?ko ime da bi email bio konzistentan
                clean_name = get_deterministic_name(user_id).lower().replace(' ', '.')
                masked_values[col_target] = f"{clean_name}@example.com"
            elif method == "mask_constant":
                masked_values[col_target] = m.get('value', '[MASKED]')

        # Dinami?ki INSERT upit
        cols_to_insert = list(masked_values.keys())
        placeholders = [f"%({c})s" for c in cols_to_insert]

        insert_query = f"""
            INSERT INTO {target_table} ({', '.join(cols_to_insert)})
            VALUES ({', '.join(placeholders)});
        """
        cur.execute(insert_query, masked_values)

        print(f"Mapped ID {user_id:3}: {data['full_name']} -> {masked_values['full_name']}")

    conn.commit()
    cur.close()