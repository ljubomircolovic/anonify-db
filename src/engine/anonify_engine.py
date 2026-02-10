import yaml
import os
from faker import Faker
from unidecode import unidecode

# U?itavanje konfiguracije
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../../config/settings.yaml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

def get_deterministic_name(user_id):
    # Koristimo locale iz YAML-a
    locale = config['anonymization']['locale']
    local_fake = Faker(locale)
    local_fake.seed_instance(user_id)

    raw_name = local_fake.name()
    return unidecode(raw_name) if config['anonymization']['use_ascii'] else raw_name

def process_data(conn, setup_target_table):
    cur = conn.cursor()
    setup_target_table(cur)

    # Koristimo ime tabele iz YAML-a
    source_table = config['database']['source_table']
    target_table = config['database']['target_table']

    cur.execute(f"SELECT id, full_name, email, salary FROM {source_table};")
    rows = cur.fetchall()

    print(f"\n{'--- ANONIFY DB | CONFIG MODE ---':^65}")
    for row in rows:
        user_id, real_name, _, _ = row
        fake_name = get_deterministic_name(user_id)

        cur.execute(f"""
            INSERT INTO {target_table} (id, full_name, email, salary_status)
            VALUES (%s, %s, %s, %s);
        """, (user_id, fake_name, f"{fake_name.lower().replace(' ', '.')}@example.com", "[PROTECTED]"))

        print(f"Mapped ID {user_id:3}: {real_name} -> {fake_name}")

    conn.commit()
    cur.close()