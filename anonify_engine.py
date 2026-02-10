
from faker import Faker
from unidecode import unidecode

fake = Faker(['de_DE', 'en_US'])

def get_deterministic_name(user_id):
    fake.seed_instance(user_id)
    # Generišemo ime i odmah skidamo specijalne karaktere (ASCII only)
    raw_name = fake.name()
    return unidecode(raw_name)

def process_data(conn, setup_target_table):
    cur = conn.cursor()
    setup_target_table(cur)

    cur.execute("SELECT id, full_name, email, salary FROM users_raw;")
    rows = cur.fetchall()

    print(f"\n{'--- ANONIFY DB | ASCII MODE ---':^65}")
    for row in rows:
        user_id, real_name, _, _ = row
        fake_name = get_deterministic_name(user_id)

        cur.execute("""
            INSERT INTO users_masked (id, full_name, email, salary_status)
            VALUES (%s, %s, %s, %s);
        """, (user_id, fake_name, f"{fake_name.lower().replace(' ', '.')}@example.com", "[PROTECTED]"))

        print(f"Mapped ID {user_id:3}: {real_name} -> {fake_name}")

    conn.commit()
    cur.close()