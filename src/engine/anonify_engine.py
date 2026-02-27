import yaml
import os
import logging
import random
import pandas as pd
from faker import Faker
from unidecode import unidecode

# Initialize logger
logger = logging.getLogger(__name__)

# Configuration path setup
current_dir = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(current_dir, '../../config/settings.yaml')

def transform_to_bucket(value):
    """Sigurna funkcija koja koristi Unicode escape karaktere."""
    try:
        if isinstance(value, str):
            # \u20ac je Unicode za evro - ovo ne moze da baci SyntaxError
            # Takodje uklanjamo zareze koji se cesto javljaju u AdventureWorks CSV-ovima
            clean_val = value.lower().replace('\u20ac', '').replace('$', '').replace('k', '000')
            clean_val = clean_val.replace(',', '').strip()
            
            # Ako vrednost ima decimalna mesta (npr. 70000.00), uzimamo samo ceo deo
            if '.' in clean_val:
                clean_val = clean_val.split('.')[0]
                
            num = float(clean_val)
        else:
            num = float(value)

        # Mapiranje u opsege
        euro_symbol = "\u20ac"
        if num < 50000:
            return f"< 50.000 {euro_symbol}"
        elif 50000 <= num < 100000:
            return f"50.000 {euro_symbol} - 100.000 {euro_symbol}"
        elif 100000 <= num < 150000:
            return f"100.000 {euro_symbol} - 150.000 {euro_symbol}"
        else:
            return f"> 150.000 {euro_symbol}"
    except:
        return "N/A"


def load_config():
    """Load transformation rules from YAML config."""
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return {}

config = load_config()

def get_salary_bucket(salary_val, locale="de_DE"):
    try:
        if pd.isna(salary_val) or str(salary_val).strip() == "":
            return "[NO DATA]"

        # 1. Standardize to lowercase for easier check
        raw_str = str(salary_val).lower().strip()

        # 2. Handle the "k" suffix BEFORE stripping digits
        # If it finds 'k', it multiplies by 1000
        multiplier = 1
        if 'k' in raw_str:
            multiplier = 1000

        # 3. Now clean everything except digits
        clean_numeric = "".join(c for c in raw_str if c.isdigit())

        if not clean_numeric:
            return "[INVALID]"

        # 4. Final calculation
        amount = int(clean_numeric) * multiplier

        # Formatting helper
        def fmt_curr(val):
            if locale == "de_DE":
                return f"{val:,.0f}".replace(",", ".") + " \u20ac"
            return f"${val // 1000}k"

        # 5. Corrected ranges
        if amount >= 150000: return f"> {fmt_curr(150000)}"
        if amount >= 100000: return f"{fmt_curr(100000)} - {fmt_curr(150000)}"
        if amount >= 50000:  return f"{fmt_curr(50000)} - {fmt_curr(100000)}"
        return f"< {fmt_curr(50000)}"

    except Exception as e:
        return f"[ERROR]"
def get_name_dynamic(user_id, locale, use_ascii, is_deterministic):
    """Generate localized fake names using deterministic seeds."""
    fake = Faker(locale)
    if is_deterministic:
        random.seed(user_id)
        fake.seed_instance(user_id)

    raw_name = fake.name()
    return unidecode(raw_name) if use_ascii else raw_name

def anonymize_dataframe(df, locale='de_DE', use_ascii=True, is_deterministic=True):
    fake = Faker(locale)

    # Ako je deterministicki, uvek koristimo isti seed
    if is_deterministic:
        fake.seed_instance(42)

    res_df = df.copy()

    # Pametna detekcija kolona (Case-insensitive)
    cols = {c.lower(): c for c in df.columns}

    # 1. Obrada IMENA (FirstName + LastName)
    fname_col = next((v for k, v in cols.items() if 'first' in k and 'name' in k), None)
    lname_col = next((v for k, v in cols.items() if 'last' in k and 'name' in k), None)

    if fname_col and lname_col:
        # Kreiramo novu kolonu full_name na osnovu maskiranih vrednosti
        res_df['full_name'] = res_df.apply(lambda _: fake.name(), axis=1)
        # Brišemo originale da ne cure podaci
        res_df.drop([fname_col, lname_col], axis=1, inplace=True)

    # 2. Obrada EMAIL-a
    email_col = next((v for k, v in cols.items() if 'email' in k), None)
    if email_col:
        res_df['email'] = res_df.apply(lambda _: fake.free_email(), axis=1)
        if email_col != 'email':
            res_df.drop(email_col, axis=1, inplace=True)

    # 3. Obrada PLATA / PRIHODA (Income, Salary, TotalDue)
    salary_col = next((v for k, v in cols.items() if any(x in k for x in ['salary', 'income', 'total', 'subtotal'])), None)
    if salary_col:
        # Ovde primenjujemo tvoju ve? napisanu logiku za bucket-e
        res_df['salary_bucket'] = res_df[salary_col].apply(lambda x: transform_to_bucket(x))
        res_df.drop(salary_col, axis=1, inplace=True)

    # ASCII konverzija ako je uklju?ena
    if use_ascii:
        # ... tvoj postoje?i kod za unidecode ...
        pass

    return res_df
    """
    Main entry point for data anonymization.
    Forces clean target columns to prevent data leakage/concatenation.
    """
    processed_df = df.copy()

    # Ensure ID exists for deterministic seeding
    if 'id' not in processed_df.columns:
        processed_df['id'] = range(1, len(processed_df) + 1)

    mappings = config.get('mappings', [])
    for m in mappings:
        src = m.get('source')
        tgt = m.get('target')
        method = m.get('method')

        if src not in processed_df.columns:
            logger.warning(f"Source column {src} not found in dataframe.")
            continue

        # CLEANUP: If target exists and is not source, drop it to prevent string 'ghosting'
        if tgt in processed_df.columns and src != tgt:
            processed_df.drop(columns=[tgt], inplace=True)

        if method == "fake_name":
            processed_df[tgt] = processed_df['id'].apply(
                lambda x: get_name_dynamic(x, locale, use_ascii, is_deterministic)
            )

        elif method == "fake_email":
            # Emails always use ASCII version of the dynamic name
            processed_df[tgt] = processed_df['id'].apply(
                lambda x: get_name_dynamic(x, locale, True, is_deterministic).lower().replace(' ', '.') + "@example.com"
            )

        elif method == "salary_bucket":
            # Apply bucket logic and force cast to values to break Series link
            bucket_series = processed_df[src].apply(lambda x: get_salary_bucket(x, locale))
            processed_df[tgt] = bucket_series.values

    return processed_df