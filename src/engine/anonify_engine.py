# -*- coding: utf-8 -*-
import concurrent.futures
import yaml
import os
import logging
import random
import pandas as pd
from faker import Faker
from unidecode import unidecode
import pandas as pd
import hashlib

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

def anonymize_dataframe(df, is_deterministic=True):
    """
    Anonymizes sensitive columns by detecting keywords like 'name', 'email', 'salary'.
    """
    anon_df = df.copy()
    
    # Mapping logic for different PII types
    for col in anon_df.columns:
        col_lower = col.lower()
        
        # 1. Name detection (matches 'full_name', 'first_name', 'name', etc.)
        if 'name' in col_lower:
            anon_df[col] = anon_df[col].apply(
                lambda x: f"User_{hashlib.md5(str(x).encode()).hexdigest()[:6]}" if pd.notnull(x) else x
            )
            
        # 2. Email detection
        elif 'email' in col_lower:
            anon_df[col] = anon_df[col].apply(
                lambda x: f"anon_{hashlib.md5(str(x).encode()).hexdigest()[:8]}@example.com" if pd.notnull(x) else x
            )
            
        # 3. Sensitive numbers (Salary, Balance) - let's mask or blur them
        elif 'salary' in col_lower or 'pay' in col_lower:
            # Simple noise addition (Senior level: adding +/- 10% variance)
            anon_df[col] = anon_df[col].apply(
                lambda x: round(x * 0.95, -2) if pd.notnull(x) else x # 5% discount for 'safe' analytics
            )

    return anon_df

def process_all_tables_parallel(engine, tables_metadata, max_workers=4):
    """
    Paralelno izvršava anonimizaciju više tabela.
    """
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Ovde pozivaš svoju glavnu funkciju za anonimizaciju jedne tabele
        futures = [
            executor.submit(process_single_table, engine, table_meta) 
            for table_meta in tables_metadata
        ]
        
        for future in concurrent.futures.as_completed(futures):
            # Logovanje progresa
            pass