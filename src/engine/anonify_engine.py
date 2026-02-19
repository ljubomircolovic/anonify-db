import yaml
import os
import logging
import random
from faker import Faker
from unidecode import unidecode

# Initialize logger
logger = logging.getLogger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(current_dir, '../../config/settings.yaml')

def load_config():
    """Load application settings from the YAML configuration file."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return {}

config = load_config()

def get_salary_bucket(salary_str):
    """
    Categorize exact salary strings into ranges.
    Defined before anonymize_dataframe to avoid import errors.
    """
    try:
        if not salary_str or str(salary_str).lower() in ['none', 'nan', '']:
            return "[NO DATA]"
        
        clean_numeric = ''.join(filter(str.isdigit, str(salary_str)))
        if not clean_numeric:
            return "[INVALID FORMAT]"
            
        amount = int(clean_numeric)
        if amount < 1000:
            amount *= 1000
            
        if amount < 50000: return "< 50k"
        if amount < 100000: return "50k - 100k"
        if amount < 150000: return "100k - 150k"
        return "150k+"
    except Exception:
        return "[ERROR]"

def get_name_dynamic(user_id, locale, use_ascii, is_deterministic):
    """
    Generate a fake name with fixed seeds for deterministic mapping.
    Supports en_US and de_DE.
    """
    local_fake = Faker(locale)
    
    if is_deterministic:
        # Re-seeding both ensures maximum stability across different Faker providers
        random.seed(user_id)
        local_fake.seed_instance(user_id)
    else:
        # Use a high-range random seed for variety on each run
        random.seed(random.randint(0, 10**6))
        local_fake.seed_instance(random.randint(0, 10**6))
        
    raw_name = local_fake.name()

    if use_ascii:
        return unidecode(raw_name)
    return raw_name

def anonymize_dataframe(df, locale=None, use_ascii=None, is_deterministic=True):
    """
    Main entry point for data anonymization.
    English comments as requested.
    """
    mappings = config.get('mappings', [])
    processed_df = df.copy()
    
    # Resolve parameters: UI > Config > Default
    target_locale = locale if locale else config.get('anonymization', {}).get('locale', 'en_US')
    target_ascii = use_ascii if use_ascii is not None else config.get('anonymization', {}).get('use_ascii', True)

    if 'id' not in processed_df.columns:
        processed_df['id'] = range(1, len(processed_df) + 1)

    for m in mappings:
        col_source = m.get('source')
        col_target = m.get('target')
        method = m.get('method')

        if col_source not in processed_df.columns:
            continue

        if method == "fake_name":
            processed_df[col_target] = processed_df['id'].apply(
                lambda x: get_name_dynamic(x, target_locale, target_ascii, is_deterministic)
            )
        
        elif method == "fake_email":
            def make_email(uid):
                # Using dynamic name generation with ASCII forced for emails
                name = get_name_dynamic(uid, target_locale, True, is_deterministic).lower().replace(' ', '.')
                return f"{name}@example.com"
            processed_df[col_target] = processed_df['id'].apply(make_email)
            
        elif method == "salary_bucket":
            # Calling the locally defined function
            processed_df[col_target] = processed_df[col_source].apply(get_salary_bucket)

    return processed_df