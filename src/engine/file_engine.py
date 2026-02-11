import pandas as pd
import logging
import os
from engine.anonify_engine import get_deterministic_name, get_salary_bucket

logger = logging.getLogger(__name__)

def load_data_safely(input_path, ext):
    """
    Attempts to load data with multiple encodings to handle special characters.
    """
    encodings = ['utf-8', 'latin1', 'cp1252']

    if ext == '.xlsx':
        return pd.read_excel(input_path)

    for enc in encodings:
        try:
            if ext == '.csv':
                return pd.read_csv(input_path, encoding=enc)
            elif ext == '.json':
                return pd.read_json(input_path, encoding=enc)
        except (UnicodeDecodeError, ValueError) as e:
            logger.warning(f"?? Failed to read with encoding {enc}, trying next...")
            continue

    raise ValueError(f"? Could not decode file {input_path} using common encodings.")

def process_file(input_path, output_path, mappings):
    """
    Reads a file, applies anonymization mappings, and saves to the output path.
    Supported: .csv, .xlsx, .json
    """
    try:
        ext = os.path.splitext(input_path)[-1].lower()
        logger.info(f"?? Reading input file: {input_path}")

        # Use the safe loader to handle encoding issues like byte 0xfc
        df = load_data_safely(input_path, ext)

        logger.info(f"??  Anonymizing {len(df)} rows...")

        # Apply mappings
        for idx, row in df.iterrows():
            # Use 'id' column if exists, otherwise use DataFrame index
            user_id = int(row.get('id', idx))

            for m in mappings:
                col_source = m['source']
                col_target = m.get('target', col_source)
                method = m['method']

                if method == "fake_name":
                    df.at[idx, col_target] = get_deterministic_name(user_id)
                elif method == "fake_email":
                    clean_name = get_deterministic_name(user_id).lower().replace(' ', '.')
                    df.at[idx, col_target] = f"{clean_name}@example.com"
                elif method == "salary_bucket":
                    raw_value = str(row[col_source])
                    df.at[idx, col_target] = get_salary_bucket(raw_value)

        # Save data based on output extension
        out_ext = os.path.splitext(output_path)[-1].lower()
        logger.info(f"?? Saving anonymized data to: {output_path}")

        if out_ext == '.csv':
            # utf-8-sig ensures Excel opens the CSV correctly with special characters
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
        elif out_ext == '.xlsx':
            df.to_excel(output_path, index=False)
        elif out_ext == '.json':
            # force_ascii=False je klju?an za ?itljiva slova (ü, ö, €, itd.)
            df.to_json(output_path, orient='records', indent=4, force_ascii=False)


        logger.info("? File anonymization completed.")

    except Exception as e:
        logger.error(f"? An error occurred during file processing: {str(e)}")