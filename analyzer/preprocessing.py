import pandas as pd
import logging
import re
import ast

# Configure logging
logger = logging.getLogger("Preprocessing")
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("preprocessing.log", mode="a", encoding="utf-8")
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def preprocess_notices(df: pd.DataFrame) -> pd.DataFrame:
    try:
        required = ['tender-value', 'TVH', 'tender-value-lowest']
        df = insert_missing_columns(df, required)
        df = handle_missing_values(df)
        df = convert_data_types(df)
        df = handle_categorical_data(df)
        df = remove_irrelevant_columns(df)
        df = impute_numerics(df)
        return df
    except Exception as e:
        logger.error(f"Preprocessing failed: {e}")
        raise


def insert_missing_columns(df, required_columns):
    for col in required_columns:
        if col not in df.columns:
            df[col] = pd.NA
            logger.warning(
                f"Missing column '{col}' — inserting default values.")
    return df


def handle_missing_values(df):
    # Replace NaN with pd.NA for consistency
    df = df.fillna(value=pd.NA)

    # Convert specific columns to appropriate dtypes before filling missing values
    for col in ['tender-value', 'TVH', 'tender-value-lowest']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(0)

    return df


def convert_data_types(df):
    for col in ['dispatch-date', 'publication-date']:
        if col in df.columns:
            # Parse datetime and drop timezones for consistency
            # Strip timezone manually before parsing
            df[col] = df[col].astype(str).str.split(
                '+', n=1).str[0].str.replace('Z', '', regex=False)
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.tz_localize(None)
    return df


def handle_categorical_data(df, top_n=10):
    categorical_columns = ['notice-type',
                           'contract-nature', 'main-activity', 'buyer-country']

    for col in categorical_columns:
        if col not in df.columns:
            continue

        def extract_first(val):
            if isinstance(val, str):
                try:
                    # Try parsing stringified list: e.g. "['works', 'services']"
                    parsed = ast.literal_eval(val)
                    if isinstance(parsed, list) and parsed:
                        return str(parsed[0])
                except Exception:
                    pass
                # If it’s a clean string, return as-is
                return val.strip()
            return "Others"  # fallback

        df[col] = df[col].apply(extract_first)

        # Simplify any outliers
        top_categories = df[col].value_counts().index[:top_n]
        df[col] = df[col].apply(
            lambda x: x if x in top_categories else "Others")

    df = pd.get_dummies(df, columns=categorical_columns, dummy_na=False)
    return df


def remove_irrelevant_columns(df):
    # We cannot drop 'publication-number' due to requirements regarding traceability of notices back to the original TED API.
    columns_to_drop = [
        'classification-cpv', 'recurrence-lot',
        'duration-period-value-lot', 'dispatch-date',
        'TV_CUR', 'renewal-maximum-lot', 'term-performance-lot', 'links'
    ]
    df = df.drop(columns=columns_to_drop, errors='ignore')
    return df


def impute_numerics(df):
    for col in ['tender-value', 'TVH', 'tender-value-lowest']:
        if col not in df.columns:
            logger.warning(
                f"Missing column '{col}' — inserting default values.")
            df[col] = 0.0
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(df[col].mean())
    return df
