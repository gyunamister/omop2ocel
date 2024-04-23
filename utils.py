import pandas as pd

def load_data(file_path):
    """Utility function to load data safely."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        print(f"Failed to load {file_path}: {e}")
        return None
    
def merge_dataframes(primary_df, secondary_df, key, how='inner', suffixes=('', '_y')):
    """Merge two dataframes on a key."""
    return pd.merge(primary_df, secondary_df, on=key, how=how, suffixes=suffixes)

def convert_to_datetime(df, cols, date_format="%Y-%m-%d %H:%M:%S"):
    """Convert specified columns of a DataFrame to datetime."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format=date_format)
    return df

