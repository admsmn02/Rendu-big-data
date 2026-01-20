from io import BytesIO
from pathlib import Path
import pandas as pd
from datetime import datetime

from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client


@task(name="load_from_bronze", retries=2)
def load_data_from_bronze(object_name: str) -> pd.DataFrame:
    """
    Load CSV data from bronze bucket.

    Args:
        object_name: Name of object in bronze bucket

    Returns:
        DataFrame with raw data
    """
    client = get_minio_client()
    
    try:
        response = client.get_object(BUCKET_BRONZE, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        
        df = pd.read_csv(BytesIO(data))
        print(f"Loaded {len(df)} rows from {object_name}")
        return df
    except Exception as e:
        print(f"Error loading {object_name}: {e}")
        raise


@task(name="clean_clients_data")
def clean_clients_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize clients data.
    
    Transformations:
    - Remove null values in critical columns
    - Standardize date formats
    - Normalize country names
    - Deduplicate records
    - Validate email formats
    
    Args:
        df: Raw clients DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    print(f"Initial rows: {len(df)}")
    
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # 1. Handle missing values
    # Critical columns should not have nulls
    initial_count = len(df_clean)
    df_clean = df_clean.dropna(subset=['id_client', 'email'])
    print(f"Removed {initial_count - len(df_clean)} rows with null id_client or email")
    
    # Fill missing names with placeholder
    df_clean['name'] = df_clean['name'].fillna('Unknown')
    
    # Fill missing countries with 'Unknown'
    df_clean['country'] = df_clean['country'].fillna('Unknown')
    
    # 2. Standardize date formats
    df_clean['date_inscription'] = pd.to_datetime(
        df_clean['date_inscription'], 
        errors='coerce'
    )
    
    # Remove rows with invalid dates
    initial_count = len(df_clean)
    df_clean = df_clean.dropna(subset=['date_inscription'])
    print(f"Removed {initial_count - len(df_clean)} rows with invalid dates")
    
    # Filter out future dates (data quality issue)
    today = pd.Timestamp.now()
    future_dates = df_clean['date_inscription'] > today
    if future_dates.any():
        print(f"Warning: Found {future_dates.sum()} future registration dates")
        # Keep them but flag for analysis
        df_clean['has_future_date'] = future_dates
    else:
        df_clean['has_future_date'] = False
    
    # 3. Normalize data types
    df_clean['id_client'] = df_clean['id_client'].astype(int)
    
    # 4. Standardize text fields
    df_clean['email'] = df_clean['email'].str.lower().str.strip()
    df_clean['country'] = df_clean['country'].str.strip()
    df_clean['name'] = df_clean['name'].str.strip()
    
    # 5. Validate email format (basic validation)
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    valid_emails = df_clean['email'].str.match(email_pattern, na=False)
    invalid_count = (~valid_emails).sum()
    if invalid_count > 0:
        print(f"Warning: Found {invalid_count} invalid email formats")
        df_clean['email_valid'] = valid_emails
    else:
        df_clean['email_valid'] = True
    
    # 6. Deduplicate
    # First by exact duplicates
    initial_count = len(df_clean)
    df_clean = df_clean.drop_duplicates()
    print(f"Removed {initial_count - len(df_clean)} exact duplicate rows")
    
    # Then by business key (email should be unique)
    initial_count = len(df_clean)
    df_clean = df_clean.sort_values('date_inscription', ascending=False)  # Keep most recent
    df_clean = df_clean.drop_duplicates(subset=['email'], keep='first')
    print(f"Removed {initial_count - len(df_clean)} duplicate emails")
    
    # 7. Add data quality metadata
    df_clean['cleaned_at'] = datetime.now()
    df_clean['data_quality_score'] = (
        df_clean['email_valid'].astype(int) * 0.4 +
        (~df_clean['has_future_date']).astype(int) * 0.3 +
        (df_clean['name'] != 'Unknown').astype(int) * 0.2 +
        (df_clean['country'] != 'Unknown').astype(int) * 0.1
    )
    
    print(f"Final cleaned rows: {len(df_clean)}")
    print(f"Average data quality score: {df_clean['data_quality_score'].mean():.2f}")
    
    return df_clean


@task(name="clean_achats_data")
def clean_achats_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize purchases (achats) data.
    
    Transformations:
    - Remove null values in critical columns
    - Standardize date formats
    - Validate amount ranges (no negative values)
    - Deduplicate records
    - Normalize product names
    
    Args:
        df: Raw achats DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    print(f"Initial rows: {len(df)}")
    
    # Create a copy
    df_clean = df.copy()
    
    # 1. Handle missing values
    initial_count = len(df_clean)
    df_clean = df_clean.dropna(subset=['id_achat', 'id_client', 'montant'])
    print(f"Removed {initial_count - len(df_clean)} rows with null critical fields")
    
    # Fill missing product names
    df_clean['produit'] = df_clean['produit'].fillna('Unknown')
    
    # 2. Standardize date formats
    df_clean['date_achat'] = pd.to_datetime(
        df_clean['date_achat'],
        errors='coerce'
    )
    
    initial_count = len(df_clean)
    df_clean = df_clean.dropna(subset=['date_achat'])
    print(f"Removed {initial_count - len(df_clean)} rows with invalid dates")
    
    # Flag future dates
    today = pd.Timestamp.now()
    future_dates = df_clean['date_achat'] > today
    if future_dates.any():
        print(f"Warning: Found {future_dates.sum()} future purchase dates")
        df_clean['has_future_date'] = future_dates
    else:
        df_clean['has_future_date'] = False
    
    # 3. Normalize data types
    df_clean['id_achat'] = df_clean['id_achat'].astype(int)
    df_clean['id_client'] = df_clean['id_client'].astype(int)
    df_clean['montant'] = df_clean['montant'].astype(float)
    
    # 4. Validate amounts
    # Check for negative or zero amounts
    invalid_amounts = df_clean['montant'] <= 0
    if invalid_amounts.any():
        print(f"Warning: Found {invalid_amounts.sum()} invalid amounts (<= 0)")
        df_clean = df_clean[~invalid_amounts]
        print(f"Removed invalid amounts")
    
    # Flag outliers (using IQR method)
    Q1 = df_clean['montant'].quantile(0.25)
    Q3 = df_clean['montant'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 3 * IQR
    upper_bound = Q3 + 3 * IQR
    
    outliers = (df_clean['montant'] < lower_bound) | (df_clean['montant'] > upper_bound)
    df_clean['is_outlier'] = outliers
    print(f"Identified {outliers.sum()} amount outliers (kept for analysis)")
    
    # 5. Standardize text fields
    df_clean['produit'] = df_clean['produit'].str.strip().str.title()
    
    # 6. Deduplicate
    initial_count = len(df_clean)
    df_clean = df_clean.drop_duplicates()
    print(f"Removed {initial_count - len(df_clean)} exact duplicate rows")
    
    # Check for potential duplicates (same client, same date, same amount, same product)
    initial_count = len(df_clean)
    df_clean = df_clean.drop_duplicates(
        subset=['id_client', 'date_achat', 'montant', 'produit'],
        keep='first'
    )
    print(f"Removed {initial_count - len(df_clean)} likely duplicate purchases")
    
    # 7. Add data quality metadata
    df_clean['cleaned_at'] = datetime.now()
    df_clean['data_quality_score'] = (
        (~df_clean['is_outlier']).astype(int) * 0.5 +
        (~df_clean['has_future_date']).astype(int) * 0.3 +
        (df_clean['produit'] != 'Unknown').astype(int) * 0.2
    )
    
    print(f"Final cleaned rows: {len(df_clean)}")
    print(f"Average data quality score: {df_clean['data_quality_score'].mean():.2f}")
    
    return df_clean


@task(name="save_to_silver", retries=2)
def save_to_silver(df: pd.DataFrame, object_name: str) -> str:
    """
    Save cleaned DataFrame to silver bucket.

    Args:
        df: Cleaned DataFrame
        object_name: Name of object in silver bucket

    Returns:
        Object name in silver layer
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    # Convert DataFrame to CSV
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    client.put_object(
        BUCKET_SILVER,
        object_name,
        csv_buffer,
        length=len(csv_buffer.getvalue())
    )
    
    print(f"Saved {len(df)} rows to {BUCKET_SILVER}/{object_name}")
    return object_name


@flow(name="Silver Transformation Flow")
def silver_transformation_flow() -> dict:
    """
    Main flow: Load from bronze, clean data, and save to silver layer.

    Returns:
        Dictionary with transformation statistics
    """
    
    # Load data from bronze
    clients_df = load_data_from_bronze("clients.csv")
    achats_df = load_data_from_bronze("achats.csv")
    
    # Clean data
    clients_clean = clean_clients_data(clients_df)
    achats_clean = clean_achats_data(achats_df)
    
    # Save to silver
    clients_silver = save_to_silver(clients_clean, "clients_clean.csv")
    achats_silver = save_to_silver(achats_clean, "achats_clean.csv")
    
    # Calculate statistics
    stats = {
        "clients": {
            "bronze_rows": len(clients_df),
            "silver_rows": len(clients_clean),
            "rows_removed": len(clients_df) - len(clients_clean),
            "removal_rate": (len(clients_df) - len(clients_clean)) / len(clients_df) * 100,
            "avg_quality_score": float(clients_clean['data_quality_score'].mean())
        },
        "achats": {
            "bronze_rows": len(achats_df),
            "silver_rows": len(achats_clean),
            "rows_removed": len(achats_df) - len(achats_clean),
            "removal_rate": (len(achats_df) - len(achats_clean)) / len(achats_df) * 100,
            "avg_quality_score": float(achats_clean['data_quality_score'].mean())
        }
    }
    
    return stats


if __name__ == "__main__":
    result = silver_transformation_flow()
    print("\n=== Silver Transformation Complete ===")
    print(f"Clients: {result['clients']['bronze_rows']} → {result['clients']['silver_rows']} rows "
          f"({result['clients']['removal_rate']:.1f}% removed)")
    print(f"Achats: {result['achats']['bronze_rows']} → {result['achats']['silver_rows']} rows "
          f"({result['achats']['removal_rate']:.1f}% removed)")
