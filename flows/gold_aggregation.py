from io import BytesIO
import pandas as pd
from datetime import datetime

from prefect import flow, task

from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client


@task(name="load_from_silver", retries=2)
def load_data_from_silver(object_name: str) -> pd.DataFrame:
    """
    Load cleaned CSV data from silver bucket.

    Args:
        object_name: Name of object in silver bucket

    Returns:
        DataFrame with cleaned data
    """
    client = get_minio_client()
    
    try:
        response = client.get_object(BUCKET_SILVER, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        
        df = pd.read_csv(BytesIO(data))
        
        # Parse date columns
        if 'date_inscription' in df.columns:
            df['date_inscription'] = pd.to_datetime(df['date_inscription'])
        if 'date_achat' in df.columns:
            df['date_achat'] = pd.to_datetime(df['date_achat'])
        if 'cleaned_at' in df.columns:
            df['cleaned_at'] = pd.to_datetime(df['cleaned_at'])
            
        print(f"Loaded {len(df)} rows from {object_name}")
        return df
    except Exception as e:
        print(f"Error loading {object_name}: {e}")
        raise


@task(name="create_fact_achats")
def create_fact_achats(achats_df: pd.DataFrame, clients_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create fact table for purchases with enriched dimensions.
    
    Args:
        achats_df: Cleaned purchases data
        clients_df: Cleaned clients data
        
    Returns:
        Fact table with purchases and client dimensions
    """
    print("Creating fact_achats table...")
    
    # Join achats with clients to enrich with client dimensions
    fact = achats_df.merge(
        clients_df[['id_client', 'country', 'date_inscription']],
        on='id_client',
        how='left'
    )
    
    # Add temporal dimensions
    fact['year'] = fact['date_achat'].dt.year
    fact['month'] = fact['date_achat'].dt.month
    fact['quarter'] = fact['date_achat'].dt.quarter
    fact['week'] = fact['date_achat'].dt.isocalendar().week
    fact['day_of_week'] = fact['date_achat'].dt.dayofweek
    fact['day_of_week_name'] = fact['date_achat'].dt.day_name()
    fact['year_month'] = fact['date_achat'].dt.to_period('M').astype(str)
    fact['year_week'] = fact['date_achat'].dt.to_period('W').astype(str)
    
    # Add client age (in days since registration)
    fact['client_age_days'] = (fact['date_achat'] - fact['date_inscription']).dt.days
    fact['client_age_months'] = fact['client_age_days'] / 30.44
    
    # Add derived metrics
    fact['is_high_value'] = fact['montant'] > fact['montant'].quantile(0.75)
    
    print(f"Created fact table with {len(fact)} rows and {len(fact.columns)} columns")
    return fact


@task(name="calculate_kpis_by_period")
def calculate_kpis_by_period(fact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate KPIs aggregated by time periods.
    
    Metrics:
    - Total volume (count of purchases)
    - Total revenue (CA - Chiffre d'Affaires)
    - Average basket
    - Number of unique clients
    - Growth rate vs previous period
    
    Args:
        fact_df: Fact table with purchases
        
    Returns:
        DataFrame with KPIs by period
    """
    print("Calculating KPIs by period...")
    
    # Monthly aggregations
    monthly_kpis = fact_df.groupby(['year', 'month', 'year_month']).agg({
        'id_achat': 'count',
        'montant': ['sum', 'mean', 'std', 'min', 'max'],
        'id_client': 'nunique',
        'produit': 'nunique'
    }).reset_index()
    
    # Flatten column names
    monthly_kpis.columns = [
        'year', 'month', 'year_month',
        'volume_achats', 
        'ca_total', 'panier_moyen', 'panier_std', 'panier_min', 'panier_max',
        'nb_clients_uniques', 'nb_produits_uniques'
    ]
    
    # Sort by date
    monthly_kpis = monthly_kpis.sort_values(['year', 'month'])
    
    # Calculate growth rates (month over month)
    monthly_kpis['ca_growth_rate'] = monthly_kpis['ca_total'].pct_change() * 100
    monthly_kpis['volume_growth_rate'] = monthly_kpis['volume_achats'].pct_change() * 100
    
    # Add period type
    monthly_kpis['period_type'] = 'monthly'
    
    print(f"Generated {len(monthly_kpis)} monthly KPI records")
    return monthly_kpis


@task(name="calculate_kpis_by_country")
def calculate_kpis_by_country(fact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate KPIs aggregated by country.
    
    Args:
        fact_df: Fact table with purchases
        
    Returns:
        DataFrame with KPIs by country
    """
    print("Calculating KPIs by country...")
    
    country_kpis = fact_df.groupby('country').agg({
        'id_achat': 'count',
        'montant': ['sum', 'mean', 'std'],
        'id_client': 'nunique',
        'produit': 'nunique'
    }).reset_index()
    
    # Flatten column names
    country_kpis.columns = [
        'country',
        'volume_achats',
        'ca_total', 'panier_moyen', 'panier_std',
        'nb_clients_uniques', 'nb_produits_uniques'
    ]
    
    # Calculate market share
    total_ca = country_kpis['ca_total'].sum()
    country_kpis['market_share'] = (country_kpis['ca_total'] / total_ca * 100).round(2)
    
    # Calculate revenue per client
    country_kpis['ca_per_client'] = (
        country_kpis['ca_total'] / country_kpis['nb_clients_uniques']
    ).round(2)
    
    # Sort by revenue
    country_kpis = country_kpis.sort_values('ca_total', ascending=False)
    
    print(f"Generated KPIs for {len(country_kpis)} countries")
    return country_kpis


@task(name="calculate_kpis_by_product")
def calculate_kpis_by_product(fact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate KPIs aggregated by product.
    
    Args:
        fact_df: Fact table with purchases
        
    Returns:
        DataFrame with KPIs by product
    """
    print("Calculating KPIs by product...")
    
    product_kpis = fact_df.groupby('produit').agg({
        'id_achat': 'count',
        'montant': ['sum', 'mean', 'std', 'min', 'max'],
        'id_client': 'nunique'
    }).reset_index()
    
    # Flatten column names
    product_kpis.columns = [
        'produit',
        'volume_achats',
        'ca_total', 'panier_moyen', 'panier_std', 'panier_min', 'panier_max',
        'nb_clients_uniques'
    ]
    
    # Calculate product metrics
    total_ca = product_kpis['ca_total'].sum()
    total_volume = product_kpis['volume_achats'].sum()
    
    product_kpis['market_share_ca'] = (product_kpis['ca_total'] / total_ca * 100).round(2)
    product_kpis['market_share_volume'] = (product_kpis['volume_achats'] / total_volume * 100).round(2)
    
    # Sort by revenue
    product_kpis = product_kpis.sort_values('ca_total', ascending=False)
    
    print(f"Generated KPIs for {len(product_kpis)} products")
    return product_kpis


@task(name="calculate_cohort_analysis")
def calculate_cohort_analysis(fact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform cohort analysis based on client registration month.
    
    Args:
        fact_df: Fact table with purchases
        
    Returns:
        DataFrame with cohort metrics
    """
    print("Performing cohort analysis...")
    
    # Create cohort (based on registration month)
    fact_df = fact_df.copy()
    fact_df['cohort'] = fact_df['date_inscription'].dt.to_period('M')
    
    # Calculate cohort metrics
    cohort_data = fact_df.groupby(['cohort', 'year_month']).agg({
        'id_achat': 'count',
        'montant': 'sum',
        'id_client': 'nunique'
    }).reset_index()
    
    cohort_data.columns = ['cohort', 'period', 'nb_achats', 'ca_total', 'nb_clients']
    
    # Convert cohort to string for easier handling
    cohort_data['cohort'] = cohort_data['cohort'].astype(str)
    
    print(f"Generated cohort analysis with {len(cohort_data)} records")
    return cohort_data


@task(name="calculate_client_segmentation")
def calculate_client_segmentation(fact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Segment clients based on purchase behavior (RFM-like analysis).
    
    Args:
        fact_df: Fact table with purchases
        
    Returns:
        DataFrame with client segments
    """
    print("Calculating client segmentation...")
    
    # Get the reference date (most recent purchase date)
    reference_date = fact_df['date_achat'].max()
    
    # Calculate RFM metrics per client
    client_rfm = fact_df.groupby('id_client').agg({
        'date_achat': lambda x: (reference_date - x.max()).days,  # Recency
        'id_achat': 'count',  # Frequency
        'montant': 'sum'  # Monetary
    }).reset_index()
    
    client_rfm.columns = ['id_client', 'recency_days', 'frequency', 'monetary_value']
    
    # Add country
    client_country = fact_df.groupby('id_client')['country'].first().reset_index()
    client_rfm = client_rfm.merge(client_country, on='id_client')
    
    # Create quartile-based scores (1-4, 4 being best)
    client_rfm['recency_score'] = pd.qcut(
        client_rfm['recency_days'], 
        q=4, 
        labels=[4, 3, 2, 1],  # Inverted: lower recency is better
        duplicates='drop'
    ).astype(int)
    
    client_rfm['frequency_score'] = pd.qcut(
        client_rfm['frequency'], 
        q=4, 
        labels=[1, 2, 3, 4],
        duplicates='drop'
    ).astype(int)
    
    client_rfm['monetary_score'] = pd.qcut(
        client_rfm['monetary_value'], 
        q=4, 
        labels=[1, 2, 3, 4],
        duplicates='drop'
    ).astype(int)
    
    # Create overall RFM score
    client_rfm['rfm_score'] = (
        client_rfm['recency_score'] + 
        client_rfm['frequency_score'] + 
        client_rfm['monetary_score']
    )
    
    # Segment clients
    def segment_client(row):
        if row['rfm_score'] >= 10:
            return 'Champions'
        elif row['rfm_score'] >= 8:
            return 'Loyal Customers'
        elif row['rfm_score'] >= 6:
            return 'Potential Loyalists'
        elif row['rfm_score'] >= 5 and row['recency_score'] >= 3:
            return 'Recent Customers'
        elif row['rfm_score'] >= 5:
            return 'At Risk'
        else:
            return 'Lost'
    
    client_rfm['segment'] = client_rfm.apply(segment_client, axis=1)
    
    print(f"Segmented {len(client_rfm)} clients")
    print("Segment distribution:")
    print(client_rfm['segment'].value_counts())
    
    return client_rfm


@task(name="calculate_weekly_trends")
def calculate_weekly_trends(fact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate weekly trends and aggregations.
    
    Args:
        fact_df: Fact table with purchases
        
    Returns:
        DataFrame with weekly metrics
    """
    print("Calculating weekly trends...")
    
    weekly_kpis = fact_df.groupby(['year', 'week', 'year_week']).agg({
        'id_achat': 'count',
        'montant': ['sum', 'mean'],
        'id_client': 'nunique'
    }).reset_index()
    
    weekly_kpis.columns = [
        'year', 'week', 'year_week',
        'volume_achats', 'ca_total', 'panier_moyen', 'nb_clients_uniques'
    ]
    
    weekly_kpis = weekly_kpis.sort_values(['year', 'week'])
    weekly_kpis['ca_growth_rate'] = weekly_kpis['ca_total'].pct_change() * 100
    weekly_kpis['period_type'] = 'weekly'
    
    print(f"Generated {len(weekly_kpis)} weekly trend records")
    return weekly_kpis


@task(name="save_to_gold", retries=2)
def save_to_gold(df: pd.DataFrame, object_name: str) -> str:
    """
    Save aggregated DataFrame to gold bucket.

    Args:
        df: Aggregated DataFrame
        object_name: Name of object in gold bucket

    Returns:
        Object name in gold layer
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)

    # Convert DataFrame to CSV
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    client.put_object(
        BUCKET_GOLD,
        object_name,
        csv_buffer,
        length=len(csv_buffer.getvalue())
    )
    
    print(f"Saved {len(df)} rows to {BUCKET_GOLD}/{object_name}")
    return object_name


@flow(name="Gold Aggregation Flow")
def gold_aggregation_flow() -> dict:
    """
    Main flow: Load from silver, create aggregations, and save to gold layer.

    Returns:
        Dictionary with gold layer statistics
    """
    
    # Load clean data from silver
    clients_df = load_data_from_silver("clients_clean.csv")
    achats_df = load_data_from_silver("achats_clean.csv")
    
    # Create fact table
    fact_achats = create_fact_achats(achats_df, clients_df)
    save_to_gold(fact_achats, "fact_achats.csv")
    
    # Calculate various KPIs and aggregations
    monthly_kpis = calculate_kpis_by_period(fact_achats)
    save_to_gold(monthly_kpis, "kpis_monthly.csv")
    
    weekly_kpis = calculate_weekly_trends(fact_achats)
    save_to_gold(weekly_kpis, "kpis_weekly.csv")
    
    country_kpis = calculate_kpis_by_country(fact_achats)
    save_to_gold(country_kpis, "kpis_by_country.csv")
    
    product_kpis = calculate_kpis_by_product(fact_achats)
    save_to_gold(product_kpis, "kpis_by_product.csv")
    
    cohort_analysis = calculate_cohort_analysis(fact_achats)
    save_to_gold(cohort_analysis, "cohort_analysis.csv")
    
    client_segments = calculate_client_segmentation(fact_achats)
    save_to_gold(client_segments, "client_segmentation.csv")
    
    # Generate summary statistics
    stats = {
        "fact_table_rows": len(fact_achats),
        "monthly_periods": len(monthly_kpis),
        "weekly_periods": len(weekly_kpis),
        "countries": len(country_kpis),
        "products": len(product_kpis),
        "cohorts": len(cohort_analysis),
        "clients_segmented": len(client_segments),
        "total_revenue": float(fact_achats['montant'].sum()),
        "total_purchases": len(fact_achats),
        "unique_clients": fact_achats['id_client'].nunique(),
        "avg_basket": float(fact_achats['montant'].mean()),
        "date_range": {
            "start": str(fact_achats['date_achat'].min().date()),
            "end": str(fact_achats['date_achat'].max().date())
        }
    }
    
    return stats


if __name__ == "__main__":
    result = gold_aggregation_flow()
    print("\n=== Gold Aggregation Complete ===")
    print(f"Fact table: {result['fact_table_rows']:,} rows")
    print(f"Time periods: {result['monthly_periods']} months, {result['weekly_periods']} weeks")
    print(f"Dimensions: {result['countries']} countries, {result['products']} products")
    print(f"Total Revenue: €{result['total_revenue']:,.2f}")
    print(f"Total Purchases: {result['total_purchases']:,}")
    print(f"Unique Clients: {result['unique_clients']:,}")
    print(f"Average Basket: €{result['avg_basket']:.2f}")
    print(f"Date Range: {result['date_range']['start']} to {result['date_range']['end']}")
