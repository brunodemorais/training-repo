import requests
import pandas as pd
import json

# Method 1: Using Socrata API (Recommended - no library installation needed)
def fetch_ev_data_basic(limit=1000, offset=0):
    """
    Fetch EV population data using basic requests
    
    Parameters:
    - limit: number of records to fetch per request (max 50000)
    - offset: starting position for pagination
    """
    base_url = "https://data.wa.gov/resource/f6w7-q2d2.json"
    
    params = {
        "$limit": limit,
        "$offset": offset
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        print(f"Fetched {len(data)} records")
        return pd.DataFrame(data)
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None


# Method 2: Fetch ALL data with pagination
def fetch_all_ev_data():
    """
    Fetch all EV data using pagination
    """
    all_data = []
    limit = 50000  # Maximum allowed per request
    offset = 0
    
    print("Fetching all EV population data...")
    
    while True:
        base_url = "https://data.wa.gov/resource/f6w7-q2d2.json"
        params = {
            "$limit": limit,
            "$offset": offset
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data:  # No more data
                break
            
            all_data.extend(data)
            print(f"Fetched {len(data)} records (Total: {len(all_data)})")
            
            if len(data) < limit:  # Last page
                break
            
            offset += limit
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break
    
    return pd.DataFrame(all_data)


# Method 3: Using filters (SoQL queries)
def fetch_filtered_ev_data(make=None, model_year=None, ev_type=None):
    """
    Fetch filtered EV data using Socrata Query Language (SoQL)
    
    Parameters:
    - make: Vehicle make (e.g., 'TESLA', 'NISSAN')
    - model_year: Model year (e.g., 2023)
    - ev_type: Electric vehicle type (e.g., 'Battery Electric Vehicle (BEV)')
    """
    base_url = "https://data.wa.gov/resource/f6w7-q2d2.json"
    
    # Build filter conditions
    where_conditions = []
    if make:
        where_conditions.append(f"make='{make}'")
    if model_year:
        where_conditions.append(f"model_year='{model_year}'")
    if ev_type:
        where_conditions.append(f"electric_vehicle_type='{ev_type}'")
    
    params = {
        "$limit": 50000
    }
    
    if where_conditions:
        params["$where"] = " AND ".join(where_conditions)
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        print(f"Fetched {len(data)} filtered records")
        return pd.DataFrame(data)
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None


# Example usage
if __name__ == "__main__":
    # Example 1: Fetch first 1000 records
    print("=" * 50)
    print("Example 1: Fetching first 1000 records")
    print("=" * 50)
    df = fetch_ev_data_basic(limit=1000)
    if df is not None:
        print(f"\nDataset shape: {df.shape}")
        print(f"\nColumn names:\n{df.columns.tolist()}")
        print(f"\nFirst few rows:\n{df.head()}")
    
    # Example 2: Fetch all data (warning: may take a while)
    print("\n" + "=" * 50)
    print("Example 2: Fetching ALL records (this may take time)")
    print("=" * 50)
    # Uncomment the line below to fetch all data
    # df_all = fetch_all_ev_data()
    # df_all.to_csv('ev_population_data.csv', index=False)
    
    # Example 3: Fetch filtered data (Tesla vehicles from 2023)
    print("\n" + "=" * 50)
    print("Example 3: Fetching Tesla vehicles from 2023")
    print("=" * 50)
    df_filtered = fetch_filtered_ev_data(make='TESLA', model_year=2023)
    if df_filtered is not None:
        print(f"\nFiltered dataset shape: {df_filtered.shape}")
        print(f"\nFirst few rows:\n{df_filtered.head()}")
    
    # Example 4: Save to CSV
    if df is not None:
        df.to_csv('ev_data_sample.csv', index=False)
        print("\n✓ Data saved to 'ev_data_sample.csv'")