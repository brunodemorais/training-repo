import requests
import pandas as pd
import os

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

# Example usage
if __name__ == "__main__":
    # Fetch all data (warning: may take a while)
    print("\n" + "=" * 50)
    print("Fetching ALL records (this may take time)")
    print("=" * 50)
    # Uncomment the line below to fetch all data
    df_all = fetch_all_ev_data()
    # Uncomment the lines below to print the data to csv
    os.makedirs('src/etl/electric_vehicles/data/ev_population_data.csv', exist_ok=True)
    df_all.to_csv('src/etl/electric_vehicles/data/ev_population_data.csv', index=False)