import time
import os
import requests
import csv
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

start_time = time.time()

# Load environment variables from .env file
load_dotenv()

# Get API keys from environment variables
API_KEYS = os.getenv("API_KEYS", "").split(",")

# Remove any empty strings (in case of trailing commas or empty env variables)
API_KEYS = [key.strip() for key in API_KEYS if key.strip()]

if not API_KEYS:
    raise ValueError("No API keys found. Make sure to define API_KEYS in your .env file.")


# Define region
LAT_MIN, LAT_MAX = 24.0, 50.0  # Latitude range
LON_MIN, LON_MAX = -125.0, -67.0  # Longitude range
GRID_SIZE = 0.5  
REQUESTS_PER_MIN = 60  # API limit per key per minute
TOTAL_KEYS = len(API_KEYS)
MAX_REQUESTS_PER_BATCH = TOTAL_KEYS * REQUESTS_PER_MIN  
MAX_WORKERS = min(TOTAL_KEYS * 5, 20)  # Limit threads

# Input and output CSV files
LAND_CSV_FILE = "updated_output.csv"  # CSV containing lat, lon, and is_land values
OUTPUT_CSV_FILE = "aqi_data_parallel_optimized_13_apr.csv"

# Load land data into a dictionary for quick lookup
land_data = {}

with open(LAND_CSV_FILE, mode="r") as file:
    reader = csv.reader(file)
    next(reader)  # Skip header
    for row in reader:
        lat, lon, is_land = float(row[0]), float(row[1]), row[2].strip().lower() == 'true'
        land_data[(lat, lon)] = is_land

def fetch_aqi(lat, lon, api_key):
    """Fetch AQI and air quality components for a given latitude and longitude."""
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"
    try:
        time.sleep(1)
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if "list" in data and len(data["list"]) > 0:
                aqi_info = data["list"][0]
                return [
                    lat, lon, aqi_info["main"]["aqi"],
                    aqi_info["components"]["co"], aqi_info["components"]["no"],
                    aqi_info["components"]["no2"], aqi_info["components"]["o3"],
                    aqi_info["components"]["so2"], aqi_info["components"]["pm2_5"],
                    aqi_info["components"]["pm10"], aqi_info["components"]["nh3"],
                    datetime.utcfromtimestamp(aqi_info["dt"]).strftime('%Y-%m-%d %H:%M:%S')
                ]
        else:
            print(f"❌ Request failed for ({lat}, {lon}): {response.status_code} - {response.text}")
    except requests.RequestException as e:
        print(f"❌ Request error for ({lat}, {lon}): {e}")
    return None

def check_if_land(lat, lon):
    """Check if the given latitude and longitude is on land using preloaded CSV data."""
    return land_data.get((lat, lon), False)  # Default to False (water) if not found

def process_grid_point(lat, lon, api_key):
    """Fetch AQI data and check if location is on land, then return row data."""
    is_land = check_if_land(lat, lon)
    if not is_land:
        print(f"🌊 Skipping ({lat}, {lon}) - It's over water.")
        return None  # Skip water locations

    aqi_data = fetch_aqi(lat, lon, api_key)
    if aqi_data:
        row = aqi_data + [is_land]  # Append land status
        print(f"✅ Saved AQI for ({lat}, {lon}) | Is_Land: {is_land}")
        return row
    return None

# Prepare list of coordinates
coordinates = [
    (lat * GRID_SIZE, lon * GRID_SIZE)
    for lat in range(int(LAT_MIN / GRID_SIZE), int(LAT_MAX / GRID_SIZE) + 1)
    for lon in range(int(LON_MIN / GRID_SIZE), int(LON_MAX / GRID_SIZE) + 1)
]

# Open CSV file and write header
with open(OUTPUT_CSV_FILE, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Latitude", "Longitude", "AQI", "CO", "NO", "NO2", "O3", "SO2", "PM2.5", "PM10", "NH3", "Timestamp", "Is_Land"])

    total_requests = 0
    batch_size = REQUESTS_PER_MIN * TOTAL_KEYS  # Max requests per minute

    # Process in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i in range(0, len(coordinates), batch_size):
            batch = coordinates[i:i + batch_size]
            future_to_coord = {
                executor.submit(process_grid_point, lat, lon, API_KEYS[idx % TOTAL_KEYS]): (lat, lon)
                for idx, (lat, lon) in enumerate(batch)
            }

            for future in as_completed(future_to_coord):
                result = future.result()
                if result:
                    writer.writerow(result)
                    total_requests += 1

            print(f"⏳ Processed {total_requests} requests. Sleeping for 60 seconds to respect rate limits...")
            time.sleep(60)  # Enforce rate limit

print(f"✅ AQI data saved to {OUTPUT_CSV_FILE}")


# After all processing is done
end_time = time.time()

# Calculate elapsed time
elapsed_time = end_time - start_time

# Convert to hours, minutes, and seconds
hours, rem = divmod(elapsed_time, 3600)
minutes, seconds = divmod(rem, 60)

print(f"⏱️ Total execution time: {int(hours)}h {int(minutes)}m {seconds:.2f}s")
