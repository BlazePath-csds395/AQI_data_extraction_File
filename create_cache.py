import requests
import csv
import time

# Define region and grid size
LAT_MIN, LAT_MAX = 24.0, 50.0
LON_MIN, LON_MAX = -125.0, -67.0
GRID_SIZE = 0.5
OUTPUT_FILE = "land_water_map.csv"

NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
HEADERS = {
    "User-Agent": "MyGISApp/1.0 (your-email@example.com)"  # Replace with your actual email
}

def check_if_land(lat, lon, retries=3, delay=1.5):
    """Determines if a coordinate is on land using Nominatim API, handling rate limits and retries."""
    url = f"{NOMINATIM_URL}?format=json&lat={lat}&lon={lon}&zoom=10"

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            
            # Handle rate limit (HTTP 429)
            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", delay))
                print(f"⚠️ Rate limit hit! Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue  # Retry request

            # If request is successful
            if response.status_code == 200:
                data = response.json()
                if "address" in data:
                    address = data["address"]
                    return any(key in address for key in ["country", "city", "town", "village", "hamlet"])  # True = land
            
        except requests.RequestException as e:
            print(f"⚠️ Attempt {attempt + 1}/{retries} failed for ({lat}, {lon}): {e}")

        time.sleep(delay)  # Wait before retrying

    print(f"⚠️ All retries failed for ({lat}, {lon}). Assuming water.")
    return False  # Default to water if all attempts fail

# Open CSV file and write header
with open(OUTPUT_FILE, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Latitude", "Longitude", "Is_Land"])  # Header

    for lat in range(int(LAT_MIN / GRID_SIZE), int(LAT_MAX / GRID_SIZE) + 1):
        for lon in range(int(LON_MIN / GRID_SIZE), int(LON_MAX / GRID_SIZE) + 1):
            lat_val = lat * GRID_SIZE
            lon_val = lon * GRID_SIZE
            is_land = check_if_land(lat_val, lon_val)

            writer.writerow([lat_val, lon_val, is_land])
            print(f"✅ Checked ({lat_val}, {lon_val}) - Is Land: {is_land}")

            time.sleep(1.5)  # Respect API rate limits

print(f"✅ Land/Water data saved to {OUTPUT_FILE}")
