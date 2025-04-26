import pandas as pd

# === 1. Load CSV ===
df = pd.read_csv("aqi_data_parallel_optimized_13_apr.csv")  # Replace with actual path to your CSV

# === 2. Convert O₃ from µg/m³ to ppm ===
# Using formula: ppm = (µg/m³ * 24.45) / (1000 * molecular weight)
# O₃ molecular weight = 48 → ppm ≈ µg/m³ * 0.000509
df["O3_ppm"] = df["O3"] * 0.000509

# === 3. AQI Breakpoints from EPA Table 6 ===
breakpoints = {
    "PM2.5": [(0.0, 12.0, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
              (55.5, 125.4, 151, 200), (125.5, 225.4, 201, 300),
              (225.5, 325.4, 301, 400), (325.5, 500.4, 401, 500)],
    "PM10": [(0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150),
             (255, 354, 151, 200), (355, 424, 201, 300),
             (425, 504, 301, 400), (505, 604, 401, 500)],
    "CO": [(0.0, 4.4, 0, 50), (4.5, 9.4, 51, 100), (9.5, 12.4, 101, 150),
           (12.5, 15.4, 151, 200), (15.5, 30.4, 201, 300),
           (30.5, 40.4, 301, 400), (40.5, 50.4, 401, 500)],
    "SO2": [(0, 35, 0, 50), (36, 75, 51, 100), (76, 185, 101, 150),
            (186, 304, 151, 200), (305, 604, 201, 300),
            (605, 804, 301, 400), (805, 1004, 401, 500)],
    "NO2": [(0, 53, 0, 50), (54, 100, 51, 100), (101, 360, 101, 150),
            (361, 649, 151, 200), (650, 1249, 201, 300),
            (1250, 1649, 301, 400), (1650, 2049, 401, 500)],
    "O3_ppm": [(0.000, 0.054, 0, 50), (0.055, 0.070, 51, 100), (0.071, 0.085, 101, 150),
               (0.086, 0.105, 151, 200), (0.106, 0.200, 201, 300),
               (0.201, 0.404, 301, 500)]
}

# === 4. AQI Category Labels ===
aqi_categories = [
    (0, 50, "Good"),
    (51, 100, "Moderate"),
    (101, 150, "Unhealthy for Sensitive Groups"),
    (151, 200, "Unhealthy"),
    (201, 300, "Very Unhealthy"),
    (301, 500, "Hazardous")
]

# === 5. Calculate AQI for a pollutant ===
def calculate_individual_aqi(conc, bps):
    for c_low, c_high, i_low, i_high in bps:
        if c_low <= conc <= c_high:
            return round(((i_high - i_low) / (c_high - c_low)) * (conc - c_low) + i_low)
    return None

# === 6. Get AQI Category Label ===
def get_aqi_category(aqi):
    for low, high, label in aqi_categories:
        if low <= aqi <= high:
            return label
    return "Beyond AQI Scale"

# === 7. Compute AQI Row-Wise ===
def compute_aqi(row):
    aqi_values = {}
    for pollutant in ["PM2.5", "PM10", "CO", "SO2", "NO2", "O3_ppm"]:
        conc = row.get(pollutant)
        if pd.notna(conc):
            aqi = calculate_individual_aqi(conc, breakpoints[pollutant])
            if aqi is not None:
                aqi_values[pollutant] = aqi
    if aqi_values:
        main = max(aqi_values, key=aqi_values.get)
        max_aqi = aqi_values[main]
        category = get_aqi_category(max_aqi)
        return pd.Series([max_aqi, main, category])
    return pd.Series([None, None, None])

# === 8. Apply and Output ===
df[["Calculated_AQI", "Main_Pollutant", "AQI_Category"]] = df.apply(compute_aqi, axis=1)

# Save final CSV
df.to_csv("final_aqi_results.csv", index=False)
print("✅ AQI calculated successfully and saved to 'final_aqi_results.csv'")
