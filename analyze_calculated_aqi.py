import pandas as pd

df = pd.read_csv("final_aqi_results.csv")

# Overall AQI statistics
print("AQI Summary Stats:")
print(df["Calculated_AQI"].describe())