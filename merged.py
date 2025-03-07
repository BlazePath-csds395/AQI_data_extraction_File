# file to create the cache for checking land or water for each coordinate
import pandas as pd
import numpy as np

# Load the CSV file
df = pd.read_csv('aqi_data_parallel_with_land_check.csv')

# Select only the required columns
selected_columns = df[['Latitude', 'Longitude', 'Is_Land']]

# Define grid parameters
LAT_MIN, LAT_MAX = 24.0, 50.0
LON_MIN, LON_MAX = -125.0, -67.0
GRID_SIZE = 0.5

# Generate grid points
latitudes = np.arange(LAT_MIN, LAT_MAX + GRID_SIZE, GRID_SIZE)
longitudes = np.arange(LON_MIN, LON_MAX + GRID_SIZE, GRID_SIZE)

grid = [(lat, lon) for lat in latitudes for lon in longitudes]
df_grid = pd.DataFrame(grid, columns=['Latitude', 'Longitude'])

# Merge with existing data to fill Is_Land values
df_grid = df_grid.merge(selected_columns, on=['Latitude', 'Longitude'], how='left')
df_grid['Is_Land'].fillna(False, inplace=True)

# Save the updated CSV
df_grid.to_csv('updated_output_new_.csv', index=False)

print("Updated data saved to updated_output.csv")
