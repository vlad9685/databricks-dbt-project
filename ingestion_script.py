import requests
import os
import zipfile
from io import BytesIO
import pandas as pd
import json

# CONFIGURATION

# Create a directory to store raw data if it doesn't exist
output_dir = 'raw_data'
os.makedirs(output_dir, exist_ok=True)

# Define the year and month for the data

YEAR = 2024
MONTH = 4

# Create a string version with a leading zero for use in URLs
MONTH_STR = f"{MONTH:02d}"

print("Setup complete. Ready to define ingestion functions.")

# Ingest from API
def ingest_weather_data():
    print(f"Fetching weather data for NYC ({YEAR}-{MONTH_STR})...")

    # Define the start and end dates for the month
    start_date = f"{YEAR}-{MONTH_STR}-01"
    end_date = pd.to_datetime(start_date).to_period('M').end_time.strftime('%Y-%m-%d')

    # The URL for the API endpoint
    url = "https://archive-api.open-meteo.com/v1/archive"

    # Programmatically create the 'daily' parameter string
    daily_params = [
        "weathercode",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "snowfall_sum"
    ]

    # Parameters to tell the API what data we want
    params = {
        "latitude": 40.71,
        "longitude": -74.01,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(daily_params), # Join list into comma-separated string
        "timezone": "America/New_York"
    }

    try:
        # Make the API request
        response = requests.get(url, params=params)
        # Error if request failed
        response.raise_for_status()
        # Parse the JSON response into a dictionary
        data = response.json()
        # Define the output file path
        file_path = os.path.join(output_dir, f"nyc_weather_{YEAR}_{MONTH_STR}.json")
        # Open the file in write mode and save the JSON data
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)

        print(f"Weather data saved to {file_path}")

    except requests.exceptions.RequestException as e:
        # This block catches any errors during the request
        print(f"Error fetching weather data: {e}")

# Ingest from CSV: NYC Citi Bike Data (ZIP)
def ingest_citibike_csv_data():
    print(f"Downloading Citi Bike data for NYC ({YEAR}-{MONTH_STR})...")
    url = f"https://s3.amazonaws.com/tripdata/{YEAR}{MONTH_STR}-citibike-tripdata.zip"

    output_filename = f"citibike_tripdata_{YEAR}_{MONTH_STR}.csv"
    output_path = os.path.join(output_dir, output_filename)

    try:
        # Download the ZIP file
        print(f"Downloading from {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        # Treat the downloaded content as a in memory file
        zip_file = zipfile.ZipFile(BytesIO(response.content))
        # Get the name of the first file in the zip
        csv_filename_in_zip = zip_file.namelist()[0]
        # Extract the CSV file and save it
        with zip_file.open(csv_filename_in_zip) as zf, open(output_path, 'wb') as f:
            f.write(zf.read())
        print(f"Citi Bike data saved to {output_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading Citi Bike data: {e}")
    except Exception as e:
        print(f"An error occurred during Citi Bike data processing: {e}")

# Ingest from Parquet: NYC Yellow Taxi Data
def ingest_taxi_parquet_data():
    print(f"Downloading Yellow Taxi Paruet data for {YEAR}-{MONTH_STR}...")
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{YEAR}-{MONTH_STR}.parquet"

    output_filename = f"yellow_tripdata_{YEAR}_{MONTH_STR}.parquet"
    output_path = os.path.join(output_dir, output_filename)

    try:
        print(f"Downloading from {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Yellow Taxi data saved to {output_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading Yellow Taxi data: {e}")
    except Exception as e:
        print(f"An error occurred during Yellow Taxi data processing: {e}")
        
# Main execution block
if __name__ == "__main__":
    ingest_weather_data()
    ingest_citibike_csv_data()
    ingest_taxi_parquet_data()
    print("Data ingestion complete.")