import os
import zipfile
from ftplib import FTP

import pandas
import sqlite3

import requests
import sqlalchemy
from rich.progress import track

from logger import log

raw_dir: str = "raw_data"
db_connection_uri: str = "sqlite:///processed_data/data.sqlite"
# db_connection_uri: str = "sqlite:///../data.sqlite"
engine = sqlalchemy.create_engine(db_connection_uri)


def main():
    log("Starting data collector", "info")
    pull_power_data()
    pull_weather_data()
    log("Completed data collection", timestamp=True)


def pull_power_data():
    power_data_src: str = "https://data.open-power-system-data.org/time_series/2020-10-06/time_series.sqlite"

    # Create a raw_data folder if it doesn't exist
    if not os.path.exists("raw_data"):
        os.makedirs("raw_data")

    # Check if power_data.sqlite exists
    if not os.path.exists("raw_data/power_data.sqlite"):

        # Download time_series database for usage in pandas dataframe
        response = requests.get(power_data_src)
        if response.status_code == 200:
            with open("raw_data/power_data.sqlite", "wb") as file:
                file.write(response.content)
            log("Downloaded power_data", "success")
        else:
            log("Could not download power_data", "error")

    else:
        log("Found power_data files (skipping download)", "success")

    # Filter the original dataset to only include the columns we need
    columns: list[str] = [
        "utc_timestamp",
        "cet_cest_timestamp",
        "DE_load_actual_entsoe_transparency",
        "DE_load_forecast_entsoe_transparency",
        "DE_solar_capacity",
        "DE_solar_generation_actual",
        "DE_solar_profile",
        "DE_wind_capacity",
        "DE_wind_generation_actual",
        "DE_wind_profile",
        "DE_wind_offshore_capacity",
        "DE_wind_offshore_generation_actual",
        "DE_wind_offshore_profile",
        "DE_wind_onshore_capacity",
        "DE_wind_onshore_generation_actual",
        "DE_wind_onshore_profile",
        "DE_50hertz_load_actual_entsoe_transparency",
        "DE_50hertz_load_forecast_entsoe_transparency",
        "DE_50hertz_solar_generation_actual",
        "DE_50hertz_wind_generation_actual",
        "DE_50hertz_wind_offshore_generation_actual",
        "DE_50hertz_wind_onshore_generation_actual",
        "DE_LU_load_actual_entsoe_transparency",
        "DE_LU_load_forecast_entsoe_transparency",
        "DE_LU_price_day_ahead",
        "DE_LU_solar_generation_actual",
        "DE_LU_wind_generation_actual",
        "DE_LU_wind_offshore_generation_actual",
        "DE_LU_wind_onshore_generation_actual",
        "DE_amprion_load_actual_entsoe_transparency",
        "DE_amprion_load_forecast_entsoe_transparency",
        "DE_amprion_solar_generation_actual",
        "DE_tennet_load_actual_entsoe_transparency",
        "DE_tennet_load_forecast_entsoe_transparency",
        "DE_tennet_solar_generation_actual",
        "DE_tennet_wind_generation_actual",
        "DE_tennet_wind_offshore_generation_actual",
        "DE_tennet_wind_onshore_generation_actual",
        "DE_transnetbw_load_actual_entsoe_transparency",
        "DE_transnetbw_load_forecast_entsoe_transparency",
        "DE_transnetbw_solar_generation_actual",
        "DE_transnetbw_wind_onshore_generation_actual",
    ]

    query: str = 'SELECT {} FROM time_series_60min_singleindex'.format(", ".join(columns))

    connection = sqlite3.connect("raw_data/power_data.sqlite")
    data_frame = pandas.read_sql_query(query, connection)
    connection.close()

    # Debug print
    # print(data_frame)

    # Create a processed_data folder if it doesn't exist
    if not os.path.exists("processed_data"):
        os.makedirs("processed_data")

    # Save data to a sqlite database
    if sqlalchemy.inspect(engine).has_table("power_data"):
        log(f"Found power_data table (skipping extraction)", "success")
    else:
        data_frame.to_sql("power_data", engine, if_exists="replace", index=False)
        log("Extracted power_data", "success")


def pull_weather_data():
    data_sources = [
        {
            "name": "station_data",
            "path": "climate_environment/CDC/help/RR_Tageswerte_Beschreibung_Stationen.txt",
            "columns": ["Stations_id", "geoBreite", "geoLaenge"]
        },
        {
            "name": "rain_data",
            "path": "climate_environment/CDC/observations_germany/climate/daily/more_precip/historical/",
            "columns": ["STATIONS_ID", "MESS_DATUM", "  RS", " RSF"]
        },
        {
            "name": "cloud_data",
            "path": "climate_environment/CDC/observations_germany/climate/subdaily/cloudiness/historical/",
            "columns": ["STATIONS_ID", "MESS_DATUM", "N_TER", "CD_TER"]
        },
        {
            "name": "temperature_data",
            "path": "climate_environment/CDC/observations_germany/climate/subdaily/air_temperature/historical/",
            "columns": ["STATIONS_ID", "MESS_DATUM", "TT_TER", "RF_TER"]
        },
        {
            "name": "wind_data",
            "path": "climate_environment/CDC/observations_germany/climate/subdaily/wind/historical/",
            "columns": ["STATIONS_ID", "MESS_DATUM", "DK_TER", "FK_TER"]
        }
    ]
    ftp_uri: str = "opendata.dwd.de"

    for data_src in data_sources:
        if os.path.exists(os.path.join(raw_dir, data_src['name'])):
            log(f"Found {data_src['name']} files (skipping download)", "success")
        else:
            download(ftp_uri, data_src['name'], data_src['path'])
            log(f"Downloaded {data_src['name']}", "success")

        if sqlalchemy.inspect(engine).has_table(data_src["name"]):
            log(f"Found {data_src['name']} table (skipping extraction)", "success")
        else:
            extract_data_source(data_src['name'], data_src["columns"])
            log(f"Extracted {data_src['name']}", "success")

    log(f"Pipeline completed", timestamp=True)


def download(ftp_uri: str, data_src_name: str, path: str):
    # Connect to FTP server and navigate to the target directory
    ftp = FTP(ftp_uri)
    ftp.login()
    folder_path, file_name = os.path.split(path)
    ftp.cwd(folder_path)

    # Get a list of all files
    files: list[str] = []
    if file_name is None or file_name == "":
        files = ftp.nlst()
    else:
        files = [file_name]
    raw_data_directory: str = os.path.join(raw_dir, data_src_name)
    os.makedirs(raw_data_directory, exist_ok=True)

    # Download each file
    desc = log(f"Downloading {data_src_name} from server ", "status", ret_str=True)
    progress = track(files, description=desc, transient=True)
    for file in progress:
        local_filename: str = os.path.join(raw_data_directory, file)
        with open(local_filename, "wb") as f:
            def callback(chunk):
                f.write(chunk)

            ftp.retrbinary("RETR " + file, callback)

    # Close FTP connection
    ftp.quit()


def extract_data_source(data_src_name: str, filter_items: list[str]):
    path: str = os.path.join(raw_dir, data_src_name)

    # # Check if folder contains single file (e.g. station_data)
    # files = os.listdir(path)
    # if len(files) == 1:
    #     with open(os.path.join(path, files[0]), "r") as file:
    #         insert_into_db(file, data_src_name, filter_items)
    #     return

    # Get a list of all zip files in the folder
    zip_files = [file for file in os.listdir(path) if file.endswith(".zip")]

    # Iterate over the zips with a progress bar
    desc = log(f"Extracting {data_src_name} into database ", "status", ret_str=True)

    for zip_file in track(zip_files, description=desc, transient=True):
        zip_path: str = os.path.join(path, zip_file)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:

            # Get a list of member files contained in the zip file (excluding metadata files)
            member_files = [member for member in zip_ref.namelist() if not member.startswith("Metadaten_")]

            for member in member_files:
                # Read each member file into a pandas DataFrame
                with zip_ref.open(name=member, mode="r") as tmp_file:
                    insert_into_db(tmp_file, data_src_name, filter_items)


def insert_into_db(tmp_file, data_src_name, filter_items):
    data_frame = pandas.read_csv(tmp_file, sep=";")

    # Remove unnecessary columns from the DataFrame
    data_frame = data_frame.filter(items=filter_items)

    # print(data_frame)

    # Store the data into the SQLiteDB
    data_frame.to_sql(data_src_name, engine, if_exists="replace", index=False)


if __name__ == "__main__":
    main()
