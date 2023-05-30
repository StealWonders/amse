import os
import sqlite3
import zipfile
from ftplib import FTP

import pandas
import requests
import sqlalchemy
from rich.progress import track

from logger import log

raw_data_dir: str = "raw_data"
processed_data_dir: str = "processed_data"

db_connection_uri: str = "sqlite:///processed_data/data.sqlite"
# db_connection_uri: str = "sqlite:///../data.sqlite"
engine = sqlalchemy.create_engine(db_connection_uri)


def main():
    log("Starting data collector", "info")

    # Create a raw_data folder if it doesn't exist
    if not os.path.exists(raw_data_dir):
        os.makedirs(raw_data_dir)

    # Create a processed_data folder if it doesn't exist
    if not os.path.exists(processed_data_dir):
        os.makedirs(processed_data_dir)

    pull_power_data()
    pull_station_date()
    pull_weather_data()
    log("Completed data collection", timestamp=True)


def pull_power_data():
    power_data_src: str = "https://data.open-power-system-data.org/time_series/2020-10-06/time_series.sqlite"

    # Only download the power_data if it doesn't exist
    data_src_path: str = os.path.join(raw_data_dir, "power_data.sqlite")
    if not os.path.exists(data_src_path):
        response = requests.get(power_data_src)
        if response.status_code == 200:
            with open(data_src_path, "wb") as file:
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

    # Query our data and save it to a dataframe
    connection = sqlite3.connect(data_src_path)
    data_frame = pandas.read_sql_query(query, connection)
    connection.close()

    # Debug print
    # print(data_frame)

    # Save the data to a sqlite database if the table doesn't already exist
    if sqlalchemy.inspect(engine).has_table("power_data"):
        log(f"Found power_data table (skipping extraction)", "success")
    else:
        data_frame.to_sql("power_data", engine, if_exists="replace", index=False)
        log("Extracted power_data", "success")


def pull_station_date():
    station_data_src: str = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/subdaily/wind/historical/FK_Terminwerte_Beschreibung_Stationen.txt"

    # Only download the station_data if it doesn't exist
    data_src_path: str = os.path.join(raw_data_dir, "station_data.txt")
    if not os.path.exists(data_src_path):
        response = requests.get(station_data_src)
        if response.status_code == 200:
            with open(data_src_path, "wb") as file:
                file.write(response.content.decode("ISO-8859-1").encode("UTF-8"))  # Convert to UTF-8 encoding
                log("Downloaded station_data", "success")
        else:
            log("Could not download station_data", "error")
    else:
        log("Found station_data files (skipping download)", "success")

    # Insert the data into the database only if the table doesn't already exist
    if sqlalchemy.inspect(engine).has_table("station_data"):
        log("Found station_data table (skipping extraction)", "success")
        return

    # Extract the data from the file
    column_widths = [(0, 5), (6, 14), (15, 23), (24, 38), (39, 50), (51, 60), (61, 101), (102, None)]
    column_names = ["station_id", "von_datum", "bis_datum", "station_height", "latitude", "longitude", "station_name", "state"]

    # Read the fixed-width file into a DataFrame
    data_frame = pandas.read_fwf(data_src_path, colspecs=column_widths, names=column_names, skiprows=2)
    data_frame.to_sql("station_data", engine, if_exists="replace", index=False)

    log("Extracted station_data", "success")


def pull_weather_data():
    data_sources = [
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
        if os.path.exists(os.path.join(raw_data_dir, data_src['name'])):
            log(f"Found {data_src['name']} files (skipping download)", "success")
        else:
            download(ftp_uri, data_src['name'], data_src['path'])
            log(f"Downloaded {data_src['name']}", "success")

        if sqlalchemy.inspect(engine).has_table(data_src["name"]):
            log(f"Found {data_src['name']} table (skipping extraction)", "success")
        else:
            extract_data_source(data_src['name'], data_src["columns"])
            log(f"Extracted {data_src['name']}", "success")


def download(ftp_uri: str, data_src_name: str, path: str):
    ftp = FTP(ftp_uri)
    ftp.login()

    folder_path, file_name = os.path.split(path)
    ftp.cwd(folder_path)

    # Get a list of all files
    if file_name is None or file_name == "":
        files = ftp.nlst()
    else:
        log(f"Can not download single file: {file_name}", "error")
        ftp.quit()
        return

    # Create a folder named after the data source if it doesn't already exist
    data_src_dir: str = os.path.join(raw_data_dir, data_src_name)
    print(data_src_dir)
    if not os.path.exists(data_src_dir):
        os.mkdir(data_src_dir)

    # todo: filter files only within the last x years

    # Download each file
    desc: str = log(f"Downloading {data_src_name} from server", "status", ret_str=True)
    progress = track(files, description=desc, transient=True)
    for file in progress:
        local_filename: str = os.path.join(data_src_dir, file)
        with open(local_filename, "wb") as f:
            def callback(chunk):
                f.write(chunk)

            ftp.retrbinary("RETR " + file, callback)

    # Close FTP connection
    ftp.quit()


def extract_data_source(data_src_name: str, filter_items: list[str]):
    path: str = os.path.join(raw_data_dir, data_src_name)

    # Get a list of all zip files in the folder
    zip_files = [file for file in os.listdir(path) if file.endswith(".zip")]

    # Iterate over the zips with a progress bar
    desc = log(f"Extracting {data_src_name} into database", "status", ret_str=True)

    for zip_file in track(zip_files, description=desc, transient=True):
        zip_path: str = os.path.join(path, zip_file)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:

            # Get a list of member files contained in the zip file (excluding metadata files)
            member_files = [member for member in zip_ref.namelist() if not member.startswith("Metadaten_")]

            # Read each member file into a pandas DataFrame
            for member in member_files:
                with zip_ref.open(name=member, mode="r") as tmp_file:
                    data_frame = pandas.read_csv(tmp_file, sep=";")

                    # Remove unnecessary columns from the DataFrame
                    data_frame = data_frame.filter(items=filter_items)

                    # print(data_frame)

                    # Store the data into the SQLiteDB
                    data_frame.to_sql(data_src_name, engine, if_exists="replace", index=False)


if __name__ == "__main__":
    main()
