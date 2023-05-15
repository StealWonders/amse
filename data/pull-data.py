import os
import pandas
import sqlite3

import requests


def main():

    print("Pulling data...")

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
            print("Power data downloaded successfully")
        else:
            print("Error: could not download power data")

    else:
        print("Power data already exists (skipping download)")

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
    print(data_frame)

    # Create a processed_data folder if it doesn't exist
    if not os.path.exists("processed_data"):
        os.makedirs("processed_data")

    # Save data to a sqlite database
    data_frame.to_sql("power_data", sqlite3.connect("processed_data/power_data.sqlite"), if_exists="replace")


if __name__ == "__main__":
    main()
