import os
import urllib.request
import zipfile
from http.client import HTTPMessage

import pandas
import sqlalchemy


def main():
    bus_data_src: str = " https://gtfs.rhoenenergie-bus.de/GTFS.zip"
    bus_data_file: str = "stops.txt"
    result: tuple[str, HTTPMessage] = urllib.request.urlretrieve(bus_data_src)
    with zipfile.ZipFile(result[0], "r") as zip_ref:
        zip_ref.extract(bus_data_file)

    keep_columns: dict[str, any] = {
        "stop_id": int,
        "stop_name": str,
        "stop_lat": float,
        "stop_lon": float,
        "zone_id": int,
    }

    data_frame = pandas.read_csv(bus_data_file,
                                 encoding="utf-8",
                                 delimiter=",",
                                 usecols=keep_columns.keys(),
                                 dtype=keep_columns)

    os.remove(bus_data_file)  # clean up

    data_frame = data_frame.dropna()  # drop all rows with NaN values
    data_frame = data_frame[data_frame["zone_id"] == 2001]  # only keep stops from zone 2001

    # stop_lat/stop_lon must be a geographic coordinate between -90 and 90
    data_frame = data_frame[(data_frame["stop_lat"] >= -90) & (data_frame["stop_lat"] <= 90)]
    data_frame = data_frame[(data_frame["stop_lon"] >= -90) & (data_frame["stop_lon"] <= 90)]

    engine = sqlalchemy.create_engine("sqlite:///gtfs.sqlite")
    data_frame.to_sql("stops", engine, if_exists="replace", index=False)


if __name__ == "__main__":
    main()
