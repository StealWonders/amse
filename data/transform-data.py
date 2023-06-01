import pandas
import sqlalchemy

from logger import log

old_db_connection_uri: str = "sqlite:///processed_data/data.sqlite"
new_db_connection_uri: str = "sqlite:///processed_data/transformed_data.sqlite"
old_engine = sqlalchemy.create_engine(old_db_connection_uri)
new_engine = sqlalchemy.create_engine(new_db_connection_uri)


def main():
    log("Starting data transformation", "info")
    transform_cloud_data()
    transform_rain_data()
    transform_temperature_data()
    transform_wind_data()
    insert_power_data()
    log("Finished data transformation", "info")


def transform_cloud_data():
    table_name: str = "cloud_data"
    data_frame = pandas.read_sql_table(table_name, old_engine)

    # Remove unnecessary entries (MESS_DATUM < 2010)
    data_frame = data_frame[data_frame["MESS_DATUM"] >= 2010010100]  # with hour

    new_column_names = {
        "STATIONS_ID": "station_id",
        "MESS_DATUM":  "date",
        "N_TER":       "cloud_cover",
        "CD_TER":      "cloud_cover_form",
    }

    data_frame = data_frame.rename(columns=new_column_names)

    # Convert date column to datetime
    data_frame["date"] = pandas.to_datetime(data_frame["date"], format="%Y%m%d%H")  # with hour

    data_frame.to_sql(table_name, new_engine, if_exists="replace", index=False, dtype={
        "station_id":   sqlalchemy.types.INTEGER,
        "date":         sqlalchemy.types.DATETIME,
        "cloud_cover":  sqlalchemy.types.INTEGER,    # Percent
        "cloud_density": sqlalchemy.types.INTEGER,   # ???
    })
    log("Transformed cloud_data", "info")


def transform_rain_data():
    table_name: str = "rain_data"
    data_frame = pandas.read_sql_table(table_name, old_engine)

    # Remove unnecessary entries (MESS_DATUM < 2010)
    data_frame = data_frame[data_frame["MESS_DATUM"] >= 20100101]  # no hour

    new_column_names = {
        "STATIONS_ID": "station_id",
        "MESS_DATUM":  "date",
        "  RS":        "rain",
        " RSF":        "rain_form",
        "SH_TAG":      "snow_height",
        "NSH_TAG":     "new_snow_height",
    }

    data_frame = data_frame.rename(columns=new_column_names)

    # Convert date column to datetime
    data_frame["date"] = pandas.to_datetime(data_frame["date"], format="%Y%m%d")  # no hour

    data_frame.to_sql(table_name, new_engine, if_exists="replace", index=False, dtype={
        "station_id":      sqlalchemy.types.INTEGER,
        "date":            sqlalchemy.types.DATETIME,
        "rain":            sqlalchemy.types.FLOAT,       # mm
        "rain_form":       sqlalchemy.types.INTEGER,     # 0-9
        "snow_height":     sqlalchemy.types.INTEGER,     # cm
        "new_snow_height": sqlalchemy.types.INTEGER      # cm
    })
    log("Transformed rain_data", "info")


def transform_temperature_data():
    table_name: str = "temperature_data"
    data_frame = pandas.read_sql_table(table_name, old_engine)

    # Remove unnecessary entries (MESS_DATUM < 2010)
    data_frame = data_frame[data_frame["MESS_DATUM"] >= 2000000000]  # with hour

    # print amount of rows
    print(len(data_frame))

    new_column_names = {
        "STATIONS_ID": "station_id",
        "MESS_DATUM":  "date",
        "TT_TER":      "temperature",
        "RF_TER":      "humidity",
    }

    data_frame = data_frame.rename(columns=new_column_names)

    # Convert date column to datetime
    data_frame["date"] = pandas.to_datetime(data_frame["date"], format="%Y%m%d%H")  # with hour

    data_frame.to_sql(table_name, new_engine, if_exists="replace", index=False, dtype={
        "station_id":  sqlalchemy.types.INTEGER,
        "date":        sqlalchemy.types.DATETIME,
        "temperature": sqlalchemy.types.FLOAT,      # Degrees Celsius
        "humidity":    sqlalchemy.types.FLOAT,      # Percent
    })
    log("Transformed temperature_data", "info")


def transform_wind_data():
    table_name: str = "wind_data"
    data_frame = pandas.read_sql_table(table_name, old_engine)

    # Remove unnecessary entries (MESS_DATUM < 2010)
    data_frame = data_frame[data_frame["MESS_DATUM"] >= 2010010100]  # with hour

    new_column_names = {
        "STATIONS_ID": "station_id",
        "MESS_DATUM":  "date",
        "DK_TER":      "direction",
        "FK_TER":      "speed",
    }

    data_frame = data_frame.rename(columns=new_column_names)

    # Convert date column to datetime
    data_frame["date"] = pandas.to_datetime(data_frame["date"], format="%Y%m%d%H")  # with hour

    data_frame.to_sql(table_name, new_engine, if_exists="replace", index=False, dtype={
        "station_id": sqlalchemy.types.INTEGER,
        "date":       sqlalchemy.types.DATETIME,
        "direction":  sqlalchemy.types.INTEGER,    # Degrees (0-360)
        "speed":      sqlalchemy.types.INTEGER,    # Bft (0-12)
    })
    log("Transformed wind_data", "info")


def insert_power_data():
    table_name: str = "power_data"
    data_frame = pandas.read_sql_table(table_name, old_engine)
    data_frame.to_sql(table_name, new_engine, if_exists="replace", index=False)
    log("Inserted power_data", "info")


if __name__ == "__main__":
    main()
