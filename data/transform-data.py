import pandas
import sqlalchemy

from logger import log

old_db_connection_uri: str = "sqlite:///processed_data/data.sqlite"
new_db_connection_uri: str = "sqlite:///processed_data/transformed_data.sqlite"
old_engine = sqlalchemy.create_engine(old_db_connection_uri)
new_engine = sqlalchemy.create_engine(new_db_connection_uri)


def main():
    log("Starting data transformation", "info")
    transform_temperature_data()


def transform_temperature_data():
    table_name: str = "temperature_data"
    data_frame = pandas.read_sql_table(table_name, old_engine)

    new_column_names = {
        "STATIONS_ID": "station_id",
        "MESS_DATUM":  "date",
        "TT_TER":      "temperature",
        "RF_TER":      "humidity",
    }

    data_frame = data_frame.rename(columns=new_column_names)

    # Convert date column to datetime
    data_frame["date"] = pandas.to_datetime(data_frame["date"], format="%Y%m%d%H")

    data_frame.to_sql(table_name, new_engine, if_exists="replace", index=False, dtype={
        "station_id":  sqlalchemy.types.INTEGER,
        "date":        sqlalchemy.types.DATETIME,
        "temperature": sqlalchemy.types.FLOAT,
        "humidity":    sqlalchemy.types.FLOAT,
    })
    log("Transformed temperature_data", "info")


if __name__ == "__main__":
    main()
