import pandas
import sqlalchemy


def main():
    airports_data_src: str = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"

    data_frame = pandas.read_csv(airports_data_src, delimiter=";")

    # print(data_frame)

    engine = sqlalchemy.create_engine("sqlite:///airports.sqlite")
    data_frame.to_sql("airports", engine, if_exists="replace", index=False, dtype={
        "column_1": sqlalchemy.types.INTEGER,   # Lfd. Nummer
        "column_2": sqlalchemy.types.TEXT,      # Name des Flughafens
        "column_3": sqlalchemy.types.TEXT,      # Ort
        "column_4": sqlalchemy.types.TEXT,      # Land
        "column_5": sqlalchemy.types.TEXT,      # IATA
        "column_6": sqlalchemy.types.TEXT,      # ICAO
        "column_7": sqlalchemy.types.FLOAT,     # Latitude
        "column_8": sqlalchemy.types.FLOAT,     # Longitude
        "column_9": sqlalchemy.types.INTEGER,   # Altitude
        "column_10": sqlalchemy.types.FLOAT,    # Zeitzone
        "column_11": sqlalchemy.types.CHAR,     # DST
        "column_12": sqlalchemy.types.TEXT,     # Zeitzonen-Datenbank
        "geo_punkt": sqlalchemy.types.TEXT,     # geo_punkt
    })


if __name__ == "__main__":
    main()
