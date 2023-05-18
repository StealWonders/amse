import pandas
import sqlalchemy


def main():
    airports_data_src: str = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"

    data_frame = pandas.read_csv(airports_data_src, delimiter=";")

    # print(data_frame)

    engine = sqlalchemy.create_engine("sqlite:///airports.db")
    data_frame.to_sql("airports", engine, if_exists="replace", index=False, dtype={
        "column_1": sqlalchemy.types.INTEGER,
        "column_2": sqlalchemy.types.TEXT,
        "column_3": sqlalchemy.types.TEXT,
        "column_4": sqlalchemy.types.TEXT,
        "column_5": sqlalchemy.types.TEXT,
        "column_6": sqlalchemy.types.TEXT,
        "column_7": sqlalchemy.types.FLOAT,
        "column_8": sqlalchemy.types.FLOAT,
        "column_9": sqlalchemy.types.INTEGER,
        "column_10": sqlalchemy.types.FLOAT,
        "column_11": sqlalchemy.types.CHAR,
        "column_12": sqlalchemy.types.TEXT,
        "geo_punkt": sqlalchemy.types.TEXT,
    })


if __name__ == "__main__":
    main()
