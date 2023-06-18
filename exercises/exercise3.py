import pandas
import sqlalchemy


def main():
    car_data_src: str = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"

    keep_columns: dict[int, str] = {
        0: "date",
        1: "CIN",
        2: "name",
        12: "petrol",
        22: "diesel",
        32: "gas",
        42: "electro",
        52: "hybrid",
        62: "plugInHybrid",
        72: "others",
    }

    data_frame = pandas.read_csv(car_data_src,
                                 engine="python",
                                 encoding="ISO-8859-1",
                                 delimiter=";",
                                 skiprows=7,
                                 skipfooter=4,
                                 usecols=keep_columns.keys(),
                                 names=keep_columns.values())

    # print(data_frame)

    # date example: 01.01.2022
    data_frame["date"] = pandas.to_datetime(data_frame["date"], format="%d.%m.%Y")

    # CIN example: 12345
    data_frame["CIN"] = data_frame["CIN"].astype(str)
    cin_format = "^[0-9]{5}$"
    data_frame = data_frame[data_frame["CIN"].str.match(cin_format)]

    data_frame["petrol"] = pandas.to_numeric(data_frame["petrol"], downcast="integer", errors="coerce")
    data_frame["diesel"] = pandas.to_numeric(data_frame["diesel"], downcast="integer", errors="coerce")
    data_frame["gas"] = pandas.to_numeric(data_frame["gas"], downcast="integer", errors="coerce")
    data_frame["electro"] = pandas.to_numeric(data_frame["electro"], downcast="integer", errors="coerce")
    data_frame["hybrid"] = pandas.to_numeric(data_frame["hybrid"], downcast="integer", errors="coerce")
    data_frame["plugInHybrid"] = pandas.to_numeric(data_frame["plugInHybrid"], downcast="integer", errors="coerce")
    data_frame["others"] = pandas.to_numeric(data_frame["others"], downcast="integer", errors="coerce")

    # all other columns (apart from date, cin and name) should be a positive integer > 0
    data_frame = data_frame[(data_frame["petrol"] > 0) &
                            (data_frame["diesel"] > 0) &
                            (data_frame["gas"] > 0) &
                            (data_frame["electro"] > 0) &
                            (data_frame["hybrid"] > 0) &
                            (data_frame["plugInHybrid"] > 0) &
                            (data_frame["others"] > 0)]

    data_frame.dropna()  # drop all rows with NaN values

    # print("-------------------- AFTER CLEANING --------------------")
    # print(data_frame)

    engine = sqlalchemy.create_engine("sqlite:///cars.sqlite")
    data_frame.to_sql("cars", engine, if_exists="replace", index=False, dtype={
        "date":         sqlalchemy.types.DATE,
        "CIN":          sqlalchemy.types.TEXT,
        "name":         sqlalchemy.types.TEXT,
        "petrol":       sqlalchemy.types.INTEGER,
        "diesel":       sqlalchemy.types.INTEGER,
        "gas":          sqlalchemy.types.INTEGER,
        "electro":      sqlalchemy.types.INTEGER,
        "hybrid":       sqlalchemy.types.INTEGER,
        "plugInHybrid": sqlalchemy.types.INTEGER,
        "others":       sqlalchemy.types.INTEGER
    })


if __name__ == "__main__":
    main()
