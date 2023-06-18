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

    col_types = {
        "date": str,
        "CIN": str,
        "name": str,
    }

    data_frame = pandas.read_csv(car_data_src,
                                 engine="python",
                                 encoding="ISO-8859-1",
                                 delimiter=";",
                                 skiprows=7,
                                 skipfooter=4,
                                 usecols=keep_columns.keys(),
                                 names=keep_columns.values(),
                                 dtype=col_types)

    # print(data_frame)

    # CIN example: 12345
    data_frame["CIN"] = data_frame["CIN"].astype(str)
    cin_format = "^[0-9]{5}$"
    data_frame = data_frame[data_frame["CIN"].str.match(cin_format)]

    # all other columns (apart from date, cin and name) should be a positive integer > 0
    numeric_columns = ["petrol", "diesel", "gas", "electro", "hybrid", "plugInHybrid", "others"]
    data_frame[numeric_columns] = data_frame[numeric_columns].apply(pandas.to_numeric, downcast="integer", errors="coerce")
    data_frame = data_frame[(data_frame[numeric_columns] > 0).all(axis=1)]

    data_frame.dropna()  # drop all rows with NaN values

    # print("-------------------- AFTER CLEANING --------------------")
    # print(data_frame)

    engine = sqlalchemy.create_engine("sqlite:///cars.sqlite")
    data_frame.to_sql("cars", engine, if_exists="replace", index=False, dtype={
        "petrol":       sqlalchemy.types.INTEGER,
        "diesel":       sqlalchemy.types.INTEGER,
        "gas":          sqlalchemy.types.INTEGER,
        "electro":      sqlalchemy.types.INTEGER,
        "hybrid":       sqlalchemy.types.INTEGER,
        "plugInHybrid": sqlalchemy.types.INTEGER,
        "others":       sqlalchemy.types.INTEGER,
    })


if __name__ == "__main__":
    main()
