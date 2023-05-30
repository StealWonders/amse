import os


def test_file_existence():
    # Check for DWD files
    assert os.path.exists("raw_data/cloud_data") is True
    assert os.path.exists("raw_data/rain_data") is True
    assert os.path.exists("raw_data/temperature_data") is True
    assert os.path.exists("raw_data/wind_data") is True

    # Check for open power system database
    assert os.path.exists("raw_data/power_data.sqlite") is True
    assert os.path.getsize("raw_data/power_data.sqlite") > 0

    # Check if the pipeline created the processed data directory and the database
    assert os.path.exists("processed_data/data.sqlite") is True
    assert os.path.getsize("processed_data/data.sqlite") > 0
