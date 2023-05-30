Set-Location -Path "data" -ErrorAction Stop

python pull-data.py
pytest tests/test_pipeline.py
