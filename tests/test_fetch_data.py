"""
Test Suite for `fetch_data` and `fetch_data_using_sql` Functions

This script validates the functionality and consistency of two data-fetching functions:
- `fetch_data`: Fetches data via API.
- `fetch_data_using_sql`: Fetches data via SQL query.

The tests ensure the data returned by both methods:
1. Is correctly processed.
2. Contains the expected columns.
3. Is consistent between the two methods.

### How to Run the Tests:

Run the entire test suite:
    pytest tests/test_fetch_data.py

Run a specific test:
    pytest tests/test_fetch_data.py::test_fetch_data_api

Run with verbose output:
    pytest tests/test_fetch_data.py -v

Run tests matching a specific pattern:
    pytest tests/test_fetch_data.py -k "fetch_data"
"""

from neso_solar_consumer.fetch_data import fetch_data, fetch_data_using_sql


def test_fetch_data_api(test_config):
    """
    Test the `fetch_data` function to ensure it fetches and processes data correctly via API.

    Parameters:
        test_config (dict): A dictionary containing the test configuration, including:
            - `resource_id` (str): The unique resource ID for the dataset in the API.
            - `limit` (int): The number of records to fetch.

    Assertions:
        - The DataFrame is not empty.
        - The DataFrame contains the expected columns: `Datetime_GMT`, `solar_forecast_kw`.
    """
    df_api = fetch_data(
        test_config["resource_id"],
        test_config["limit"],
    )
    assert not df_api.empty, "fetch_data returned an empty DataFrame!"
    assert set(df_api.columns) == {"Datetime_GMT", "solar_forecast_kw"}, "Column names do not match the expected structure!"


def test_fetch_data_sql(test_config):
    """
    Test the `fetch_data_using_sql` function to ensure it fetches and processes data correctly via SQL.

    Parameters:
        test_config (dict): A dictionary containing the test configuration, including:
            - `resource_id` (str): The unique resource ID for the dataset in the API.
            - `limit` (int): The number of records to fetch.

    Assertions:
        - The DataFrame is not empty.
        - The DataFrame contains the expected columns: `Datetime_GMT`, `solar_forecast_kw`.
    """
    sql_query = (
        f'SELECT * FROM "{test_config["resource_id"]}" LIMIT {test_config["limit"]}'
    )
    df_sql = fetch_data_using_sql(sql_query)
    assert not df_sql.empty, "fetch_data_using_sql returned an empty DataFrame!"
    assert set(df_sql.columns) == {"Datetime_GMT", "solar_forecast_kw"}, "Column names do not match the expected structure!"


def test_data_consistency(test_config):
    """
    Validate that the data fetched by `fetch_data` and `fetch_data_using_sql` are consistent.

    Parameters:
        test_config (dict): A dictionary containing the test configuration, including:
            - `resource_id` (str): The unique resource ID for the dataset in the API.
            - `limit` (int): The number of records to fetch.

    Assertions:
        - The data fetched by `fetch_data` matches the data fetched by `fetch_data_using_sql`.
    """
    sql_query = (
        f'SELECT * FROM "{test_config["resource_id"]}" LIMIT {test_config["limit"]}'
    )
    df_api = fetch_data(
        test_config["resource_id"],
        test_config["limit"],
    )
    df_sql = fetch_data_using_sql(sql_query)
    assert df_api.equals(df_sql), "Data from fetch_data and fetch_data_using_sql are inconsistent!"
