"""
Script to fetch NESO Solar Forecast Data
"""

import urllib.request
import urllib.parse
import json
import pandas as pd


def fetch_data(
    resource_id: str, limit: int, columns: list, rename_columns: dict
) -> pd.DataFrame:
    """
    Fetch data from the NESO API and process it into a Pandas DataFrame.

    Parameters:
        resource_id (str): The unique resource ID for the dataset in the API.
        limit (int): The number of records to fetch.
        columns (list): List of columns to extract from the data.
        rename_columns (dict): Dictionary for renaming columns in the DataFrame.

    Returns:
        pd.DataFrame: Processed DataFrame containing the requested columns with renamed headers.

    Usage Example:
        >>> resource_id = "db6c038f-98af-4570-ab60-24d71ebd0ae5"
        >>> limit = 50
        >>> columns = ['DATE_GMT', 'TIME_GMT', 'EMBEDDED_SOLAR_FORECAST']
        >>> rename_columns = {
        ...     'DATE_GMT': 'start_utc',
        ...     'TIME_GMT': 'end_utc',
        ...     'EMBEDDED_SOLAR_FORECAST': 'solar_forecast_kw'
        ... }
        >>> df = fetch_data(resource_id, limit, columns, rename_columns)
    """

    base_url = "https://api.neso.energy/api/3/action/datastore_search"
    url = f"{base_url}?resource_id={resource_id}&limit={limit}"

    try:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode("utf-8"))

        records = data["result"]["records"]

        df = pd.DataFrame(records)
        df = df[columns]

        df.rename(columns=rename_columns, inplace=True)
        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()


def fetch_data_using_sql(
    sql_query: str, columns: list, rename_columns: dict
) -> pd.DataFrame:
    """
    Fetch data from the NESO API using an SQL query, process it, and return specific columns with renamed headers.

    Parameters:
        sql_query (str): The SQL query to fetch data from the API.
        columns (list): List of columns to extract from the data.
        rename_columns (dict): Dictionary for renaming columns in the DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing the requested columns with renamed headers.

    Usage Example:
        >>> sql_query = 'SELECT * from "db6c038f-98af-4570-ab60-24d71ebd0ae5" LIMIT 5'
        >>> columns = ['DATE_GMT', 'TIME_GMT', 'EMBEDDED_SOLAR_FORECAST']
        >>> rename_columns = {
        ...     'DATE_GMT': 'start_utc',
        ...     'TIME_GMT': 'end_utc',
        ...     'EMBEDDED_SOLAR_FORECAST': 'solar_forecast_kw'
        ... }
        >>> df = fetch_data_using_sql(sql_query, columns, rename_columns)
    """

    base_url = "https://api.neso.energy/api/3/action/datastore_search_sql"
    encoded_query = urllib.parse.quote(sql_query)
    url = f"{base_url}?sql={encoded_query}"

    try:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode("utf-8"))

        records = data["result"]["records"]
        df = pd.DataFrame(records)

        df = df[columns]
        df.rename(columns=rename_columns, inplace=True)

        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()


def test_fetch_functions():
    """
    Test cases for the fetch_data and fetch_data_using_sql functions.
    Validates the functionality with different inputs and checks the correctness of the results.
    """

    resource_id = "db6c038f-98af-4570-ab60-24d71ebd0ae5"
    limit = 5
    columns = ["DATE_GMT", "TIME_GMT", "EMBEDDED_SOLAR_FORECAST"]
    rename_columns = {
        "DATE_GMT": "start_utc",
        "TIME_GMT": "end_utc",
        "EMBEDDED_SOLAR_FORECAST": "solar_forecast_kw",
    }

    sql_query = f'SELECT * from "{resource_id}" LIMIT {limit}'

    print("Testing fetch_data function...")
    df_api = fetch_data(resource_id, limit, columns, rename_columns)
    assert not df_api.empty, "fetch_data returned an empty DataFrame!"
    assert set(df_api.columns) == set(
        rename_columns.values()
    ), "Column names do not match after renaming!"
    print("fetch_data passed the tests.")

    print("Testing fetch_data_using_sql function...")
    df_sql = fetch_data_using_sql(sql_query, columns, rename_columns)
    assert not df_sql.empty, "fetch_data_using_sql returned an empty DataFrame!"
    assert set(df_sql.columns) == set(
        rename_columns.values()
    ), "Column names do not match after renaming!"
    print("fetch_data_using_sql passed the tests.")

    print("Validating data consistency between fetch_data and fetch_data_using_sql...")
    assert df_api.equals(
        df_sql
    ), "Data from fetch_data and fetch_data_using_sql are inconsistent!"
    print("Data consistency test passed.")

test_fetch_functions()
