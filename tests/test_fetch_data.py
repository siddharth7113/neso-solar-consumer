from neso_solar_consumer.fetch_data import fetch_data, fetch_data_using_sql


def test_fetch_data_api(test_config):
    """
    Test the fetch_data function to ensure it fetches and processes data correctly via API.
    """
    df_api = fetch_data(
        test_config["resource_id"],
        test_config["limit"],
        test_config["columns"],
        test_config["rename_columns"],
    )
    assert not df_api.empty, "fetch_data returned an empty DataFrame!"
    assert set(df_api.columns) == set(
        test_config["rename_columns"].values()
    ), "Column names do not match after renaming!"


def test_fetch_data_sql(test_config):
    """
    Test the fetch_data_using_sql function to ensure it fetches and processes data correctly via SQL.
    """
    sql_query = f'SELECT * FROM "{test_config["resource_id"]}" LIMIT {test_config["limit"]}'
    df_sql = fetch_data_using_sql(sql_query, test_config["columns"], test_config["rename_columns"])
    assert not df_sql.empty, "fetch_data_using_sql returned an empty DataFrame!"
    assert set(df_sql.columns) == set(
        test_config["rename_columns"].values()
    ), "Column names do not match after renaming!"


def test_data_consistency(test_config):
    """
    Validate that the data fetched by fetch_data and fetch_data_using_sql are consistent.
    """
    sql_query = f'SELECT * FROM "{test_config["resource_id"]}" LIMIT {test_config["limit"]}'
    df_api = fetch_data(
        test_config["resource_id"],
        test_config["limit"],
        test_config["columns"],
        test_config["rename_columns"],
    )
    df_sql = fetch_data_using_sql(sql_query, test_config["columns"], test_config["rename_columns"])
    assert df_api.equals(df_sql), "Data from fetch_data and fetch_data_using_sql are inconsistent!"
