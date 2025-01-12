from neso_solar_consumer.fetch_data import fetch_data
from neso_solar_consumer.format_forecast import format_to_forecast_sql


def test_format_to_forecast_sql_real(db_session, test_config):
    """
    Test `format_to_forecast_sql` with real data fetched from the NESO API.

    Steps:
    1. Fetch data from the NESO API.
    2. Format the data into a ForecastSQL object.
    3. Validate the content and consistency of the ForecastSQL object.
    """
    # Step 1: Fetch data from the API
    data = fetch_data(
        test_config["resource_id"],
        test_config["limit"],
    )
    assert not data.empty, "fetch_data returned an empty DataFrame!"
    assert set(data.columns) == {
        "Datetime_GMT",
        "solar_forecast_kw",
    }, "Unexpected DataFrame columns!"

    # Step 2: Format the data into a ForecastSQL object
    forecasts = format_to_forecast_sql(
        data, test_config["model_name"], test_config["model_version"], db_session
    )
    assert len(forecasts) == 1, f"Expected 1 ForecastSQL object, got {len(forecasts)}"

    # Step 3: Validate the ForecastSQL content
    forecast = forecasts[0]

    # Ensure the number of `ForecastValue` entries matches the number of rows in the input data
    assert len(forecast.forecast_values) == len(
        data
    ), f"Mismatch in ForecastValue entries! Expected {len(data)} but got {len(forecast.forecast_values)}."

    # Validate individual `ForecastValue` entries
    for fv, (_, row) in zip(forecast.forecast_values, data.iterrows()):
        assert (
            fv.target_time == row["Datetime_GMT"]
        ), f"Mismatch in target_time for row {row}"
        expected_power_mw = row["solar_forecast_kw"] / 1000  # Convert kW to MW
        assert fv.expected_power_generation_megawatts == expected_power_mw, (
            f"Mismatch in expected_power_generation_megawatts for row {row}. "
            f"Expected {expected_power_mw}, got {fv.expected_power_generation_megawatts}."
        )
