"""
Integration Test for Fetching, Formatting, and Saving Forecast Data

This test validates the complete pipeline:
1. Fetch data from the NESO API.
2. Format the data into `ForecastSQL` objects.
3. Save the formatted forecasts to the database.
4. Verify that the forecasts were correctly saved.

### How to Run
Execute this test using `pytest`:
    pytest tests/test_real_forecasts.py

To view detailed logs during execution, use:
    pytest -s tests/test_real_forecasts.py
"""

import pytest
from nowcasting_datamodel.models import ForecastSQL
from neso_solar_consumer.fetch_data import fetch_data
from neso_solar_consumer.format_forecast import format_to_forecast_sql
from neso_solar_consumer.save_forecast import save_forecasts_to_db


@pytest.mark.integration
def test_real_forecasts(db_session, test_config):
    """
    Integration Test: Ensures the full pipeline of fetching, formatting, and saving
    forecasts works as expected.
    """
    # Step 1: Fetch data
    df = fetch_data(
        resource_id=test_config["resource_id"],
        limit=test_config["limit"],
        columns=test_config["columns"],
        rename_columns=test_config["rename_columns"],
    )
    assert not df.empty, "fetch_data returned an empty DataFrame!"
    print("\nFetched Data:")
    print(df.to_string(index=False))

    # Step 2: Format data
    forecasts = format_to_forecast_sql(
        data=df,
        model_tag=test_config["model_name"],
        model_version=test_config["model_version"],
        session=db_session,
    )
    assert forecasts, "No forecasts were generated from the fetched data!"

    # Debug: Display formatted forecast values
    print("\nFormatted ForecastSQL Objects:")
    for forecast in forecasts:
        print(f"ForecastSQL Object: {forecast}")
        for value in forecast.forecast_values:
            print(
                f"  target_time: {value.target_time}, "
                f"expected_power_generation: {value.expected_power_generation_megawatts}"
            )

    # Step 3: Save forecasts to the database
    save_forecasts_to_db(forecasts=forecasts, session=db_session)

    # Step 4: Verify forecasts were saved correctly
    saved_forecast = db_session.query(ForecastSQL).first()
    assert saved_forecast is not None, "No forecast was saved to the database!"
    assert (
        saved_forecast.model.name == test_config["model_name"]
    ), "Model name mismatch!"
    assert len(saved_forecast.forecast_values) > 0, "No forecast values were saved!"
    print("\nSaved ForecastSQL Object Details:")
    print(f"Model: {saved_forecast.model.name}")
    print(f"Number of Forecast Values: {len(saved_forecast.forecast_values)}")

    print("\nIntegration test completed successfully.")
