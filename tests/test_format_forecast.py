"""
Test Suite for `format_to_forecast_sql` Function

This script validates the functionality of the `format_to_forecast_sql` function.
It ensures that the function correctly creates a single ForecastSQL object with multiple
ForecastValue entries.

### How to Run the Tests:

Run the entire suite:
    pytest tests/test_format_forecast.py

Run a specific test:
    pytest tests/test_format_forecast.py::test_format_to_forecast_sql_real

For verbose output:
    pytest tests/test_format_forecast.py -v

Note: 
This test assumes a local PostgreSQL database configured with:
    - Username: `postgres`
    - Password: `12345`
    - Database: `testdb`

This setup is temporary for local testing and may require adjustment for CI/CD environments.
"""

import pytest
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models import MLModelSQL, ForecastSQL, ForecastValue
from neso_solar_consumer.fetch_data import fetch_data
from neso_solar_consumer.format_forecast import format_to_forecast_sql

# Test configuration constants
RESOURCE_ID = "db6c038f-98af-4570-ab60-24d71ebd0ae5"
LIMIT = 5
COLUMNS = ["DATE_GMT", "TIME_GMT", "EMBEDDED_SOLAR_FORECAST"]
RENAME_COLUMNS = {
    "DATE_GMT": "start_utc",
    "TIME_GMT": "end_utc",
    "EMBEDDED_SOLAR_FORECAST": "solar_forecast_kw",
}
MODEL_NAME = "real_data_model"
MODEL_VERSION = "1.0"


@pytest.fixture
def db_session() -> Generator:
    """
    Fixture to set up and tear down a PostgreSQL database session for testing.

    Database connection details (modify if required):
        - Host: localhost
        - User: postgres
        - Password: 12345
        - Database: testdb

    Creates fresh tables before each test and cleans up after execution.
    """
    engine = create_engine("postgresql://postgres:12345@localhost/testdb")
    Base_Forecast.metadata.drop_all(engine)  # Drop tables if they already exist
    Base_Forecast.metadata.create_all(engine)  # Create tables

    Session = sessionmaker(bind=engine)
    session = Session()

    # Add a dummy ML model for testing
    session.query(MLModelSQL).delete()  # Ensure no pre-existing data
    model = MLModelSQL(name=MODEL_NAME, version=MODEL_VERSION)
    session.add(model)
    session.commit()

    yield session  # Provide session to the test function

    # Cleanup
    session.close()
    engine.dispose()


def test_format_to_forecast_sql_real(db_session):
    """
    Test `format_to_forecast_sql` with real data fetched from the NESO API.

    Steps:
    1. Fetch test data from the NESO API.
    2. Format the data into a ForecastSQL object using the target function.
    3. Validate the creation of ForecastSQL and associated ForecastValue entries.
    4. Verify database state for correctness.

    Expected Outcomes:
    - A single ForecastSQL object is created.
    - The number of ForecastValue entries matches the input data.
    - The model metadata (name, version) matches the expected values.
    - No redundant or duplicate objects are added to the database.
    """
    # Step 1: Fetch mock data from the API
    data = fetch_data(RESOURCE_ID, LIMIT, COLUMNS, RENAME_COLUMNS)
    assert not data.empty, "fetch_data returned an empty DataFrame!"

    # Step 2: Format the data into a ForecastSQL object
    forecasts = format_to_forecast_sql(data, MODEL_NAME, MODEL_VERSION, db_session)

    # Step 3: Validate the ForecastSQL object
    assert len(forecasts) == 1, "More than one ForecastSQL object was created!"
    forecast = forecasts[0]

    # Validate the number of ForecastValue entries
    assert len(forecast.forecast_values) == len(data), (
        f"Mismatch in the number of ForecastValue entries. "
        f"Expected: {len(data)}, Got: {len(forecast.forecast_values)}"
    )

    # Step 4: Validate model metadata
    assert forecast.model.name == MODEL_NAME, f"Model name mismatch. Expected: {MODEL_NAME}"
    assert forecast.model.version == MODEL_VERSION, f"Model version mismatch. Expected: {MODEL_VERSION}"

    # Step 5: Validate database state
    forecasts_in_db = db_session.query(ForecastSQL).all()
    assert len(forecasts_in_db) == 1, "Unexpected number of ForecastSQL objects in the database!"

    total_forecast_values = db_session.query(ForecastValue).count()
    assert total_forecast_values == len(data), (
        f"Mismatch in the number of ForecastValue entries in the database. "
        f"Expected: {len(data)}, Got: {total_forecast_values}"
    )

    # Debugging information for better visibility
    print(f"ForecastSQL object created successfully with {len(forecast.forecast_values)} ForecastValues.")
    print(f"Total ForecastSQL objects in database: {len(forecasts_in_db)}")
    print(f"Total ForecastValue objects in database: {total_forecast_values}")
