import pytest
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models import MLModelSQL

# Shared Test Configuration Constants
TEST_DB_URL = "postgresql://postgres:12345@localhost/testdb"
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


@pytest.fixture(scope="function")
def db_session() -> Generator:
    """
    Fixture to set up and tear down a PostgreSQL database session for testing.

    This fixture:
    - Creates a fresh database schema before each test.
    - Adds a dummy ML model for test purposes.
    - Tears down the database session and cleans up resources after each test.

    Returns:
        Generator: A SQLAlchemy session object.
    """
    # Create database engine and tables
    engine = create_engine(TEST_DB_URL)
    Base_Forecast.metadata.drop_all(engine)  # Drop all tables to ensure a clean slate
    Base_Forecast.metadata.create_all(engine)  # Recreate the tables

    # Establish session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Insert a dummy model for testing
    session.query(MLModelSQL).delete()  # Clean up any pre-existing data
    model = MLModelSQL(name=MODEL_NAME, version=MODEL_VERSION)
    session.add(model)
    session.commit()

    yield session  # Provide the session to the test

    # Cleanup: close session and dispose of engine
    session.close()
    engine.dispose()


@pytest.fixture(scope="session")
def test_config():
    """
    Fixture to provide shared test configuration constants.

    Returns:
        dict: A dictionary of test configuration values.
    """
    return {
        "resource_id": RESOURCE_ID,
        "limit": LIMIT,
        "columns": COLUMNS,
        "rename_columns": RENAME_COLUMNS,
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
    }
