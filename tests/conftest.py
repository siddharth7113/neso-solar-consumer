import pytest
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models import MLModelSQL
from testcontainers.postgres import PostgresContainer

# Shared Test Configuration Constants
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


@pytest.fixture(scope="session")
def postgres_container():
    """
    Fixture to spin up a PostgreSQL container for the entire test session.

    This fixture uses `testcontainers` to start a fresh PostgreSQL container and provides
    the connection URL dynamically for use in other fixtures.
    """
    with PostgresContainer("postgres:15.5") as postgres:
        postgres.start()
        yield postgres.get_connection_url()


@pytest.fixture(scope="function")
def db_session(postgres_container) -> Generator:
    """
    Fixture to set up and tear down a PostgreSQL database session for testing.

    This fixture:
    - Connects to the PostgreSQL container provided by `postgres_container`.
    - Creates a fresh database schema before each test.
    - Adds a dummy ML model for test purposes.
    - Tears down the database session and cleans up resources after each test.

    Args:
        postgres_container (str): The dynamic connection URL provided by PostgresContainer.

    Returns:
        Generator: A SQLAlchemy session object.
    """
    # Use the dynamic connection URL
    engine = create_engine(postgres_container)
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
