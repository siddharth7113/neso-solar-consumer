"""
Main Script to Fetch, Format, and Save NESO Solar Forecast Data

This script orchestrates the following steps:
1. Fetches solar forecast data using the `fetch_data` function from `fetch_data.py`.
2. Formats the forecast data into `ForecastSQL` objects using `format_forecast.py`.
3. Saves the formatted forecasts into the database using `save_forecast.py`.

Functions:
    - get_forecast: Fetches raw solar forecast data.
    - format_forecast: Converts raw forecast data into `ForecastSQL` objects.
    - save_forecast: Saves the formatted forecast data to the database.
"""

import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from neso_solar_consumer.fetch_data import fetch_data
from neso_solar_consumer.format_forecast import format_to_forecast_sql
from neso_solar_consumer.save_forecast import save_forecasts_to_db

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Database configuration (replace with actual database URL)
DATABASE_URL = "postgresql://user:password@localhost:5432/forecast_db"

# Create SQLAlchemy session
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
db_session = Session()


def get_forecast():
    """
    Fetch solar forecast data from NESO API.

    Returns:
        pd.DataFrame: A DataFrame containing solar forecast data.
    """
    logger.info("Fetching forecast data...")
    resource_id = "example_resource_id"  # Replace with the actual resource ID.
    limit = 100  # Number of records to fetch.
    columns = [
        "DATE_GMT",
        "TIME_GMT",
        "EMBEDDED_SOLAR_FORECAST",
    ]  # Relevant columns to extract.
    rename_columns = {
        "DATE_GMT": "start_utc",
        "TIME_GMT": "end_utc",
        "EMBEDDED_SOLAR_FORECAST": "solar_forecast_kw",
    }
    return fetch_data(resource_id, limit, columns, rename_columns)


def format_forecast(data, session):
    """
    Format solar forecast data into `ForecastSQL` objects.

    Parameters:
        data (pd.DataFrame): The raw forecast data fetched from the NESO API.
        session: SQLAlchemy session for metadata.

    Returns:
        list: A list of formatted `ForecastSQL` objects.
    """
    logger.info("Formatting forecast data...")
    model_tag = "real_data_model"  # Replace with your actual model name.
    model_version = "1.0"  # Replace with your actual model version.
    return format_to_forecast_sql(data, model_tag, model_version, session)


def save_forecast(forecasts, session):
    """
    Save solar forecast data to the database.

    Parameters:
        forecasts (list): A list of `ForecastSQL` objects to save.
        session: SQLAlchemy session for database access.
    """
    logger.info("Saving forecast data to the database...")
    save_forecasts_to_db(forecasts, session)


def main():
    """
    Orchestrates the following steps:
        1. Fetch forecast data.
        2. Format forecast data into `ForecastSQL` objects.
        3. Save forecast data to the database.
    """
    try:
        # Step 1: Fetch forecast data
        forecast_data = get_forecast()

        if forecast_data.empty:
            logger.warning("No data fetched from NESO API. Exiting.")
            return

        # Step 2: Format forecast data
        forecasts = format_forecast(forecast_data, db_session)

        if not forecasts:
            logger.warning("No forecasts were generated. Exiting.")
            return

        # Step 3: Save forecast data
        save_forecast(forecasts, db_session)

        logger.info("Process completed successfully!")
    except Exception as e:
        logger.error(f"An error occurred during the process: {e}")
    finally:
        db_session.close()


if __name__ == "__main__":
    main()
