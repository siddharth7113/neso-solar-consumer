"""
Main Script to Fetch, Format, and Save NESO Solar Forecast Data

This script orchestrates the following steps:
1. Fetches solar forecast data using the `fetch_data` function.
2. Formats the forecast data into `ForecastSQL` objects using `format_forecast.py`.
3. Saves the formatted forecasts into the database using `save_forecast.py`.
"""

import logging
from neso_solar_consumer.fetch_data import fetch_data
from neso_solar_consumer.format_forecast import format_to_forecast_sql
from neso_solar_consumer.save_forecast import save_forecasts_to_db
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models import Base_Forecast

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def app(
    db_url: str,
    resource_id: str,
    limit: int = 100,
    model_tag: str = "real_data_model",
    model_version: str = "1.0",
):
    """
    Main application function to fetch, format, and save solar forecast data.

    Parameters:
        db_url (str): Database connection URL.
        resource_id (str): Resource ID to fetch data from NESO API.
        limit (int): Number of records to fetch. Default is 100.
        model_tag (str): Model name for the forecast. Default is 'real_data_model'.
        model_version (str): Model version for the forecast. Default is '1.0'.
    """
    logger.info("Starting the NESO Solar Forecast pipeline.")

    # Initialize database connection using DatabaseConnection
    connection = DatabaseConnection(url=db_url, base=Base_Forecast, echo=False)

    try:
        with connection.get_session() as session:
            # Step 1: Fetch forecast data
            logger.info("Fetching forecast data.")
            columns = ["DATE_GMT", "TIME_GMT", "EMBEDDED_SOLAR_FORECAST"]
            rename_columns = {
                "DATE_GMT": "start_utc",
                "TIME_GMT": "end_utc",
                "EMBEDDED_SOLAR_FORECAST": "solar_forecast_kw",
            }
            forecast_data = fetch_data(resource_id, limit, columns, rename_columns)

            if forecast_data.empty:
                logger.warning("No data fetched. Exiting the pipeline.")
                return

            # Step 2: Format forecast data
            logger.info(f"Formatting {len(forecast_data)} rows of forecast data.")
            forecasts = format_to_forecast_sql(
                data=forecast_data,
                model_tag=model_tag,
                model_version=model_version,
                session=session,
            )

            if not forecasts:
                logger.warning("No forecasts generated. Exiting the pipeline.")
                return

            logger.info(f"Generated {len(forecasts)} ForecastSQL objects.")

            # Step 3: Save forecasts to the database
            logger.info("Saving forecasts to the database.")
            save_forecasts_to_db(forecasts, session)

            logger.info("Forecast pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in the forecast pipeline: {e}")
        raise


if __name__ == "__main__":
    import os

    # Example configurations (can be replaced with user inputs or external configs)
    DATABASE_URL = "db_url"
    RESOURCE_ID = "example_resource_id"
    LIMIT = 100
    MODEL_TAG = "real_data_model"
    MODEL_VERSION = "1.0"

    app(
        db_url=DATABASE_URL,
        resource_id=RESOURCE_ID,
        limit=LIMIT,
        model_tag=MODEL_TAG,
        model_version=MODEL_VERSION,
    )
