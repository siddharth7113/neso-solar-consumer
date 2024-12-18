import logging
from datetime import datetime, timezone
import pandas as pd
from nowcasting_datamodel.models import ForecastSQL, ForecastValue
from nowcasting_datamodel.read.read import get_latest_input_data_last_updated, get_location
from nowcasting_datamodel.read.read_models import get_model

# Configure logging (set to INFO for production; use DEBUG during debugging)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def format_to_forecast_sql(data: pd.DataFrame, model_tag: str, model_version: str, session) -> list:
    """
    Convert NESO solar forecast data into a ForecastSQL object.

    Args:
        data (pd.DataFrame): Input DataFrame with forecast data.
        model_tag (str): The model name/tag.
        model_version (str): The model version.
        session: SQLAlchemy session for database access.

    Returns:
        list: A list containing ForecastSQL objects.
    """
    logger.info("Starting format_to_forecast_sql process...")

    # Step 1: Retrieve model metadata
    model = get_model(name=model_tag, version=model_version, session=session)
    logger.debug(f"Model Retrieved: {model}")

    # Step 2: Fetch input data last updated timestamp
    input_data_last_updated = get_latest_input_data_last_updated(session=session)
    logger.debug(f"Input Data Last Updated: {input_data_last_updated}")

    # Step 3: Fetch or create the location using get_location
    location = get_location(session=session, gsp_id=0)
    logger.debug(f"Location Retrieved or Created: {location}")

    # Step 4: Deduplicate data to avoid database conflicts
    initial_row_count = len(data)
    data = data.drop_duplicates(subset=["start_utc", "solar_forecast_kw"])
    deduplicated_row_count = len(data)

    logger.info(
        f"Removed {initial_row_count - deduplicated_row_count} duplicate rows based on 'start_utc' and 'solar_forecast_kw'."
    )

    # Step 5: Process rows into ForecastValue objects
    forecast_values = []
    for _, row in data.iterrows():
        if pd.isnull(row["start_utc"]) or pd.isnull(row["solar_forecast_kw"]):
            logger.debug("Skipping row due to missing data")
            continue

        # Convert start_utc to a datetime object (handle both strings and datetime)
        if isinstance(row["start_utc"], datetime):
            target_time = row["start_utc"]
        else:
            try:
                target_time = datetime.fromisoformat(row["start_utc"]).replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning(f"Invalid datetime format in row: {row['start_utc']}. Skipping.")
                continue

        forecast_value = ForecastValue(
            target_time=target_time,
            expected_power_generation_megawatts=row["solar_forecast_kw"],
        ).to_orm()
        forecast_values.append(forecast_value)
        logger.debug(f"Forecast Value Created: {forecast_value}")

    if not forecast_values:
        logger.warning("No valid forecast values found in the data. Exiting.")
        return []

    # Step 6: Create a ForecastSQL object
    forecast = ForecastSQL(
        model=model,
        forecast_creation_time=datetime.now(tz=timezone.utc),
        location=location,  # Directly using the location from get_location
        input_data_last_updated=input_data_last_updated,
        forecast_values=forecast_values,
        historic=False,
    )
    logger.debug(f"ForecastSQL Object Created: {forecast}")

    logger.info("ForecastSQL object successfully created and returned.")
    return [forecast]
