import logging
from nowcasting_datamodel.save.save import save
from sqlalchemy.orm.session import Session

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def save_forecasts_to_db(forecasts: list, session: Session):
    """
    Save a list of ForecastSQL objects to the database.

    Parameters:
        forecasts (list): The list of ForecastSQL objects to save.
        session (Session): SQLAlchemy session for database access.
    """
    if not forecasts:
        logger.warning("No forecasts provided to save!")
        return

    try:
        logger.info("Saving forecasts to the database.")
        save(
            forecasts=forecasts,
            session=session,
        )
        logger.info(f"Successfully saved {len(forecasts)} forecasts to the database.")
    except Exception as e:
        logger.error(f"An error occurred while saving forecasts: {e}")
        raise
