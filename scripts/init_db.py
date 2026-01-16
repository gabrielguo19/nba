"""Initialize database with tables and TimescaleDB hypertables"""

import logging
import sys
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.persistence.db import Database, get_database_url
from config.settings import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Initialize database"""
    logger.info("Starting database initialization...")

    # Get database URL from settings
    database_url = settings.database_url
    logger.info(f"Connecting to database: {settings.db_host}:{settings.db_port}/{settings.db_name}")

    # Initialize database
    db = Database(database_url)
    try:
        db.connect()
        db.initialize()
        logger.info("Database initialization completed successfully!")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
