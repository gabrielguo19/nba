"""Script to run data ingestion"""

import asyncio
import logging
import sys
from pathlib import Path
from datetime import date, timedelta

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.persistence.db import Database
from app.workers.ingestion_worker import IngestionWorker
from config.settings import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run NBA data ingestion")
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Run initial setup (teams and players)"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Date to ingest (YYYY-MM-DD), defaults to yesterday"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for historical ingestion (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for historical ingestion (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--no-box-scores",
        action="store_true",
        help="Skip box score ingestion"
    )
    
    args = parser.parse_args()
    
    # Initialize database
    db = Database(settings.database_url)
    db.connect()
    
    try:
        worker = IngestionWorker(db)
        
        if args.setup:
            logger.info("Running initial setup...")
            await worker.run_initial_setup()
        
        elif args.start_date and args.end_date:
            start = date.fromisoformat(args.start_date)
            end = date.fromisoformat(args.end_date)
            logger.info(f"Running historical ingestion from {start} to {end}")
            await worker.run_historical_ingestion(
                start,
                end,
                include_box_scores=not args.no_box_scores
            )
        
        else:
            target_date = date.fromisoformat(args.date) if args.date else date.today() - timedelta(days=1)
            logger.info(f"Running daily ingestion for {target_date}")
            await worker.run_daily_ingestion(target_date)
    
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
