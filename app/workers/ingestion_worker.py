"""Async worker for scheduled data ingestion"""

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Optional

from app.ingestion.service import IngestionService
from app.persistence.db import Database
from config.settings import settings

logger = logging.getLogger(__name__)


class IngestionWorker:
    """Worker for async data ingestion tasks"""
    
    def __init__(self, database: Database):
        """
        Initialize ingestion worker
        
        Args:
            database: Database connection instance
        """
        self.database = database
        self.running = False
    
    async def run_daily_ingestion(self, target_date: Optional[date] = None):
        """Run daily ingestion for a specific date (defaults to yesterday)"""
        if target_date is None:
            target_date = date.today() - timedelta(days=1)
        
        logger.info(f"Starting daily ingestion for {target_date}")
        
        with IngestionService(self.database) as service:
            try:
                # Ingest games
                games_count = await service.ingest_games_for_date(target_date)
                
                # Ingest box scores
                if games_count > 0:
                    await service.ingest_box_scores_for_date(target_date)
                
                # Ingest injuries
                await service.ingest_injuries()
                
                logger.info(f"Daily ingestion complete for {target_date}")
            except Exception as e:
                logger.error(f"Error in daily ingestion: {e}")
                raise
    
    async def run_historical_ingestion(
        self,
        start_date: date,
        end_date: date,
        include_box_scores: bool = True
    ):
        """Run historical data ingestion for a date range"""
        logger.info(f"Starting historical ingestion from {start_date} to {end_date}")
        
        with IngestionService(self.database) as service:
            results = await service.ingest_date_range(
                start_date,
                end_date,
                include_box_scores=include_box_scores
            )
            
            logger.info(f"Historical ingestion complete: {results}")
            return results
    
    async def run_initial_setup(self):
        """Run initial setup: ingest teams and players"""
        logger.info("Starting initial setup...")
        
        with IngestionService(self.database) as service:
            # Ingest teams
            await service.ingest_teams()
            
            # Ingest players
            await service.ingest_players()
            
            logger.info("Initial setup complete")
    
    async def start_scheduled_ingestion(self, interval_hours: int = 24):
        """Start scheduled ingestion that runs periodically"""
        self.running = True
        logger.info(f"Starting scheduled ingestion (interval: {interval_hours} hours)")
        
        while self.running:
            try:
                await self.run_daily_ingestion()
                await asyncio.sleep(interval_hours * 3600)  # Convert hours to seconds
            except Exception as e:
                logger.error(f"Error in scheduled ingestion: {e}")
                await asyncio.sleep(3600)  # Wait 1 hour before retrying
    
    def stop(self):
        """Stop scheduled ingestion"""
        self.running = False
        logger.info("Stopping scheduled ingestion")


async def main():
    """Main entry point for ingestion worker"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Initialize database
    from app.persistence.db import Database
    db = Database(settings.database_url)
    db.connect()
    
    try:
        worker = IngestionWorker(db)
        
        # Run initial setup
        await worker.run_initial_setup()
        
        # Run daily ingestion for yesterday
        await worker.run_daily_ingestion()
        
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
