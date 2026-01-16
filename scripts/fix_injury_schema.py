"""Fix injury_reports schema to allow NULL player_id and team_id"""
import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from sqlalchemy import text

from app.persistence.db import Database
from config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_injury_schema():
    """Alter injury_reports table to allow NULL player_id and team_id"""
    db = Database(settings.database_url)
    db.connect()
    
    try:
        with db.engine.begin() as conn:
            # Drop existing foreign key constraints if they exist
            logger.info("Dropping foreign key constraints...")
            try:
                conn.execute(text("""
                    ALTER TABLE injury_reports 
                    DROP CONSTRAINT IF EXISTS injury_reports_player_id_fkey;
                """))
                conn.execute(text("""
                    ALTER TABLE injury_reports 
                    DROP CONSTRAINT IF EXISTS injury_reports_team_id_fkey;
                """))
            except Exception as e:
                logger.warning(f"Could not drop constraints (may not exist): {e}")
            
            # Alter columns to allow NULL
            logger.info("Altering columns to allow NULL...")
            conn.execute(text("""
                ALTER TABLE injury_reports 
                ALTER COLUMN player_id DROP NOT NULL;
            """))
            conn.execute(text("""
                ALTER TABLE injury_reports 
                ALTER COLUMN team_id DROP NOT NULL;
            """))
            
            # Re-add foreign key constraints (they work with NULL values)
            logger.info("Re-adding foreign key constraints...")
            conn.execute(text("""
                ALTER TABLE injury_reports 
                ADD CONSTRAINT injury_reports_player_id_fkey 
                FOREIGN KEY (player_id) REFERENCES players(player_id);
            """))
            conn.execute(text("""
                ALTER TABLE injury_reports 
                ADD CONSTRAINT injury_reports_team_id_fkey 
                FOREIGN KEY (team_id) REFERENCES teams(team_id);
            """))
            
            logger.info("✅ Successfully updated injury_reports schema")
            
    except Exception as e:
        logger.error(f"❌ Error updating schema: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    fix_injury_schema()
