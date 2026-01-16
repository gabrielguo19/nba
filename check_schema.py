"""Check the actual database schema for player_game_stats"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.persistence.db import Database
from config.settings import settings
from sqlalchemy import text

db = Database(settings.database_url)
db.connect()

try:
    with db.engine.connect() as conn:
        # Check the column types
        result = conn.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'player_game_stats'
            AND column_name IN ('points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 
                               'field_goals_made', 'field_goals_attempted', 'three_pointers_made', 
                               'three_pointers_attempted', 'free_throws_made', 'free_throws_attempted')
            ORDER BY column_name
        """))
        
        print("Column types in player_game_stats:")
        for row in result:
            print(f"  {row[0]}: {row[1]} (nullable: {row[2]})")
finally:
    db.close()
