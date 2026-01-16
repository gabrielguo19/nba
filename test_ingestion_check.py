"""Quick check to see if ingestion worked"""
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
        result = conn.execute(text("SELECT COUNT(*) FROM games WHERE game_date >= '2024-12-15' AND game_date < '2024-12-16'"))
        games_count = result.scalar()
        print(f"Games for 2024-12-15: {games_count}")
        
        result2 = conn.execute(text("SELECT COUNT(*) FROM player_game_stats WHERE game_date >= '2024-12-15' AND game_date < '2024-12-16'"))
        stats_count = result2.scalar()
        print(f"Player stats for 2024-12-15: {stats_count}")
finally:
    db.close()
