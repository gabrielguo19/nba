"""Test inserting a single player stat to see what error we get"""
import sys
import logging
from pathlib import Path
from datetime import datetime
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).parent))

from app.persistence.db import Database
from app.persistence.repository import Repository
from config.settings import settings
from sqlalchemy.orm import Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db = Database(settings.database_url)
db.connect()

try:
    session = Session(db.engine)
    repo = Repository(session)
    
    # Try to get a game and player to test with
    from sqlalchemy import select
    from app.persistence.models import Game, Player
    
    game = session.scalar(select(Game).limit(1))
    player = session.scalar(select(Player).limit(1))
    
    if not game or not player:
        print("No games or players found in database")
    else:
        print(f"Testing with game: {game.game_id}, player: {player.player_id}")
        
        # Try to insert a test stat
        test_stat = {
            "stat_id": uuid4(),
            "game_id": game.game_id,
            "player_id": player.player_id,
            "team_id": game.home_team_id,
            "game_date": game.game_date,
            "points": 10,
            "rebounds": 5,
            "assists": 3,
            "minutes_played": 25.5
        }
        
        from app.persistence.models import PlayerGameStats
        try:
            session.bulk_insert_mappings(PlayerGameStats, [test_stat])
            session.commit()
            print("✅ Test stat inserted successfully!")
        except Exception as e:
            print(f"❌ Error inserting test stat: {e}")
            import traceback
            traceback.print_exc()
            session.rollback()
finally:
    db.close()
