"""Test script for Phase 2: Ingestion Pipeline"""
import sys
import logging
from pathlib import Path
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).parent))

from app.persistence.db import Database
from config.settings import settings
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_phase2():
    """Test Phase 2 components"""
    print("=" * 70)
    print("PHASE 2: INGESTION PIPELINE - TEST")
    print("=" * 70)
    
    db = Database(settings.database_url)
    db.connect()
    
    try:
        # Test 1: Check static data (teams, players)
        print("\n1. Checking static data ingestion...")
        with db.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM teams"))
            team_count = result.scalar()
            print(f"   Teams: {team_count} (expected: ~30)")
            
            result = conn.execute(text("SELECT COUNT(*) FROM players"))
            player_count = result.scalar()
            print(f"   Players: {player_count} (expected: 400+)")
            
            if team_count >= 30 and player_count >= 400:
                print("   ✅ Static data ingestion: PASSED")
            else:
                print("   ⚠️  Static data ingestion: Run 'python scripts/run_ingestion.py --setup'")
        
        # Test 2: Check game ingestion
        print("\n2. Checking game ingestion...")
        with db.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM games"))
            game_count = result.scalar()
            print(f"   Total games: {game_count}")
            
            # Check games by date range
            recent_date = date.today() - timedelta(days=30)
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM games 
                WHERE game_date >= :recent_date
            """), {"recent_date": recent_date})
            recent_games = result.scalar()
            print(f"   Games in last 30 days: {recent_games}")
            
            if game_count > 0:
                print("   ✅ Game ingestion: PASSED")
            else:
                print("   ⚠️  Game ingestion: No games found (run ingestion for specific dates)")
        
        # Test 3: Check player stats ingestion
        print("\n3. Checking player stats ingestion...")
        with db.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM player_game_stats"))
            stats_count = result.scalar()
            print(f"   Total player stats: {stats_count}")
            
            # Check stats with valid game_id
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM player_game_stats 
                WHERE game_id IS NOT NULL
            """))
            valid_stats = result.scalar()
            print(f"   Stats with valid game_id: {valid_stats}")
            
            # Check stats with valid player_id
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM player_game_stats 
                WHERE player_id IS NOT NULL
            """))
            valid_player_stats = result.scalar()
            print(f"   Stats with valid player_id: {valid_player_stats}")
            
            if stats_count > 0 and valid_stats == stats_count:
                print("   ✅ Player stats ingestion: PASSED")
            else:
                print("   ⚠️  Player stats ingestion: Issues detected")
        
        # Test 4: Check injury reports
        print("\n4. Checking injury report ingestion...")
        with db.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM injury_reports"))
            injury_count = result.scalar()
            print(f"   Total injury reports: {injury_count}")
            
            if injury_count > 0:
                # Check recent injuries
                result = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM injury_reports 
                    WHERE reported_at >= NOW() - INTERVAL '7 days'
                """))
                recent_injuries = result.scalar()
                print(f"   Injuries in last 7 days: {recent_injuries}")
                print("   ✅ Injury report ingestion: PASSED")
            else:
                print("   ⚠️  Injury report ingestion: No injuries found (scraping may have timed out)")
        
        # Test 5: Check data relationships
        print("\n5. Checking data relationships...")
        with db.engine.connect() as conn:
            # Games with valid teams
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM games g
                WHERE g.home_team_id IS NOT NULL 
                AND g.away_team_id IS NOT NULL
            """))
            valid_games = result.scalar()
            print(f"   Games with valid team relationships: {valid_games}")
            
            # Stats with valid relationships
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                JOIN players p ON pgs.player_id = p.player_id
            """))
            linked_stats = result.scalar()
            print(f"   Stats with valid game and player links: {linked_stats}")
            
            if valid_games > 0 and linked_stats > 0:
                print("   ✅ Data relationships: PASSED")
            else:
                print("   ⚠️  Data relationships: Some issues detected")
        
        # Test 6: Check data quality
        print("\n6. Checking data quality...")
        with db.engine.connect() as conn:
            # Check for duplicate games (same game_id)
            result = conn.execute(text("""
                SELECT game_id, COUNT(*) as cnt
                FROM games
                GROUP BY game_id
                HAVING COUNT(*) > 1
                LIMIT 5
            """))
            duplicates = list(result)
            if duplicates:
                print(f"   ⚠️  Found {len(duplicates)} duplicate game_ids")
            else:
                print("   ✅ No duplicate games detected")
            
            # Check for stats with missing required fields
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM player_game_stats 
                WHERE game_id IS NULL OR player_id IS NULL
            """))
            invalid_stats = result.scalar()
            if invalid_stats == 0:
                print("   ✅ All stats have required fields")
            else:
                print(f"   ⚠️  Found {invalid_stats} stats with missing required fields")
        
        print("\n" + "=" * 70)
        print("✅ PHASE 2 TEST: COMPLETED")
        print("=" * 70)
        print("\nTo run ingestion:")
        print("  python scripts/run_ingestion.py --setup          # Initial setup")
        print("  python scripts/run_ingestion.py --date 2024-12-15  # Specific date")
        return True
        
    except Exception as e:
        print(f"\n❌ PHASE 2 TEST: FAILED - {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    success = test_phase2()
    sys.exit(0 if success else 1)
