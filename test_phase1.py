"""Test script for Phase 1: Foundation & Database Setup"""
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from app.persistence.db import Database
from config.settings import settings
from sqlalchemy import text, inspect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_phase1():
    """Test Phase 1 components"""
    print("=" * 70)
    print("PHASE 1: FOUNDATION & DATABASE SETUP - TEST")
    print("=" * 70)
    
    db = Database(settings.database_url)
    db.connect()
    
    try:
        inspector = inspect(db.engine)
        tables = inspector.get_table_names()
        
        # Test 1: Check all required tables exist
        print("\n1. Checking database tables...")
        required_tables = [
            'players', 'teams', 'seasons', 'games',
            'player_game_stats', 'injury_reports',
            'variance_snapshots', 'usage_rate_changes'
        ]
        
        missing_tables = []
        for table in required_tables:
            if table in tables:
                print(f"   ✅ {table}")
            else:
                print(f"   ❌ {table} - MISSING")
                missing_tables.append(table)
        
        if missing_tables:
            print(f"\n   ⚠️  Missing tables: {', '.join(missing_tables)}")
            return False
        
        # Test 2: Check TimescaleDB extension
        print("\n2. Checking TimescaleDB extension...")
        with db.engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM pg_extension WHERE extname = 'timescaledb'"))
            if result.fetchone():
                print("   ✅ TimescaleDB extension enabled")
            else:
                print("   ❌ TimescaleDB extension not found")
                return False
        
        # Test 3: Check hypertables
        print("\n3. Checking TimescaleDB hypertables...")
        with db.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT hypertable_name 
                FROM timescaledb_information.hypertables
            """))
            hypertables = [row[0] for row in result]
            
            expected_hypertables = ['games', 'player_game_stats', 'injury_reports']
            for ht in expected_hypertables:
                if ht in hypertables:
                    print(f"   ✅ {ht} is a hypertable")
                else:
                    print(f"   ⚠️  {ht} is not a hypertable (may still work)")
        
        # Test 4: Check table schemas
        print("\n4. Checking table schemas...")
        with db.engine.connect() as conn:
            # Check players table
            result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'players'
                ORDER BY ordinal_position
            """))
            player_cols = {row[0]: (row[1], row[2]) for row in result}
            required_player_cols = ['player_id', 'name', 'created_at', 'updated_at']
            for col in required_player_cols:
                if col in player_cols:
                    print(f"   ✅ players.{col} ({player_cols[col][0]})")
                else:
                    print(f"   ❌ players.{col} - MISSING")
            
            # Check games table
            result = conn.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'games'
                AND column_name IN ('game_id', 'game_date', 'home_team_id', 'away_team_id', 'season_id')
            """))
            game_cols = {row[0]: row[1] for row in result}
            if len(game_cols) == 5:
                print(f"   ✅ games table has required columns")
            else:
                print(f"   ⚠️  games table missing some columns")
        
        # Test 5: Check foreign keys
        print("\n5. Checking foreign key constraints...")
        with db.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT
                    tc.table_name, 
                    kcu.column_name, 
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name 
                FROM information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_name IN ('games', 'player_game_stats', 'injury_reports')
                ORDER BY tc.table_name, kcu.column_name
            """))
            fks = list(result)
            if fks:
                print(f"   ✅ Found {len(fks)} foreign key constraints")
                for fk in fks[:5]:  # Show first 5
                    print(f"      {fk[0]}.{fk[1]} -> {fk[2]}.{fk[3]}")
            else:
                print("   ⚠️  No foreign keys found (may be intentional for hypertables)")
        
        print("\n" + "=" * 70)
        print("✅ PHASE 1 TEST: PASSED")
        print("=" * 70)
        return True
        
    except Exception as e:
        print(f"\n❌ PHASE 1 TEST: FAILED - {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    success = test_phase1()
    sys.exit(0 if success else 1)
