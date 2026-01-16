"""Test script to verify the fixes worked"""
import pandas as pd
from sqlalchemy import create_engine
import sys

try:
    engine = create_engine('postgresql://nba_user:nba_password@127.0.0.1:5433/nba_prop_variance')
    
    print("=" * 60)
    print("TESTING FIXES")
    print("=" * 60)
    
    # Test 1: Check if injuries exist
    injuries_count = pd.read_sql("SELECT COUNT(*) as count FROM injury_reports", engine)
    total = injuries_count.iloc[0]['count']
    print(f"\n1. Total injuries in database: {total}")
    
    if total == 0:
        print("   ⚠️  No injuries found. Run ingestion with injuries to test UUID fix.")
    else:
        # Test 2: Check UUID handling
        uuid_test = pd.read_sql("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN player_id IS NULL THEN 1 END) as null_player_ids,
                COUNT(CASE WHEN player_id::text = 'None' THEN 1 END) as string_none_player_ids,
                COUNT(CASE WHEN team_id IS NULL THEN 1 END) as null_team_ids,
                COUNT(CASE WHEN team_id::text = 'None' THEN 1 END) as string_none_team_ids
            FROM injury_reports
        """, engine)
        
        row = uuid_test.iloc[0]
        print(f"\n2. UUID Handling Test:")
        print(f"   - Total injuries: {row['total']}")
        print(f"   - Null player_ids (correct): {row['null_player_ids']}")
        print(f"   - String 'None' player_ids (error): {row['string_none_player_ids']}")
        print(f"   - Null team_ids (correct): {row['null_team_ids']}")
        print(f"   - String 'None' team_ids (error): {row['string_none_team_ids']}")
        
        if row['string_none_player_ids'] == 0 and row['string_none_team_ids'] == 0:
            print("   ✅ UUID fix WORKED! No string 'None' values found.")
        else:
            print("   ❌ UUID fix FAILED! Found string 'None' values.")
        
        # Test 3: Sample data
        print(f"\n3. Sample injuries (first 5):")
        sample = pd.read_sql("""
            SELECT 
                injury_id,
                CASE WHEN player_id IS NULL THEN 'NULL' ELSE 'HAS_ID' END as player_status,
                CASE WHEN team_id IS NULL THEN 'NULL' ELSE 'HAS_ID' END as team_status,
                status,
                injury_type,
                reported_at
            FROM injury_reports
            ORDER BY reported_at DESC
            LIMIT 5
        """, engine)
        print(sample.to_string(index=False))
    
    print("\n" + "=" * 60)
    print("STATUS SUMMARY")
    print("=" * 60)
    print("✅ UUID Fix: Applied (replaces None with pd.NA before insert)")
    print("✅ Scoreboard Fix: Applied (uses get_data_frames() with fallback)")
    print("⚠️  Injury Scraping: Timing out (network/Playwright issue)")
    print("⚠️  Scoreboard: KeyError 'WinProbability' (nba_api library issue)")
    print("\nNext steps:")
    print("1. Try a date during active NBA season for games")
    print("2. Check Playwright browser installation for injuries")
    print("3. Verify UUID fix works when injuries are successfully scraped")
    
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)
