"""Debug script to test scoreboard API"""
from datetime import date
from nba_api.stats.endpoints import scoreboardv2
import pandas as pd

# Test date
test_date = date(2024, 12, 15)
date_str = test_date.strftime("%m/%d/%Y")

print(f"Testing scoreboard for date: {date_str}")
print("=" * 60)

try:
    # Try to get scoreboard
    scoreboard = scoreboardv2.ScoreboardV2(game_date=date_str)
    print("✅ Scoreboard object created")
    
    # Try get_data_frames()
    try:
        dfs = scoreboard.get_data_frames()
        print(f"✅ get_data_frames() worked: {len(dfs)} DataFrames")
        if dfs and len(dfs) > 0:
            game_df = dfs[0]
            print(f"   First DataFrame shape: {game_df.shape}")
            print(f"   Columns: {list(game_df.columns)[:10]}")
            print(f"   Rows: {len(game_df)}")
            if not game_df.empty:
                print("\n   First row sample:")
                print(game_df.iloc[0].to_dict())
    except Exception as e:
        print(f"❌ get_data_frames() failed: {type(e).__name__}: {e}")
    
    # Try get_dict()
    try:
        data = scoreboard.get_dict()
        print(f"\n✅ get_dict() worked")
        print(f"   Top-level keys: {list(data.keys())[:10]}")
        if "resultSets" in data:
            print(f"   resultSets count: {len(data['resultSets'])}")
            if len(data["resultSets"]) > 0:
                game_header = data["resultSets"][0]
                print(f"   First resultSet keys: {list(game_header.keys())}")
                if "rowSet" in game_header:
                    print(f"   Games found: {len(game_header['rowSet'])}")
                    if len(game_header['rowSet']) > 0:
                        print(f"   First game row: {game_header['rowSet'][0][:5]}")
    except KeyError as ke:
        print(f"❌ get_dict() failed with KeyError: {ke}")
        # Try to access raw response
        try:
            if hasattr(scoreboard, 'response_json'):
                raw = scoreboard.response_json
                print(f"   Raw response type: {type(raw)}")
                if isinstance(raw, dict):
                    print(f"   Raw response keys: {list(raw.keys())[:10]}")
        except Exception as e2:
            print(f"   Could not access raw response: {e2}")
    except Exception as e:
        print(f"❌ get_dict() failed: {type(e).__name__}: {e}")
        
except Exception as e:
    print(f"❌ Failed to create scoreboard: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("Test complete")
