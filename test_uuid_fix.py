"""Test script to verify UUID fix by manually inserting test data"""
import pandas as pd
from sqlalchemy import create_engine
from uuid import uuid4
from datetime import datetime

engine = create_engine('postgresql://nba_user:nba_password@127.0.0.1:5433/nba_prop_variance')

print("=" * 60)
print("TESTING UUID FIX")
print("=" * 60)

# Create test DataFrame with None values (simulating what would happen)
test_data = {
    "injury_id": [uuid4()],
    "player_id": [None],  # This is what was causing the error
    "team_id": [None],    # This is what was causing the error
    "reported_at": [datetime.now()],
    "injury_type": ["Test Injury"],
    "body_area": [None],
    "diagnosis": [None],
    "status": ["Questionable"],
    "effective_from": [None],
    "effective_until": [None],
    "source_url": ["https://test.com"]
}

df = pd.DataFrame(test_data)

print("\n1. Created test DataFrame with None values:")
print(f"   - player_id: {df['player_id'].iloc[0]} (type: {type(df['player_id'].iloc[0])})")
print(f"   - team_id: {df['team_id'].iloc[0]} (type: {type(df['team_id'].iloc[0])})")

# Apply the fix logic (same as in repository.py)
df_prepared = df.copy()
df_prepared["reported_at"] = pd.to_datetime(df_prepared["reported_at"])

# Ensure UUID columns are properly formatted - replace None/string 'None' with pd.NA
uuid_cols = ["injury_id", "player_id", "team_id"]
for col in uuid_cols:
    if col in df_prepared.columns:
        # Replace None, 'None', 'nan' with pd.NA to keep as null in database
        df_prepared[col] = df_prepared[col].replace([None, 'None', 'nan', pd.NA], pd.NA)
        # Only convert non-null values to string, keep nulls as None
        df_prepared[col] = df_prepared[col].apply(
            lambda x: str(x) if pd.notna(x) else None
        )

print("\n2. After applying fix:")
print(f"   - player_id: {df_prepared['player_id'].iloc[0]} (type: {type(df_prepared['player_id'].iloc[0])})")
print(f"   - team_id: {df_prepared['team_id'].iloc[0]} (type: {type(df_prepared['team_id'].iloc[0])})")

# Try to insert into database
try:
    print("\n3. Attempting to insert into database...")
    df_prepared.to_sql(
        "injury_reports",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )
    print("   ✅ SUCCESS! Insert worked with None values")
    
    # Verify it was inserted correctly
    result = pd.read_sql("""
        SELECT 
            injury_id,
            player_id,
            team_id,
            status,
            injury_type
        FROM injury_reports
        WHERE source_url = 'https://test.com'
    """, engine)
    
    print("\n4. Verification query:")
    print(result.to_string(index=False))
    
    # Check if nulls are actually NULL in database (not string 'None')
    null_check = pd.read_sql("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN player_id IS NULL THEN 1 END) as null_player_ids,
            COUNT(CASE WHEN player_id::text = 'None' THEN 1 END) as string_none_player_ids
        FROM injury_reports
        WHERE source_url = 'https://test.com'
    """, engine)
    
    print("\n5. Null check:")
    print(null_check.to_string(index=False))
    
    if null_check.iloc[0]['string_none_player_ids'] == 0:
        print("\n   ✅ UUID FIX VERIFIED! Nulls are stored as NULL, not string 'None'")
    else:
        print("\n   ❌ UUID FIX FAILED! Found string 'None' values")
    
    # Clean up test data
    print("\n6. Cleaning up test data...")
    with engine.connect() as conn:
        conn.execute("DELETE FROM injury_reports WHERE source_url = 'https://test.com'")
        conn.commit()
    print("   ✅ Test data removed")
    
except Exception as e:
    print(f"\n   ❌ ERROR: {e}")
    print("   This means the UUID fix did NOT work correctly")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("TEST COMPLETE")
print("=" * 60)
