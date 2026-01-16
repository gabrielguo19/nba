"""Quick script to check database state"""
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://nba_user:nba_password@127.0.0.1:5433/nba_prop_variance')

# Check injuries
injuries_df = pd.read_sql("""
    SELECT 
        COUNT(*) as total,
        COUNT(player_id) as with_player,
        COUNT(*) - COUNT(player_id) as without_player,
        COUNT(CASE WHEN player_id::text = 'None' THEN 1 END) as string_nones
    FROM injury_reports
""", engine)

print("=== Injury Reports ===")
print(injuries_df.to_string())

# Check sample injuries
sample_df = pd.read_sql("""
    SELECT 
        injury_id,
        player_id,
        team_id,
        status,
        injury_type,
        reported_at
    FROM injury_reports
    ORDER BY reported_at DESC
    LIMIT 10
""", engine)

print("\n=== Sample Injuries ===")
print(sample_df.to_string())
