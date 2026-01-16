"""Comprehensive verification script for Phase 1 static data ingestion"""
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://nba_user:nba_password@127.0.0.1:5433/nba_prop_variance')

print("=" * 70)
print("PHASE 1 STATIC DATA VERIFICATION")
print("=" * 70)

# ============================================================================
# 1. TEAMS VERIFICATION
# ============================================================================
print("\n" + "=" * 70)
print("1. TEAMS")
print("=" * 70)

teams_count = pd.read_sql("SELECT COUNT(*) as count FROM teams", engine)
print(f"\nTotal teams: {teams_count.iloc[0]['count']}")

if teams_count.iloc[0]['count'] > 0:
    teams_sample = pd.read_sql("""
        SELECT 
            team_id,
            name,
            city,
            abbreviation,
            external_id
        FROM teams
        ORDER BY name
        LIMIT 10
    """, engine)
    print("\nSample teams (first 10):")
    print(teams_sample.to_string(index=False))
    
    teams_by_city = pd.read_sql("""
        SELECT 
            city,
            COUNT(*) as team_count
        FROM teams
        GROUP BY city
        ORDER BY team_count DESC
    """, engine)
    print("\nTeams by city:")
    print(teams_by_city.to_string(index=False))
else:
    print("❌ NO TEAMS FOUND! Run: python scripts/run_ingestion.py --setup")

# ============================================================================
# 2. PLAYERS VERIFICATION
# ============================================================================
print("\n" + "=" * 70)
print("2. PLAYERS")
print("=" * 70)

players_count = pd.read_sql("SELECT COUNT(*) as count FROM players", engine)
print(f"\nTotal players: {players_count.iloc[0]['count']}")

if players_count.iloc[0]['count'] > 0:
    players_sample = pd.read_sql("""
        SELECT 
            player_id,
            name,
            position,
            height,
            weight,
            rookie_season,
            external_id
        FROM players
        ORDER BY name
        LIMIT 15
    """, engine)
    print("\nSample players (first 15):")
    print(players_sample.to_string(index=False))
    
    players_by_position = pd.read_sql("""
        SELECT 
            position,
            COUNT(*) as player_count
        FROM players
        WHERE position IS NOT NULL
        GROUP BY position
        ORDER BY player_count DESC
    """, engine)
    print("\nPlayers by position:")
    print(players_by_position.to_string(index=False))
    
    players_with_heights = pd.read_sql("""
        SELECT 
            COUNT(*) as total,
            COUNT(height) as with_height,
            COUNT(weight) as with_weight,
            COUNT(rookie_season) as with_rookie_season
        FROM players
    """, engine)
    print("\nPlayer data completeness:")
    print(players_with_heights.to_string(index=False))
else:
    print("❌ NO PLAYERS FOUND! Run: python scripts/run_ingestion.py --setup")

# ============================================================================
# 3. SEASONS VERIFICATION
# ============================================================================
print("\n" + "=" * 70)
print("3. SEASONS")
print("=" * 70)

seasons_count = pd.read_sql("SELECT COUNT(*) as count FROM seasons", engine)
print(f"\nTotal seasons: {seasons_count.iloc[0]['count']}")

if seasons_count.iloc[0]['count'] > 0:
    seasons_data = pd.read_sql("""
        SELECT 
            season_id,
            season_code,
            start_date,
            end_date,
            is_playoffs,
            external_id
        FROM seasons
        ORDER BY start_date DESC
    """, engine)
    print("\nAll seasons:")
    print(seasons_data.to_string(index=False))
else:
    print("❌ NO SEASONS FOUND! Run: python scripts/run_ingestion.py --setup")

# ============================================================================
# 4. GAMES VERIFICATION
# ============================================================================
print("\n" + "=" * 70)
print("4. GAMES")
print("=" * 70)

games_count = pd.read_sql("SELECT COUNT(*) as count FROM games", engine)
print(f"\nTotal games: {games_count.iloc[0]['count']}")

if games_count.iloc[0]['count'] > 0:
    games_sample = pd.read_sql("""
        SELECT 
            game_id,
            game_date,
            status,
            is_playoffs,
            home_team_id,
            away_team_id
        FROM games
        ORDER BY game_date DESC
        LIMIT 10
    """, engine)
    print("\nSample games (most recent 10):")
    print(games_sample.to_string(index=False))
else:
    print("⚠️  No games found. This is expected if you haven't ingested game data yet.")

# ============================================================================
# 5. PLAYER GAME STATS VERIFICATION
# ============================================================================
print("\n" + "=" * 70)
print("5. PLAYER GAME STATS")
print("=" * 70)

stats_count = pd.read_sql("SELECT COUNT(*) as count FROM player_game_stats", engine)
print(f"\nTotal player game stats: {stats_count.iloc[0]['count']}")

if stats_count.iloc[0]['count'] > 0:
    stats_sample = pd.read_sql("""
        SELECT 
            stat_id,
            player_id,
            game_id,
            game_date,
            points,
            rebounds,
            assists
        FROM player_game_stats
        ORDER BY game_date DESC
        LIMIT 10
    """, engine)
    print("\nSample stats (most recent 10):")
    print(stats_sample.to_string(index=False))
else:
    print("⚠️  No player stats found. This is expected if you haven't ingested box scores yet.")

# ============================================================================
# 6. INJURY REPORTS VERIFICATION
# ============================================================================
print("\n" + "=" * 70)
print("6. INJURY REPORTS")
print("=" * 70)

injuries_count = pd.read_sql("SELECT COUNT(*) as count FROM injury_reports", engine)
print(f"\nTotal injury reports: {injuries_count.iloc[0]['count']}")

if injuries_count.iloc[0]['count'] > 0:
    injuries_sample = pd.read_sql("""
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
    print("\nSample injuries (most recent 10):")
    print(injuries_sample.to_string(index=False))
else:
    print("⚠️  No injuries found. This is expected if scraping timed out.")

# ============================================================================
# 7. SUMMARY
# ============================================================================
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)

summary = pd.read_sql("""
    SELECT 
        (SELECT COUNT(*) FROM teams) as teams,
        (SELECT COUNT(*) FROM players) as players,
        (SELECT COUNT(*) FROM seasons) as seasons,
        (SELECT COUNT(*) FROM games) as games,
        (SELECT COUNT(*) FROM player_game_stats) as player_stats,
        (SELECT COUNT(*) FROM injury_reports) as injuries
""", engine)

print("\nData counts:")
print(summary.to_string(index=False))

print("\n" + "=" * 70)
print("VERIFICATION STATUS")
print("=" * 70)

teams = summary.iloc[0]['teams']
players = summary.iloc[0]['players']
seasons = summary.iloc[0]['seasons']

if teams > 0 and players > 0:
    print("✅ Phase 1 Static Data: SUCCESS")
    print(f"   - Teams: {teams} (expected: ~30)")
    print(f"   - Players: {players} (expected: 400+)")
    if seasons > 0:
        print(f"   - Seasons: {seasons}")
    else:
        print("   - Seasons: 0 (optional)")
else:
    print("❌ Phase 1 Static Data: FAILED")
    print("   Run: python scripts/run_ingestion.py --setup")

if summary.iloc[0]['games'] > 0:
    print(f"✅ Games: {summary.iloc[0]['games']} found")
else:
    print("⚠️  Games: None (run ingestion for specific dates)")

if summary.iloc[0]['player_stats'] > 0:
    print(f"✅ Player Stats: {summary.iloc[0]['player_stats']} found")
else:
    print("⚠️  Player Stats: None (run ingestion with box scores)")

if summary.iloc[0]['injuries'] > 0:
    print(f"✅ Injuries: {summary.iloc[0]['injuries']} found")
else:
    print("⚠️  Injuries: None (scraping may have timed out)")

print("\n" + "=" * 70)
