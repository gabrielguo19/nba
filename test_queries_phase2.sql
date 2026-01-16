-- ============================================================================
-- PHASE 2: INGESTION PIPELINE - VERIFICATION QUERIES
-- ============================================================================

-- 1. Static Data Verification
-- ============================================================================

-- Check teams count (should be ~30)
SELECT 
    COUNT(*) as total_teams,
    COUNT(DISTINCT name) as unique_team_names
FROM teams;

-- Check players count (should be 400+)
SELECT 
    COUNT(*) as total_players,
    COUNT(DISTINCT name) as unique_player_names,
    COUNT(DISTINCT position) as unique_positions
FROM players;

-- Sample teams
SELECT 
    team_id,
    name,
    abbreviation,
    city,
    created_at
FROM teams
ORDER BY name
LIMIT 10;

-- Sample players
SELECT 
    player_id,
    name,
    position,
    height,
    weight,
    created_at
FROM players
ORDER BY name
LIMIT 10;

-- ============================================================================
-- 2. Game Ingestion Verification
-- ============================================================================

-- Total games count
SELECT COUNT(*) as total_games FROM games;

-- Games by date range
SELECT 
    DATE(game_date) as game_date,
    COUNT(*) as games_count
FROM games
GROUP BY DATE(game_date)
ORDER BY game_date DESC
LIMIT 30;

-- Games by season
SELECT 
    s.year_start || '-' || s.year_end as season,
    COUNT(*) as games_count
FROM games g
JOIN seasons s ON g.season_id = s.season_id
GROUP BY s.year_start, s.year_end
ORDER BY s.year_start DESC;

-- Games with team information
SELECT 
    g.game_id,
    g.game_date,
    ht.name as home_team,
    at.name as away_team,
    g.is_playoffs,
    g.status
FROM games g
JOIN teams ht ON g.home_team_id = ht.team_id
JOIN teams at ON g.away_team_id = at.team_id
ORDER BY g.game_date DESC
LIMIT 10;

-- ============================================================================
-- 3. Player Stats Ingestion Verification
-- ============================================================================

-- Total player stats count
SELECT COUNT(*) as total_stats FROM player_game_stats;

-- Stats by date
SELECT 
    DATE(game_date) as game_date,
    COUNT(*) as stats_count,
    COUNT(DISTINCT player_id) as unique_players,
    COUNT(DISTINCT game_id) as unique_games
FROM player_game_stats
GROUP BY DATE(game_date)
ORDER BY game_date DESC
LIMIT 30;

-- Top scoring games (individual player)
SELECT 
    p.name as player_name,
    t.name as team_name,
    DATE(pgs.game_date) as game_date,
    pgs.points,
    pgs.rebounds,
    pgs.assists,
    pgs.minutes_played
FROM player_game_stats pgs
JOIN players p ON pgs.player_id = p.player_id
JOIN teams t ON pgs.team_id = t.team_id
ORDER BY pgs.points DESC
LIMIT 20;

-- Stats with valid relationships
SELECT 
    COUNT(*) as total_stats,
    COUNT(DISTINCT pgs.player_id) as unique_players,
    COUNT(DISTINCT pgs.game_id) as unique_games,
    COUNT(DISTINCT pgs.team_id) as unique_teams
FROM player_game_stats pgs
WHERE pgs.game_id IS NOT NULL
AND pgs.player_id IS NOT NULL
AND pgs.team_id IS NOT NULL;

-- Average stats per game
SELECT 
    DATE(game_date) as game_date,
    COUNT(*) as total_stats,
    ROUND(AVG(points), 2) as avg_points,
    ROUND(AVG(rebounds), 2) as avg_rebounds,
    ROUND(AVG(assists), 2) as avg_assists,
    ROUND(AVG(minutes_played), 2) as avg_minutes
FROM player_game_stats
WHERE game_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(game_date)
ORDER BY game_date DESC;

-- ============================================================================
-- 4. Injury Report Verification
-- ============================================================================

-- Total injury reports
SELECT COUNT(*) as total_injuries FROM injury_reports;

-- Recent injuries
SELECT 
    p.name as player_name,
    t.name as team_name,
    ir.status,
    ir.injury_type,
    ir.body_area,
    ir.reported_at,
    ir.source_url
FROM injury_reports ir
LEFT JOIN players p ON ir.player_id = p.player_id
LEFT JOIN teams t ON ir.team_id = t.team_id
WHERE ir.reported_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY ir.reported_at DESC
LIMIT 20;

-- Injuries by status
SELECT 
    status,
    COUNT(*) as count
FROM injury_reports
GROUP BY status
ORDER BY count DESC;

-- Injuries by team
SELECT 
    t.name as team_name,
    COUNT(*) as injury_count
FROM injury_reports ir
JOIN teams t ON ir.team_id = t.team_id
GROUP BY t.name
ORDER BY injury_count DESC
LIMIT 10;

-- ============================================================================
-- 5. Data Quality Checks
-- ============================================================================

-- Check for duplicate games
SELECT 
    game_id,
    COUNT(*) as duplicate_count
FROM games
GROUP BY game_id
HAVING COUNT(*) > 1;

-- Check for stats with missing game_id
SELECT 
    COUNT(*) as stats_without_game
FROM player_game_stats
WHERE game_id IS NULL;

-- Check for stats with missing player_id
SELECT 
    COUNT(*) as stats_without_player
FROM player_game_stats
WHERE player_id IS NULL;

-- Check for orphaned stats (game doesn't exist)
SELECT 
    COUNT(*) as orphaned_stats
FROM player_game_stats pgs
LEFT JOIN games g ON pgs.game_id = g.game_id
WHERE g.game_id IS NULL;

-- Check for orphaned stats (player doesn't exist)
SELECT 
    COUNT(*) as orphaned_stats
FROM player_game_stats pgs
LEFT JOIN players p ON pgs.player_id = p.player_id
WHERE p.player_id IS NULL;

-- ============================================================================
-- 6. Data Completeness Summary
-- ============================================================================

SELECT 
    'Teams' as data_type,
    COUNT(*) as count,
    CASE WHEN COUNT(*) >= 30 THEN '✅' ELSE '⚠️' END as status
FROM teams
UNION ALL
SELECT 
    'Players',
    COUNT(*),
    CASE WHEN COUNT(*) >= 400 THEN '✅' ELSE '⚠️' END
FROM players
UNION ALL
SELECT 
    'Games',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✅' ELSE '⚠️' END
FROM games
UNION ALL
SELECT 
    'Player Stats',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✅' ELSE '⚠️' END
FROM player_game_stats
UNION ALL
SELECT 
    'Injury Reports',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✅' ELSE '⚠️' END
FROM injury_reports;

-- ============================================================================
-- 7. Performance Queries (TimescaleDB specific)
-- ============================================================================

-- Recent games with stats (using TimescaleDB time_bucket)
SELECT 
    time_bucket('1 day', game_date) as day,
    COUNT(DISTINCT game_id) as games,
    COUNT(*) as total_stats,
    COUNT(DISTINCT player_id) as unique_players
FROM player_game_stats
WHERE game_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY day
ORDER BY day DESC;

-- Player performance over time (last 7 days)
SELECT 
    p.name as player_name,
    COUNT(*) as games_played,
    ROUND(AVG(pgs.points), 1) as avg_points,
    ROUND(AVG(pgs.rebounds), 1) as avg_rebounds,
    ROUND(AVG(pgs.assists), 1) as avg_assists
FROM player_game_stats pgs
JOIN players p ON pgs.player_id = p.player_id
WHERE pgs.game_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY p.player_id, p.name
HAVING COUNT(*) >= 2
ORDER BY avg_points DESC
LIMIT 20;
