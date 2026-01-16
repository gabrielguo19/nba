-- ============================================
-- NBA Prop-Variance Engine - Verification Queries
-- Paste these into pgAdmin Query Tool
-- ============================================

-- 1. STATIC DATA COUNTS
-- ============================================
SELECT 'Teams' as table_name, COUNT(*) as count FROM teams
UNION ALL
SELECT 'Players', COUNT(*) FROM players
UNION ALL
SELECT 'Seasons', COUNT(*) FROM seasons
UNION ALL
SELECT 'Games', COUNT(*) FROM games
UNION ALL
SELECT 'Player Game Stats', COUNT(*) FROM player_game_stats
UNION ALL
SELECT 'Injury Reports', COUNT(*) FROM injury_reports;

-- 2. TEAMS VERIFICATION
-- ============================================
SELECT 
    team_id,
    name,
    abbreviation,
    city,
    conference,
    division
FROM teams
ORDER BY name
LIMIT 10;

-- 3. PLAYERS VERIFICATION
-- ============================================
SELECT 
    player_id,
    name,
    position,
    height,
    weight,
    rookie_season
FROM players
ORDER BY name
LIMIT 10;

-- 4. INJURY REPORTS - DETAILED CHECK
-- ============================================
-- Total injuries with breakdown
SELECT 
    COUNT(*) as total_injuries,
    COUNT(CASE WHEN player_id IS NOT NULL THEN 1 END) as with_player_id,
    COUNT(CASE WHEN player_id IS NULL THEN 1 END) as without_player_id,
    COUNT(CASE WHEN team_id IS NOT NULL THEN 1 END) as with_team_id,
    COUNT(CASE WHEN team_id IS NULL THEN 1 END) as without_team_id,
    COUNT(DISTINCT status) as unique_statuses
FROM injury_reports;

-- 5. INJURY REPORTS - SAMPLE DATA
-- ============================================
SELECT 
    injury_id,
    player_id,
    team_id,
    reported_at,
    injury_type,
    status,
    diagnosis,
    source_url
FROM injury_reports
ORDER BY reported_at DESC
LIMIT 20;

-- 6. INJURIES BY STATUS
-- ============================================
SELECT 
    status,
    COUNT(*) as count
FROM injury_reports
GROUP BY status
ORDER BY count DESC;

-- 7. INJURIES BY SOURCE
-- ============================================
SELECT 
    source_url,
    COUNT(*) as count
FROM injury_reports
GROUP BY source_url
ORDER BY count DESC;

-- 8. RECENT INJURIES (Last 24 hours)
-- ============================================
SELECT 
    ir.injury_id,
    p.name as player_name,
    t.name as team_name,
    ir.status,
    ir.injury_type,
    ir.reported_at
FROM injury_reports ir
LEFT JOIN players p ON ir.player_id = p.player_id
LEFT JOIN teams t ON ir.team_id = t.team_id
WHERE ir.reported_at >= NOW() - INTERVAL '24 hours'
ORDER BY ir.reported_at DESC;

-- 9. INJURIES WITH MISSING PLAYER/TEAM MAPPING
-- ============================================
SELECT 
    COUNT(*) as unmapped_injuries,
    COUNT(CASE WHEN player_id IS NULL THEN 1 END) as missing_player,
    COUNT(CASE WHEN team_id IS NULL THEN 1 END) as missing_team,
    COUNT(CASE WHEN player_id IS NULL AND team_id IS NULL THEN 1 END) as missing_both
FROM injury_reports;

-- 10. GAMES VERIFICATION (if any exist)
-- ============================================
SELECT 
    game_id,
    game_date,
    home_team_id,
    away_team_id,
    is_playoffs,
    status
FROM games
ORDER BY game_date DESC
LIMIT 10;

-- 11. PLAYER GAME STATS (if any exist)
-- ============================================
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
LIMIT 10;

-- 12. TIMESCALEDB HYPERTABLE STATUS
-- ============================================
SELECT 
    hypertable_name,
    num_dimensions,
    compression_enabled,
    tablespaces
FROM timescaledb_information.hypertables
WHERE hypertable_schema = 'public';

-- 13. DATA QUALITY CHECK - INJURIES
-- ============================================
SELECT 
    'Total Injuries' as metric,
    COUNT(*)::text as value
FROM injury_reports
UNION ALL
SELECT 
    'Injuries with NULL player_id (expected)',
    COUNT(*)::text
FROM injury_reports
WHERE player_id IS NULL
UNION ALL
SELECT 
    'Injuries with NULL team_id (expected)',
    COUNT(*)::text
FROM injury_reports
WHERE team_id IS NULL
UNION ALL
SELECT 
    'Unique Status Values',
    COUNT(DISTINCT status)::text
FROM injury_reports
UNION ALL
SELECT 
    'Unique Source URLs',
    COUNT(DISTINCT source_url)::text
FROM injury_reports;

-- 14. LATEST INJURY TIMESTAMPS
-- ============================================
SELECT 
    MIN(reported_at) as earliest_injury,
    MAX(reported_at) as latest_injury,
    COUNT(*) as total_in_last_24h
FROM injury_reports
WHERE reported_at >= NOW() - INTERVAL '24 hours';
