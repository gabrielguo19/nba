-- ============================================================================
-- PHASE 1: FOUNDATION & DATABASE SETUP - VERIFICATION QUERIES
-- ============================================================================

-- 1. Check all tables exist
SELECT 
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = 'public'
AND table_name IN (
    'players', 'teams', 'seasons', 'games',
    'player_game_stats', 'injury_reports',
    'variance_snapshots', 'usage_rate_changes'
)
ORDER BY table_name;

-- 2. Check TimescaleDB extension
SELECT 
    extname as extension_name,
    extversion as version
FROM pg_extension
WHERE extname = 'timescaledb';

-- 3. Check hypertables
SELECT 
    hypertable_schema,
    hypertable_name,
    num_dimensions,
    compression_enabled
FROM timescaledb_information.hypertables
ORDER BY hypertable_name;

-- 4. Check Players table schema
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'players'
ORDER BY ordinal_position;

-- 5. Check Games table schema
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'games'
ORDER BY ordinal_position;

-- 6. Check PlayerGameStats table schema
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'player_game_stats'
AND column_name IN (
    'stat_id', 'game_id', 'player_id', 'team_id', 'game_date',
    'points', 'rebounds', 'assists', 'minutes_played'
)
ORDER BY ordinal_position;

-- 7. Check foreign key constraints
SELECT
    tc.table_name, 
    kcu.column_name, 
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name,
    tc.constraint_name
FROM information_schema.table_constraints AS tc 
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
AND tc.table_schema = 'public'
ORDER BY tc.table_name, kcu.column_name;

-- 8. Check primary keys
SELECT
    tc.table_name,
    kcu.column_name,
    tc.constraint_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
AND tc.table_schema = 'public'
ORDER BY tc.table_name, kcu.ordinal_position;

-- 9. Check indexes (for performance)
SELECT
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
AND tablename IN ('games', 'player_game_stats', 'injury_reports')
ORDER BY tablename, indexname;

-- 10. Check table row counts (should be 0 for new database)
SELECT 
    'players' as table_name, COUNT(*) as row_count FROM players
UNION ALL
SELECT 'teams', COUNT(*) FROM teams
UNION ALL
SELECT 'seasons', COUNT(*) FROM seasons
UNION ALL
SELECT 'games', COUNT(*) FROM games
UNION ALL
SELECT 'player_game_stats', COUNT(*) FROM player_game_stats
UNION ALL
SELECT 'injury_reports', COUNT(*) FROM injury_reports
ORDER BY table_name;
