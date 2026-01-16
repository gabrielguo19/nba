# Testing Guide - Phase 1 & Phase 2

## Quick Start

### Run All Tests
```bash
# Phase 1: Foundation & Database Setup
python test_phase1.py

# Phase 2: Ingestion Pipeline
python test_phase2.py
```

### Run SQL Queries
```bash
# Connect to database via psql or pgAdmin
psql -h localhost -U nba_user -d nba_prop_variance

# Or use pgAdmin at http://localhost:5050
# Then run the SQL files:
# - test_queries_phase1.sql
# - test_queries_phase2.sql
```

---

## Test Scripts

### 1. `test_phase1.py`
**Purpose**: Verify Phase 1 (Foundation & Database Setup)

**Tests**:
- ✅ All required tables exist
- ✅ TimescaleDB extension enabled
- ✅ Hypertables configured
- ✅ Table schemas correct
- ✅ Foreign key constraints
- ✅ Primary keys

**Usage**:
```bash
python test_phase1.py
```

**Expected Output**:
```
✅ PHASE 1 TEST: PASSED
```

---

### 2. `test_phase2.py`
**Purpose**: Verify Phase 2 (Ingestion Pipeline)

**Tests**:
- ✅ Static data (teams, players)
- ✅ Game ingestion
- ✅ Player stats ingestion
- ✅ Injury report ingestion
- ✅ Data relationships
- ✅ Data quality

**Usage**:
```bash
python test_phase2.py
```

**Expected Output**:
```
✅ PHASE 2 TEST: COMPLETED
```

---

### 3. `test_ingestion_check.py`
**Purpose**: Quick check of ingested data

**Usage**:
```bash
python test_ingestion_check.py
```

**Output**:
```
Games for 2024-12-15: 84
Player stats for 2024-12-15: 140
```

---

## SQL Query Files

### 1. `test_queries_phase1.sql`
**Purpose**: Phase 1 verification queries

**Contains**:
- Table existence checks
- TimescaleDB extension check
- Hypertable verification
- Schema inspection
- Foreign key verification
- Index verification
- Row counts

**Usage**:
```sql
-- Run in pgAdmin or psql
\i test_queries_phase1.sql
```

---

### 2. `test_queries_phase2.sql`
**Purpose**: Phase 2 data verification queries

**Contains**:
- Static data verification (teams, players)
- Game ingestion verification
- Player stats verification
- Injury report verification
- Data quality checks
- Data completeness summary
- Performance queries (TimescaleDB)

**Usage**:
```sql
-- Run in pgAdmin or psql
\i test_queries_phase2.sql
```

---

## Manual Testing Checklist

### Phase 1 Testing
- [ ] Run `python test_phase1.py` - Should pass
- [ ] Run `test_queries_phase1.sql` in pgAdmin
- [ ] Verify all tables visible in pgAdmin
- [ ] Check TimescaleDB hypertables in pgAdmin

### Phase 2 Testing
- [ ] Run initial setup: `python scripts/run_ingestion.py --setup`
- [ ] Verify teams and players in database
- [ ] Run `python test_phase2.py` - Should pass
- [ ] Ingest specific date: `python scripts/run_ingestion.py --date 2024-12-15`
- [ ] Verify games and stats in database
- [ ] Run `test_queries_phase2.sql` in pgAdmin
- [ ] Check data relationships and quality

---

## Expected Results

### Phase 1
- ✅ 8 tables created
- ✅ TimescaleDB extension enabled
- ✅ 3 hypertables (games, player_game_stats, injury_reports)
- ✅ All foreign keys configured

### Phase 2
- ✅ ~30 teams in database
- ✅ 400+ players in database
- ✅ Games ingested for test dates
- ✅ Player stats ingested with valid relationships
- ✅ Injury reports scraped (may vary)

---

## Troubleshooting

### Phase 1 Issues
- **Tables missing**: Run `python scripts/init_db.py`
- **TimescaleDB not enabled**: Check docker-compose.yml and restart containers
- **Hypertables not created**: Check init_db.py logs

### Phase 2 Issues
- **No teams/players**: Run `python scripts/run_ingestion.py --setup`
- **No games**: Run ingestion for specific dates
- **No player stats**: Check if games were ingested first
- **Missing players in stats**: Some players may not be in database yet (filtered automatically)

---

## Database Connection

### Via psql
```bash
psql -h localhost -U nba_user -d nba_prop_variance
# Password: nba_password
```

### Via pgAdmin
- URL: http://localhost:5050
- Email: admin@nba.com
- Password: admin

### Connection String
```
postgresql://nba_user:nba_password@localhost:5432/nba_prop_variance
```

---

## Next Steps

After verifying Phase 1 and Phase 2:

1. **Ingest Historical Data**:
   ```bash
   python scripts/run_ingestion.py --start-date 2024-10-01 --end-date 2024-12-15
   ```

2. **Set Up Scheduled Ingestion**:
   - Configure cron job or task scheduler
   - Run daily ingestion automatically

3. **Monitor Data Quality**:
   - Run `test_phase2.py` regularly
   - Check SQL queries for data completeness
   - Monitor for duplicate or missing data
