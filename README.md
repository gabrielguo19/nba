# NBA Prop-Variance & Performance Engine

A high-performance backend system for analyzing NBA player performance variance, usage rate changes, and prop betting strategies using Pandas/NumPy for vectorized data processing.

## Architecture

- **Data Ingestion**: Asynchronous pipeline using `nba_api` (box scores) and Playwright (real-time injury reports)
- **Analytics Engine**: Pandas DataFrames and NumPy for vectorized calculations
- **Persistence**: PostgreSQL with TimescaleDB extension for time-series optimization
- **API**: FastAPI with strict Pydantic models

## Setup

### Prerequisites

- Python 3.10+
- Docker and Docker Compose

### Installation

1. Clone the repository and install dependencies:
```bash
pip install -r requirements.txt
```

2. Install Playwright browsers:
```bash
playwright install
```

3. Start TimescaleDB and pgAdmin containers:
```bash
docker-compose up -d
```

4. Initialize the database:
```bash
python scripts/init_db.py
```

### Database Access

- **TimescaleDB**: `localhost:5432`
  - Database: `nba_prop_variance`
  - User: `nba_user`
  - Password: `nba_password`

- **pgAdmin**: `http://localhost:5050`
  - Email: `admin@nba.com`
  - Password: `admin`

### Environment Variables

Copy `.env.example` to `.env` and adjust settings as needed:
```bash
cp .env.example .env
```

## Project Structure

```
nba_prop_variance_engine/
├── app/
│   ├── analytics/          # Pandas/NumPy-based analysis
│   ├── ingestion/          # Data fetching and validation
│   ├── persistence/        # Database models and operations
│   ├── api/                # FastAPI endpoints
│   ├── backtester/         # Temporal integrity-aware backtesting
│   └── workers/            # Async workers
├── config/                 # Application settings
├── scripts/                # Utility scripts
└── tests/                  # Unit and integration tests
```

## Development

### Database Initialization

The database initialization script creates all tables and converts time-series tables to TimescaleDB hypertables:

```bash
python scripts/init_db.py
```

This will:
1. Create all SQLAlchemy models as database tables
2. Enable the TimescaleDB extension
3. Convert `games`, `player_game_stats`, and `injury_reports` to hypertables

## License

MIT