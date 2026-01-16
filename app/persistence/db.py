"""Database initialization and TimescaleDB hypertable setup"""

import logging
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool

from app.persistence.models import Base

logger = logging.getLogger(__name__)


class Database:
    """Database connection and session management"""

    def __init__(self, database_url: str):
        """
        Initialize database connection

        Args:
            database_url: PostgreSQL connection string
        """
        self.database_url = database_url
        self.engine: Optional[Engine] = None
        self.SessionLocal: Optional[sessionmaker] = None

    def connect(self) -> None:
        """Create database engine and session factory"""
        # Force IPv4 connection by using 127.0.0.1 instead of localhost
        # This prevents IPv6 connection issues on Windows
        database_url = self.database_url
        if "localhost" in database_url:
            database_url = database_url.replace("localhost", "127.0.0.1")
        
        self.engine = create_engine(
            database_url,
            poolclass=NullPool,  # TimescaleDB works better without connection pooling
            echo=False,  # Set to True for SQL query logging
            connect_args={"connect_timeout": 10},  # 10 second connection timeout
        )
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        
        # Test the connection to ensure authentication works
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection established and verified")
        except Exception as e:
            logger.error(f"Failed to verify database connection: {e}")
            raise

    def create_tables(self) -> None:
        """Create all database tables"""
        if not self.engine:
            raise RuntimeError("Database not connected. Call connect() first.")

        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database tables created successfully")

    def enable_timescaledb_extension(self) -> None:
        """Enable TimescaleDB extension"""
        if not self.engine:
            raise RuntimeError("Database not connected. Call connect() first.")

        logger.info("Enabling TimescaleDB extension...")
        with self.engine.begin() as conn:
            # Enable TimescaleDB extension
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb;"))
        logger.info("TimescaleDB extension enabled")

    def create_hypertables(self) -> None:
        """
        Convert time-series tables to TimescaleDB hypertables

        Hypertables are partitioned by the time column (game_date or reported_at)
        for optimal time-series query performance.
        """
        if not self.engine:
            raise RuntimeError("Database not connected. Call connect() first.")

        logger.info("Creating TimescaleDB hypertables...")

        # Create hypertables in separate transactions
        # Note: We'll drop and recreate the unique constraint after creating hypertables
        # Convert games table to hypertable
        # First, temporarily drop the unique constraint (TimescaleDB needs partitioning column in PK)
        with self.engine.begin() as conn:
            try:
                conn.execute(text("ALTER TABLE games DROP CONSTRAINT IF EXISTS uq_games_game_id;"))
            except Exception:
                pass
        
        with self.engine.begin() as conn:
            try:
                conn.execute(text("""
                    SELECT create_hypertable(
                        'games',
                        'game_date',
                        if_not_exists => TRUE,
                        chunk_time_interval => INTERVAL '1 month'
                    );
                """))
                logger.info("Created hypertable: games")
                # Re-add unique constraint on game_id for foreign key references
                conn.execute(text("ALTER TABLE games ADD CONSTRAINT uq_games_game_id UNIQUE (game_id);"))
            except Exception as e:
                logger.warning(f"Could not create hypertable for games: {e}")
                # Check if it already exists
                try:
                    result = conn.execute(text("""
                        SELECT COUNT(*) FROM timescaledb_information.hypertables
                        WHERE hypertable_name = 'games';
                    """))
                    if result.scalar() > 0:
                        logger.info("Hypertable 'games' already exists")
                        # Ensure unique constraint exists
                        try:
                            conn.execute(text("ALTER TABLE games ADD CONSTRAINT uq_games_game_id UNIQUE (game_id);"))
                        except Exception:
                            pass  # Constraint may already exist
                except Exception:
                    pass

        # Convert player_game_stats table to hypertable
        with self.engine.begin() as conn:
            try:
                conn.execute(text("""
                    SELECT create_hypertable(
                        'player_game_stats',
                        'game_date',
                        if_not_exists => TRUE,
                        chunk_time_interval => INTERVAL '1 month'
                    );
                """))
                logger.info("Created hypertable: player_game_stats")
            except Exception as e:
                logger.warning(f"Could not create hypertable for player_game_stats: {e}")
                try:
                    result = conn.execute(text("""
                        SELECT COUNT(*) FROM timescaledb_information.hypertables
                        WHERE hypertable_name = 'player_game_stats';
                    """))
                    if result.scalar() > 0:
                        logger.info("Hypertable 'player_game_stats' already exists")
                except Exception:
                    pass

        # Convert injury_reports table to hypertable
        with self.engine.begin() as conn:
            try:
                conn.execute(text("""
                    SELECT create_hypertable(
                        'injury_reports',
                        'reported_at',
                        if_not_exists => TRUE,
                        chunk_time_interval => INTERVAL '1 week'
                    );
                """))
                logger.info("Created hypertable: injury_reports")
            except Exception as e:
                logger.warning(f"Could not create hypertable for injury_reports: {e}")
                try:
                    result = conn.execute(text("""
                        SELECT COUNT(*) FROM timescaledb_information.hypertables
                        WHERE hypertable_name = 'injury_reports';
                    """))
                    if result.scalar() > 0:
                        logger.info("Hypertable 'injury_reports' already exists")
                except Exception:
                    pass

        logger.info("TimescaleDB hypertables setup complete")

    def initialize(self) -> None:
        """
        Complete database initialization:
        1. Create tables
        2. Enable TimescaleDB extension
        3. Create hypertables
        """
        self.create_tables()
        self.enable_timescaledb_extension()
        self.create_hypertables()

    def get_session(self) -> Session:
        """
        Get a database session

        Returns:
            SQLAlchemy session

        Usage:
            db = Database(database_url)
            db.connect()
            session = db.get_session()
            try:
                # Use session
                pass
            finally:
                session.close()
        """
        if not self.SessionLocal:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self.SessionLocal()

    def close(self) -> None:
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")


def get_database_url(
    host: str = "localhost",
    port: int = 5432,
    database: str = "nba_prop_variance",
    user: str = "nba_user",
    password: str = "nba_password"
) -> str:
    """
    Construct PostgreSQL database URL

    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password

    Returns:
        PostgreSQL connection string
    """
    from urllib.parse import quote_plus
    
    # URL encode user and password to handle special characters
    user_encoded = quote_plus(user)
    password_encoded = quote_plus(password)
    
    return f"postgresql://{user_encoded}:{password_encoded}@{host}:{port}/{database}"
