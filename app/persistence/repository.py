"""Repository layer for database operations with Pandas DataFrame support"""

import logging
from typing import List, Optional
from uuid import UUID

import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import select, and_

from app.persistence.models import (
    Player, Team, Season, Game, PlayerGameStats,
    InjuryReport, VarianceSnapshot, UsageRateChange
)

logger = logging.getLogger(__name__)


class PlayerRepository:
    """Repository for Player operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def get_or_create(self, external_player_id: Optional[int], name: str, **kwargs) -> Player:
        """Get existing player or create new one"""
        # Try to find by name (external IDs not stored in current schema)
        stmt = select(Player).where(Player.name == name)
        player = self.session.scalar(stmt)
        if player:
            return player
        
        # Create new player
        player = Player(
            name=name,
            position=kwargs.get("position"),
            height=kwargs.get("height"),
            weight=kwargs.get("weight"),
            rookie_season=kwargs.get("rookie_season")
        )
        self.session.add(player)
        self.session.flush()  # Flush to get UUID
        return player
    
    def get_by_name(self, name: str) -> Optional[Player]:
        """Get player by name"""
        stmt = select(Player).where(Player.name == name)
        return self.session.scalar(stmt)
    
    def bulk_upsert_from_dataframe(self, df: pd.DataFrame) -> None:
        """Bulk upsert players from DataFrame"""
        for _, row in df.iterrows():
            self.get_or_create(
                external_player_id=row.get("player_id"),  # External ID from API
                name=row["name"],
                position=row.get("position"),
                height=row.get("height"),
                weight=row.get("weight"),
                rookie_season=row.get("rookie_season")
            )
        self.session.commit()
    
    def get_name_to_uuid_map(self) -> dict:
        """Get mapping of player names to UUIDs"""
        stmt = select(Player.player_id, Player.name)
        results = self.session.execute(stmt).all()
        return {name: player_id for player_id, name in results}


class TeamRepository:
    """Repository for Team operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def get_or_create(self, external_team_id: Optional[int], name: str, **kwargs) -> Team:
        """Get existing team or create new one"""
        # Try to find by name (external IDs not stored in current schema)
        stmt = select(Team).where(Team.name == name)
        team = self.session.scalar(stmt)
        if team:
            return team
        
        # Create new team
        team = Team(
            name=name,
            city=kwargs.get("city"),
            abbreviation=kwargs.get("abbreviation")
        )
        self.session.add(team)
        self.session.flush()  # Flush to get UUID
        return team
    
    def get_by_name(self, name: str) -> Optional[Team]:
        """Get team by name"""
        stmt = select(Team).where(Team.name == name)
        return self.session.scalar(stmt)
    
    def bulk_upsert_from_dataframe(self, df: pd.DataFrame) -> None:
        """Bulk upsert teams from DataFrame"""
        for _, row in df.iterrows():
            self.get_or_create(
                external_team_id=row.get("team_id"),  # External ID from API
                name=row["name"],
                city=row.get("city"),
                abbreviation=row.get("abbreviation")
            )
        self.session.commit()
    
    def get_name_to_uuid_map(self) -> dict:
        """Get mapping of team names to UUIDs"""
        stmt = select(Team.team_id, Team.name)
        results = self.session.execute(stmt).all()
        return {name: team_id for team_id, name in results}


class GameRepository:
    """Repository for Game operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def get_or_create(self, game_id: str, game_date: pd.Timestamp, **kwargs) -> Game:
        """Get existing game or create new one"""
        # Convert game_id to UUID if needed
        try:
            game_uuid = UUID(game_id) if isinstance(game_id, str) else game_id
        except ValueError:
            # Generate new UUID if invalid
            game_uuid = UUID(int(game_id)) if game_id.isdigit() else None
        
        if game_uuid:
            stmt = select(Game).where(Game.game_id == game_uuid)
            game = self.session.scalar(stmt)
            if game:
                return game
        
        # Create new game
        game = Game(
            game_id=game_uuid or UUID(int(game_id)) if game_id.isdigit() else None,
            season_id=kwargs.get("season_id"),
            game_date=game_date.to_pydatetime() if isinstance(game_date, pd.Timestamp) else game_date,
            home_team_id=kwargs.get("home_team_id"),
            away_team_id=kwargs.get("away_team_id"),
            is_playoffs=kwargs.get("is_playoffs", False),
            status=kwargs.get("status", "Scheduled")
        )
        self.session.add(game)
        return game
    
    def bulk_insert_from_dataframe(self, df: pd.DataFrame) -> None:
        """Bulk insert games from DataFrame using SQLAlchemy for better UUID handling"""
        if df.empty:
            return
        
        logger.info(f"Preparing to insert {len(df)} games into database...")
        
        # Prepare DataFrame for bulk insert
        df_prepared = df.copy()
        df_prepared["game_date"] = pd.to_datetime(df_prepared["game_date"])
        
        # Convert UUID columns from string back to UUID objects for SQLAlchemy
        from uuid import UUID as UUIDType
        uuid_cols = ["game_id", "season_id", "home_team_id", "away_team_id"]
        for col in uuid_cols:
            if col in df_prepared.columns:
                df_prepared[col] = df_prepared[col].apply(
                    lambda x: UUIDType(str(x)) if pd.notna(x) and x is not None else None
                )
        
        logger.info(f"Converted UUID columns, inserting {len(df_prepared)} rows...")
        
        # Use SQLAlchemy bulk insert for better control and error handling
        from app.persistence.models import Game
        from sqlalchemy.dialects.postgresql import insert
        
        games_to_insert = []
        for _, row in df_prepared.iterrows():
            try:
                games_to_insert.append({
                    "game_id": row["game_id"],
                    "season_id": row["season_id"],
                    "game_date": row["game_date"],
                    "home_team_id": row["home_team_id"],
                    "away_team_id": row["away_team_id"],
                    "is_playoffs": bool(row.get("is_playoffs", False)),
                    "status": str(row.get("status", "Scheduled"))
                })
            except Exception as e:
                logger.warning(f"Error preparing game row for insert: {e}, row: {row.to_dict()}")
                continue
        
        if not games_to_insert:
            logger.warning("No games prepared for insertion")
            return
        
        logger.info(f"Inserting {len(games_to_insert)} games using SQLAlchemy bulk insert...")
        
        try:
            # Use bulk_insert_mappings for efficient insert
            self.session.bulk_insert_mappings(Game, games_to_insert)
            self.session.commit()
            logger.info(f"Successfully inserted {len(games_to_insert)} games")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error inserting games: {e}")
            # Try to get more details about the error
            if hasattr(e, 'orig'):
                logger.error(f"Database error details: {e.orig}")
            raise


class PlayerGameStatsRepository:
    """Repository for PlayerGameStats operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def bulk_insert_from_dataframe(self, df: pd.DataFrame) -> None:
        """Bulk insert player game stats from DataFrame using SQLAlchemy for better UUID handling"""
        if df.empty:
            return
        
        logger.info(f"Preparing to insert {len(df)} player game stats into database...")
        
        # Prepare DataFrame
        df_prepared = df.copy()
        df_prepared["game_date"] = pd.to_datetime(df_prepared["game_date"])
        
        # Ensure required columns exist
        required_cols = ["stat_id", "game_id", "player_id", "team_id", "game_date"]
        for col in required_cols:
            if col not in df_prepared.columns:
                raise ValueError(f"Missing required column: {col}")
        
        # Convert UUID columns from string back to UUID objects, handling None properly
        from uuid import UUID as UUIDType
        uuid_cols = ["stat_id", "game_id", "player_id", "team_id"]
        for col in uuid_cols:
            if col in df_prepared.columns:
                # Replace None, 'None', 'nan' with actual None
                df_prepared[col] = df_prepared[col].replace([None, 'None', 'nan', np.nan, pd.NA], None)
                # Convert non-None values to UUID
                df_prepared[col] = df_prepared[col].apply(
                    lambda x: UUIDType(str(x)) if x is not None and pd.notna(x) else None
                )
        
        logger.info(f"Converted UUID columns, inserting {len(df_prepared)} rows...")
        
        # Use SQLAlchemy bulk insert for better control and error handling
        from app.persistence.models import PlayerGameStats
        
        stats_to_insert = []
        for _, row in df_prepared.iterrows():
            try:
                # Helper function to convert float to int safely (for integer columns)
                def to_int(val):
                    if val is None or pd.isna(val):
                        return None
                    try:
                        return int(float(val))
                    except (ValueError, TypeError):
                        return None
                
                # Helper function to convert to float safely
                def to_float(val):
                    if val is None or pd.isna(val):
                        return None
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        return None
                
                stats_to_insert.append({
                    "stat_id": row["stat_id"],
                    "game_id": row.get("game_id"),  # Can be None
                    "player_id": row.get("player_id"),  # Can be None
                    "team_id": row["team_id"],
                    "game_date": row["game_date"],
                    "minutes_played": to_float(row.get("minutes_played")),
                    "points": to_int(row.get("points")),
                    "rebounds": to_int(row.get("rebounds")),
                    "assists": to_int(row.get("assists")),
                    "steals": to_int(row.get("steals")),
                    "blocks": to_int(row.get("blocks")),
                    "turnovers": to_int(row.get("turnovers")),
                    "field_goals_made": to_int(row.get("field_goals_made")),
                    "field_goals_attempted": to_int(row.get("field_goals_attempted")),
                    "three_pointers_made": to_int(row.get("three_pointers_made")),
                    "three_pointers_attempted": to_int(row.get("three_pointers_attempted")),
                    "free_throws_made": to_int(row.get("free_throws_made")),
                    "free_throws_attempted": to_int(row.get("free_throws_attempted")),
                    "usage_rate": to_float(row.get("usage_rate")),
                    "true_shooting_pct": to_float(row.get("true_shooting_pct")),
                    "started": bool(row.get("started", False)),
                    "advanced_metrics": row.get("advanced_metrics")
                })
            except Exception as e:
                logger.warning(f"Error preparing stat row for insert: {e}, row: {row.to_dict()}")
                continue
        
        if not stats_to_insert:
            logger.warning("No stats prepared for insertion")
            return
        
        logger.info(f"Inserting {len(stats_to_insert)} player game stats using SQLAlchemy bulk insert...")
        
        try:
            # Use bulk_insert_mappings for efficient insert
            self.session.bulk_insert_mappings(PlayerGameStats, stats_to_insert)
            self.session.commit()
            logger.info(f"Successfully inserted {len(stats_to_insert)} player game stats")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error inserting player game stats: {e}")
            # Try to get more details about the error
            if hasattr(e, 'orig'):
                logger.error(f"Database error details: {e.orig}")
            raise


class InjuryReportRepository:
    """Repository for InjuryReport operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def bulk_insert_from_dataframe(self, df: pd.DataFrame) -> None:
        """Bulk insert injury reports from DataFrame using Pandas"""
        if df.empty:
            return
        
        # Prepare DataFrame
        df_prepared = df.copy()
        df_prepared["reported_at"] = pd.to_datetime(df_prepared["reported_at"])
        
        # Ensure UUID columns are properly formatted - replace None/string 'None' with pd.NA
        uuid_cols = ["injury_id", "player_id", "team_id"]
        for col in uuid_cols:
            if col in df_prepared.columns:
                # Replace None, 'None', 'nan' with pd.NA to keep as null in database
                df_prepared[col] = df_prepared[col].replace([None, 'None', 'nan', np.nan], pd.NA)
                # Only convert non-null values to string, keep nulls as None
                df_prepared[col] = df_prepared[col].apply(
                    lambda x: str(x) if pd.notna(x) else None
                )
        
        # Use Pandas to_sql for efficient bulk insert
        df_prepared.to_sql(
            "injury_reports",
            self.session.bind,
            if_exists="append",
            index=False,
            method="multi"
        )
        self.session.commit()


class SeasonRepository:
    """Repository for Season operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def get_or_create_by_year(self, year_start: int, year_end: int, season_type: str = "Regular") -> Season:
        """Get or create a season by year range"""
        stmt = select(Season).where(
            and_(
                Season.year_start == year_start,
                Season.year_end == year_end,
                Season.season_type == season_type
            )
        )
        season = self.session.scalar(stmt)
        if season:
            return season
        
        # Create new season
        season = Season(
            year_start=year_start,
            year_end=year_end,
            season_type=season_type
        )
        self.session.add(season)
        self.session.flush()  # Flush to get UUID
        return season
    
    def get_season_for_date(self, game_date) -> Season:
        """Get season for a game date (NBA seasons: Oct-June)"""
        from datetime import date
        if isinstance(game_date, date):
            year = game_date.year
            month = game_date.month
        else:
            year = game_date.year
            month = game_date.month
        
        # NBA season starts in October, ends in June
        # If month >= 10, it's the start of a new season (e.g., Oct 2024 = 2024-25 season)
        # If month < 10, it's the end of the previous season (e.g., Apr 2024 = 2023-24 season)
        if month >= 10:
            year_start = year
            year_end = year + 1
        else:
            year_start = year - 1
            year_end = year
        
        return self.get_or_create_by_year(year_start, year_end, "Regular")


class Repository:
    """Main repository aggregator"""
    
    def __init__(self, session: Session):
        self.session = session
        self.players = PlayerRepository(session)
        self.teams = TeamRepository(session)
        self.seasons = SeasonRepository(session)
        self.games = GameRepository(session)
        self.player_stats = PlayerGameStatsRepository(session)
        self.injuries = InjuryReportRepository(session)
