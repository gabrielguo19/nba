"""Repository layer for database operations with Pandas DataFrame support"""

import logging
from typing import List, Optional
from uuid import UUID

import pandas as pd
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
        """Bulk insert games from DataFrame using Pandas"""
        if df.empty:
            return
        
        # Prepare DataFrame for bulk insert
        df_prepared = df.copy()
        df_prepared["game_date"] = pd.to_datetime(df_prepared["game_date"])
        
        # Ensure UUID columns are properly formatted
        if "game_id" in df_prepared.columns:
            df_prepared["game_id"] = df_prepared["game_id"].astype(str)
        
        # Use Pandas to_sql for efficient bulk insert
        df_prepared.to_sql(
            "games",
            self.session.bind,
            if_exists="append",
            index=False,
            method="multi"
        )
        self.session.commit()


class PlayerGameStatsRepository:
    """Repository for PlayerGameStats operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def bulk_insert_from_dataframe(self, df: pd.DataFrame) -> None:
        """Bulk insert player game stats from DataFrame using Pandas"""
        if df.empty:
            return
        
        # Prepare DataFrame
        df_prepared = df.copy()
        df_prepared["game_date"] = pd.to_datetime(df_prepared["game_date"])
        
        # Ensure required columns exist
        required_cols = ["stat_id", "game_id", "player_id", "team_id", "game_date"]
        for col in required_cols:
            if col not in df_prepared.columns:
                raise ValueError(f"Missing required column: {col}")
        
        # Ensure UUID columns are properly formatted
        uuid_cols = ["stat_id", "game_id", "player_id", "team_id"]
        for col in uuid_cols:
            if col in df_prepared.columns:
                df_prepared[col] = df_prepared[col].astype(str)
        
        # Use Pandas to_sql for efficient bulk insert
        df_prepared.to_sql(
            "player_game_stats",
            self.session.bind,
            if_exists="append",
            index=False,
            method="multi"
        )
        self.session.commit()


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
        
        # Ensure UUID columns are properly formatted
        uuid_cols = ["injury_id", "player_id", "team_id"]
        for col in uuid_cols:
            if col in df_prepared.columns:
                df_prepared[col] = df_prepared[col].astype(str)
        
        # Use Pandas to_sql for efficient bulk insert
        df_prepared.to_sql(
            "injury_reports",
            self.session.bind,
            if_exists="append",
            index=False,
            method="multi"
        )
        self.session.commit()


class Repository:
    """Main repository aggregator"""
    
    def __init__(self, session: Session):
        self.session = session
        self.players = PlayerRepository(session)
        self.teams = TeamRepository(session)
        self.games = GameRepository(session)
        self.player_stats = PlayerGameStatsRepository(session)
        self.injuries = InjuryReportRepository(session)
