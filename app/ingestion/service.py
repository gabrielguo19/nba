"""Ingestion service that orchestrates data fetching, validation, and persistence"""

import logging
from datetime import date, datetime, timedelta
from typing import List, Optional

import pandas as pd

import pandas as pd

from app.ingestion.nba_api_client import NBAAPIClient
from app.ingestion.injury_scraper import InjuryScraper
from app.ingestion.transformers import DataTransformer
from app.ingestion.validators import (
    RawPlayerData, RawTeamData, RawGameData, RawPlayerGameStats, RawInjuryReport
)
from app.persistence.db import Database
from app.persistence.repository import Repository

logger = logging.getLogger(__name__)


class IngestionService:
    """Main service for ingesting NBA data"""
    
    def __init__(self, database: Database):
        """
        Initialize ingestion service
        
        Args:
            database: Database connection instance
        """
        self.database = database
        self.nba_client = NBAAPIClient()
        self.transformer = DataTransformer()
        self.session = None
    
    def __enter__(self):
        """Context manager entry"""
        self.session = self.database.get_session()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.session:
            self.session.close()
        self.nba_client.close()
    
    async def ingest_teams(self) -> int:
        """Ingest all NBA teams"""
        logger.info("Starting team ingestion...")
        
        # Fetch teams from API
        raw_teams = await self.nba_client.get_all_teams()
        if not raw_teams:
            logger.warning("No teams fetched from API")
            return 0
        
        # Transform to DataFrame
        teams_df = self.transformer.teams_to_dataframe(raw_teams)
        
        # Persist to database
        repo = Repository(self.session)
        repo.teams.bulk_upsert_from_dataframe(teams_df)
        
        logger.info(f"Successfully ingested {len(teams_df)} teams")
        return len(teams_df)
    
    async def ingest_players(self, season: Optional[str] = None) -> int:
        """Ingest all NBA players"""
        logger.info("Starting player ingestion...")
        
        # Fetch players from API
        raw_players = await self.nba_client.get_all_players(season=season)
        if not raw_players:
            logger.warning("No players fetched from API")
            return 0
        
        # Transform to DataFrame
        players_df = self.transformer.players_to_dataframe(raw_players)
        
        # Persist to database
        repo = Repository(self.session)
        repo.players.bulk_upsert_from_dataframe(players_df)
        
        logger.info(f"Successfully ingested {len(players_df)} players")
        return len(players_df)
    
    async def ingest_games_for_date(self, game_date: date) -> int:
        """Ingest games for a specific date"""
        logger.info(f"Starting game ingestion for {game_date}...")
        
        # Fetch games from API
        raw_games = await self.nba_client.get_scoreboard(game_date)
        if not raw_games:
            logger.warning(f"No games found for {game_date}")
            return 0
        
        # Get ID mappings for foreign keys
        repo = Repository(self.session)
        
        # Get team mappings (name -> UUID)
        repo = Repository(self.session)
        team_map = repo.teams.get_name_to_uuid_map()
        
        # Build team ID to name mapping from nba_api static data
        from nba_api.stats.static import teams
        teams_static = teams.get_teams()
        team_id_to_name_map = {team["id"]: team["full_name"] for team in teams_static}
        
        # Transform to DataFrame
        games_df = self.transformer.games_to_dataframe(
            raw_games,
            team_map=team_map,
            team_id_to_name_map=team_id_to_name_map
        )
        
        # Persist to database
        repo.games.bulk_insert_from_dataframe(games_df)
        
        logger.info(f"Successfully ingested {len(games_df)} games for {game_date}")
        return len(games_df)
    
    async def ingest_box_scores_for_date(self, game_date: date) -> int:
        """Ingest box scores for all games on a specific date"""
        logger.info(f"Starting box score ingestion for {game_date}...")
        
        # First, get games for the date
        raw_games = await self.nba_client.get_scoreboard(game_date)
        if not raw_games:
            logger.warning(f"No games found for {game_date}")
            return 0
        
        # Get ID mappings
        repo = Repository(self.session)
        
        # Get player mappings (name -> UUID)
        repo = Repository(self.session)
        player_map = repo.players.get_name_to_uuid_map()
        team_map = repo.teams.get_name_to_uuid_map()
        
        # Build player ID to name mapping from nba_api static data
        from nba_api.stats.static import players
        players_static = players.get_players()
        player_id_to_name_map = {
            player["id"]: f"{player['first_name']} {player['last_name']}"
            for player in players_static
        }
        
        # Build team ID to name mapping
        from nba_api.stats.static import teams
        teams_static = teams.get_teams()
        team_id_to_name_map = {team["id"]: team["full_name"] for team in teams_static}
        
        # Get game mappings (external game_id -> UUID)
        # Query games by date to get UUIDs
        from sqlalchemy import text, select
        from app.persistence.models import Game
        stmt = select(Game.game_id, Game.game_date).where(
            Game.game_date >= datetime.combine(game_date, datetime.min.time()),
            Game.game_date < datetime.combine(game_date + timedelta(days=1), datetime.min.time())
        )
        games_result = self.session.execute(stmt).all()
        # Create mapping: we'll match by date for now since external IDs aren't stored
        game_map = {}  # Will be populated by matching game dates
        
        # Fetch box scores for each game
        all_stats = []
        for game in raw_games:
            try:
                stats = await self.nba_client.get_box_score(
                    game.game_id,
                    game.game_date
                )
                all_stats.extend(stats)
            except Exception as e:
                logger.error(f"Error fetching box score for game {game.game_id}: {e}")
                continue
        
        if not all_stats:
            logger.warning(f"No player stats found for {game_date}")
            return 0
        
        # Transform to DataFrame
        stats_df = self.transformer.player_stats_to_dataframe(
            all_stats,
            player_map=player_map,
            team_map=team_map,
            game_map=game_map,
            player_id_to_name_map=player_id_to_name_map,
            team_id_to_name_map=team_id_to_name_map
        )
        
        # Persist to database
        repo.player_stats.bulk_insert_from_dataframe(stats_df)
        
        logger.info(f"Successfully ingested {len(stats_df)} player stats for {game_date}")
        return len(stats_df)
    
    async def ingest_injuries(self) -> int:
        """Ingest injury reports from web scraping"""
        logger.info("Starting injury report ingestion...")
        
        # Scrape injuries
        async with InjuryScraper(headless=True) as scraper:
            raw_injuries = await scraper.scrape_all_sources()
        
        if not raw_injuries:
            logger.warning("No injuries scraped")
            return 0
        
        # Get ID mappings
        repo = Repository(self.session)
        
        # Get player mappings (by name since we don't have external IDs)
        players_df = pd.read_sql(
            "SELECT player_id, name FROM players",
            self.session.bind
        )
        player_map = dict(zip(players_df["player_id"], players_df["name"]))
        
        # Get team mappings
        teams_df = pd.read_sql(
            "SELECT team_id, name FROM teams",
            self.session.bind
        )
        team_map = dict(zip(teams_df["team_id"], teams_df["name"]))
        
        # Transform to DataFrame
        injuries_df = self.transformer.injuries_to_dataframe(
            raw_injuries,
            player_map=player_map,
            team_map=team_map
        )
        
        # Persist to database
        repo.injuries.bulk_insert_from_dataframe(injuries_df)
        
        logger.info(f"Successfully ingested {len(injuries_df)} injury reports")
        return len(injuries_df)
    
    async def ingest_date_range(
        self,
        start_date: date,
        end_date: date,
        include_box_scores: bool = True
    ) -> dict:
        """Ingest games and box scores for a date range"""
        logger.info(f"Ingesting data from {start_date} to {end_date}")
        
        results = {
            "games": 0,
            "box_scores": 0,
            "errors": []
        }
        
        current_date = start_date
        while current_date <= end_date:
            try:
                # Ingest games
                games_count = await self.ingest_games_for_date(current_date)
                results["games"] += games_count
                
                # Ingest box scores if requested
                if include_box_scores and games_count > 0:
                    stats_count = await self.ingest_box_scores_for_date(current_date)
                    results["box_scores"] += stats_count
                
            except Exception as e:
                error_msg = f"Error ingesting {current_date}: {e}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
            
            current_date += timedelta(days=1)
        
        logger.info(f"Ingestion complete: {results['games']} games, {results['box_scores']} box scores")
        return results
