"""Data transformation layer: Pydantic models -> Pandas DataFrames -> Database models"""

import logging
from typing import List, Optional
from uuid import UUID, uuid4
from datetime import datetime

import pandas as pd
import numpy as np

from app.ingestion.validators import (
    RawPlayerData, RawTeamData, RawGameData, RawPlayerGameStats, RawInjuryReport
)
from app.persistence.models import Player, Team, Season, Game, PlayerGameStats, InjuryReport

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms validated Pydantic models to Pandas DataFrames and database models"""
    
    @staticmethod
    def players_to_dataframe(players: List[RawPlayerData]) -> pd.DataFrame:
        """Convert list of RawPlayerData to DataFrame"""
        if not players:
            return pd.DataFrame()
        
        data = []
        for player in players:
            data.append({
                "player_id": player.player_id,
                "name": player.name,
                "position": player.position,
                "height": player.height,
                "weight": player.weight,
                "rookie_season": player.rookie_season
            })
        
        df = pd.DataFrame(data)
        logger.info(f"Created DataFrame with {len(df)} players")
        return df
    
    @staticmethod
    def teams_to_dataframe(teams: List[RawTeamData]) -> pd.DataFrame:
        """Convert list of RawTeamData to DataFrame"""
        if not teams:
            return pd.DataFrame()
        
        data = []
        for team in teams:
            data.append({
                "team_id": team.team_id,
                "name": team.name,
                "city": team.city,
                "abbreviation": team.abbreviation
            })
        
        df = pd.DataFrame(data)
        logger.info(f"Created DataFrame with {len(df)} teams")
        return df
    
    @staticmethod
    def games_to_dataframe(
        games: List[RawGameData],
        season_map: Optional[dict] = None,
        team_map: Optional[dict] = None,
        team_id_to_name_map: Optional[dict] = None
    ) -> pd.DataFrame:
        """Convert list of RawGameData to DataFrame with UUID mappings
        
        Args:
            games: List of raw game data
            season_map: Mapping of season IDs to UUIDs
            team_map: Mapping of team names to UUIDs
            team_id_to_name_map: Mapping of external team IDs to team names
        """
        if not games:
            return pd.DataFrame()
        
        data = []
        for game in games:
            # Map external team IDs to names, then to UUIDs
            home_team_uuid = None
            if game.home_team_id and team_id_to_name_map and team_map:
                home_team_name = team_id_to_name_map.get(game.home_team_id)
                if home_team_name:
                    home_team_uuid = team_map.get(home_team_name)
            
            away_team_uuid = None
            if game.away_team_id and team_id_to_name_map and team_map:
                away_team_name = team_id_to_name_map.get(game.away_team_id)
                if away_team_name:
                    away_team_uuid = team_map.get(away_team_name)
            
            # Generate UUID for game_id if needed
            try:
                game_uuid = UUID(game.game_id) if len(game.game_id) == 36 else uuid4()
            except ValueError:
                game_uuid = uuid4()
            
            data.append({
                "game_id": game_uuid,
                "season_id": season_map.get(game.season_id) if game.season_id and season_map else None,
                "game_date": game.game_date,
                "home_team_id": home_team_uuid,
                "away_team_id": away_team_uuid,
                "is_playoffs": game.is_playoffs,
                "status": game.status
            })
        
        df = pd.DataFrame(data)
        logger.info(f"Created DataFrame with {len(df)} games")
        return df
    
    @staticmethod
    def player_stats_to_dataframe(
        stats: List[RawPlayerGameStats],
        player_map: Optional[dict] = None,
        team_map: Optional[dict] = None,
        game_map: Optional[dict] = None,
        player_id_to_name_map: Optional[dict] = None,
        team_id_to_name_map: Optional[dict] = None
    ) -> pd.DataFrame:
        """Convert list of RawPlayerGameStats to DataFrame with UUID mappings
        
        Args:
            stats: List of raw player game stats
            player_map: Mapping of player names to UUIDs
            team_map: Mapping of team names to UUIDs
            game_map: Mapping of external game IDs to UUIDs
            player_id_to_name_map: Mapping of external player IDs to player names
            team_id_to_name_map: Mapping of external team IDs to team names
        """
        if not stats:
            return pd.DataFrame()
        
        data = []
        for stat in stats:
            # Map external player ID to name, then to UUID
            player_uuid = None
            if stat.player_id and player_id_to_name_map and player_map:
                player_name = player_id_to_name_map.get(stat.player_id)
                if player_name:
                    player_uuid = player_map.get(player_name)
            
            # Map external team ID to name, then to UUID
            team_uuid = None
            if stat.team_id and team_id_to_name_map and team_map:
                team_name = team_id_to_name_map.get(stat.team_id)
                if team_name:
                    team_uuid = team_map.get(team_name)
            
            # Map external game ID to UUID
            game_uuid = None
            if stat.game_id and game_map:
                game_uuid = game_map.get(stat.game_id)
            
            # Generate stat_id
            stat_uuid = uuid4()
            
            data.append({
                "stat_id": stat_uuid,
                "game_id": game_uuid,
                "player_id": player_uuid,
                "team_id": team_uuid,
                "game_date": stat.game_date,
                "minutes_played": stat.minutes_played,
                "points": stat.points,
                "rebounds": stat.rebounds,
                "assists": stat.assists,
                "steals": stat.steals,
                "blocks": stat.blocks,
                "turnovers": stat.turnovers,
                "field_goals_made": stat.field_goals_made,
                "field_goals_attempted": stat.field_goals_attempted,
                "three_pointers_made": stat.three_pointers_made,
                "three_pointers_attempted": stat.three_pointers_attempted,
                "free_throws_made": stat.free_throws_made,
                "free_throws_attempted": stat.free_throws_attempted,
                "usage_rate": stat.usage_rate,
                "true_shooting_pct": stat.true_shooting_pct,
                "started": stat.started,
                "advanced_metrics": stat.advanced_metrics
            })
        
        df = pd.DataFrame(data)
        logger.info(f"Created DataFrame with {len(df)} player stats")
        return df
    
    @staticmethod
    def injuries_to_dataframe(
        injuries: List[RawInjuryReport],
        player_map: Optional[dict] = None,
        team_map: Optional[dict] = None
    ) -> pd.DataFrame:
        """Convert list of RawInjuryReport to DataFrame with UUID mappings"""
        if not injuries:
            return pd.DataFrame()
        
        data = []
        for injury in injuries:
            # Map external IDs to internal UUIDs
            player_uuid = None
            if injury.player_id and player_map:
                player_uuid = player_map.get(injury.player_id)
            elif player_map:
                # Try to find by name
                for pid, pname in player_map.items():
                    if isinstance(pname, str) and injury.player_name.lower() in pname.lower():
                        player_uuid = pid
                        break
            
            team_uuid = None
            if injury.team_id and team_map:
                team_uuid = team_map.get(injury.team_id)
            elif injury.team_name and team_map:
                # Try to find by name
                for tid, tname in team_map.items():
                    if isinstance(tname, str) and injury.team_name.lower() in tname.lower():
                        team_uuid = tid
                        break
            
            injury_uuid = uuid4()
            
            data.append({
                "injury_id": injury_uuid,
                "player_id": player_uuid,
                "team_id": team_uuid,
                "reported_at": injury.reported_at,
                "injury_type": injury.injury_type,
                "body_area": injury.body_area,
                "diagnosis": injury.diagnosis,
                "status": injury.status,
                "effective_from": injury.effective_from,
                "effective_until": injury.effective_until,
                "source_url": injury.source_url
            })
        
        df = pd.DataFrame(data)
        logger.info(f"Created DataFrame with {len(df)} injury reports")
        return df
    
    @staticmethod
    def create_id_mapping(
        df: pd.DataFrame,
        external_id_col: str,
        uuid_col: str
    ) -> dict:
        """Create mapping from external ID to UUID"""
        if df.empty or external_id_col not in df.columns or uuid_col not in df.columns:
            return {}
        
        # Filter out null values
        valid_df = df[[external_id_col, uuid_col]].dropna(subset=[external_id_col, uuid_col])
        mapping = dict(zip(valid_df[external_id_col], valid_df[uuid_col]))
        logger.info(f"Created ID mapping with {len(mapping)} entries")
        return mapping
