"""Async NBA API client for fetching box scores and game data"""

import asyncio
import logging
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor

from nba_api.stats.endpoints import (
    boxscoretraditionalv2,
    playergamelog,
    teamgamelog,
    commonplayerinfo,
    commonteamroster,
    scoreboardv2
)
from nba_api.stats.static import teams, players

from app.ingestion.validators import (
    RawPlayerData, RawTeamData, RawGameData, RawPlayerGameStats
)

logger = logging.getLogger(__name__)


class NBAAPIClient:
    """Async wrapper around nba_api library"""
    
    def __init__(self, max_workers: int = 5):
        """
        Initialize NBA API client
        
        Args:
            max_workers: Maximum number of threads for concurrent API calls
        """
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self._teams_cache: Optional[Dict[int, Dict]] = None
        self._players_cache: Optional[Dict[int, Dict]] = None
    
    async def _run_in_executor(self, func, *args, **kwargs):
        """Run synchronous nba_api calls in thread pool"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, func, *args, **kwargs)
    
    async def get_all_teams(self) -> List[RawTeamData]:
        """Fetch all NBA teams"""
        try:
            teams_data = await self._run_in_executor(teams.get_teams)
            validated_teams = [
                RawTeamData(
                    team_id=team["id"],
                    name=team["full_name"],
                    city=team["city"],
                    abbreviation=team["abbreviation"]
                )
                for team in teams_data
            ]
            logger.info(f"Fetched {len(validated_teams)} teams")
            return validated_teams
        except Exception as e:
            logger.error(f"Error fetching teams: {e}")
            return []
    
    async def get_all_players(self, season: Optional[str] = None) -> List[RawPlayerData]:
        """Fetch all NBA players"""
        try:
            players_data = await self._run_in_executor(players.get_players)
            validated_players = [
                RawPlayerData(
                    player_id=player["id"],
                    name=f"{player['first_name']} {player['last_name']}",
                    position=None,  # Not in static data
                    height=None,
                    weight=None,
                    rookie_season=None
                )
                for player in players_data
            ]
            logger.info(f"Fetched {len(validated_players)} players")
            return validated_players
        except Exception as e:
            logger.error(f"Error fetching players: {e}")
            return []
    
    async def get_player_info(self, player_id: int) -> Optional[RawPlayerData]:
        """Fetch detailed player information"""
        try:
            player_info = await self._run_in_executor(
                commonplayerinfo.CommonPlayerInfo,
                player_id=player_id
            )
            info = player_info.get_dict()["resultSets"][0]["rowSet"]
            if not info:
                return None
            
            data = info[0]
            return RawPlayerData(
                player_id=player_id,
                name=f"{data[3]} {data[4]}",  # First name, Last name
                position=data[14] if len(data) > 14 else None,  # Position
                height=data[10] if len(data) > 10 else None,  # Height
                weight=data[11] if len(data) > 11 else None,  # Weight
                rookie_season=int(data[22]) if len(data) > 22 and data[22] else None
            )
        except Exception as e:
            logger.error(f"Error fetching player info for {player_id}: {e}")
            return None
    
    async def get_scoreboard(self, game_date: date) -> List[RawGameData]:
        """Fetch scoreboard for a specific date"""
        try:
            scoreboard = await self._run_in_executor(
                scoreboardv2.ScoreboardV2,
                game_date=game_date.strftime("%m/%d/%Y")
            )
            data = scoreboard.get_dict()
            
            # Extract game data from scoreboard
            games = []
            if "resultSets" in data and len(data["resultSets"]) > 0:
                game_header = data["resultSets"][0]
                if "rowSet" in game_header:
                    for game_row in game_header["rowSet"]:
                        try:
                            games.append(RawGameData(
                                game_id=str(game_row[2]),  # GAME_ID
                                game_date=datetime.strptime(
                                    f"{game_row[0]} {game_row[1]}",
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                home_team_id=game_row[6],  # HOME_TEAM_ID
                                away_team_id=game_row[7],  # VISITOR_TEAM_ID
                                is_playoffs=game_row[8] == 1,  # GAME_STATUS_ID
                                status=game_row[4]  # GAME_STATUS_TEXT
                            ))
                        except (IndexError, ValueError) as e:
                            logger.warning(f"Error parsing game data: {e}")
                            continue
            
            logger.info(f"Fetched {len(games)} games for {game_date}")
            return games
        except Exception as e:
            logger.error(f"Error fetching scoreboard for {game_date}: {e}")
            return []
    
    async def get_box_score(self, game_id: str, game_date: datetime) -> List[RawPlayerGameStats]:
        """Fetch box score for a specific game"""
        try:
            box_score = await self._run_in_executor(
                boxscoretraditionalv2.BoxScoreTraditionalV2,
                game_id=game_id
            )
            data = box_score.get_dict()
            
            stats = []
            if "resultSets" in data and len(data["resultSets"]) > 0:
                # Player stats are typically in the first result set
                player_stats = data["resultSets"][0]
                if "rowSet" in player_stats and "headers" in player_stats:
                    headers = player_stats["headers"]
                    for row in player_stats["rowSet"]:
                        try:
                            row_dict = dict(zip(headers, row))
                            stats.append(RawPlayerGameStats(
                                game_id=game_id,
                                player_id=row_dict.get("PLAYER_ID"),
                                team_id=row_dict.get("TEAM_ID"),
                                game_date=game_date,
                                minutes_played=self._parse_minutes(row_dict.get("MIN")),
                                points=row_dict.get("PTS"),
                                rebounds=row_dict.get("REB"),
                                assists=row_dict.get("AST"),
                                steals=row_dict.get("STL"),
                                blocks=row_dict.get("BLK"),
                                turnovers=row_dict.get("TOV"),
                                field_goals_made=row_dict.get("FGM"),
                                field_goals_attempted=row_dict.get("FGA"),
                                three_pointers_made=row_dict.get("FG3M"),
                                three_pointers_attempted=row_dict.get("FG3A"),
                                free_throws_made=row_dict.get("FTM"),
                                free_throws_attempted=row_dict.get("FTA"),
                                usage_rate=row_dict.get("USG_PCT"),
                                true_shooting_pct=row_dict.get("TS_PCT"),
                                started=row_dict.get("START_POSITION") is not None
                            ))
                        except (KeyError, ValueError) as e:
                            logger.warning(f"Error parsing player stat: {e}")
                            continue
            
            logger.info(f"Fetched {len(stats)} player stats for game {game_id}")
            return stats
        except Exception as e:
            logger.error(f"Error fetching box score for game {game_id}: {e}")
            return []
    
    @staticmethod
    def _parse_minutes(minutes_str: Optional[str]) -> Optional[float]:
        """Parse minutes string (e.g., '35:30') to float"""
        if not minutes_str:
            return None
        try:
            parts = minutes_str.split(":")
            if len(parts) == 2:
                return float(parts[0]) + float(parts[1]) / 60.0
            return float(minutes_str)
        except (ValueError, AttributeError):
            return None
    
    async def get_player_game_log(self, player_id: int, season: str) -> List[RawPlayerGameStats]:
        """Fetch game log for a specific player"""
        try:
            game_log = await self._run_in_executor(
                playergamelog.PlayerGameLog,
                player_id=player_id,
                season=season
            )
            data = game_log.get_dict()
            
            stats = []
            if "resultSets" in data and len(data["resultSets"]) > 0:
                player_stats = data["resultSets"][0]
                if "rowSet" in player_stats and "headers" in player_stats:
                    headers = player_stats["headers"]
                    for row in player_stats["rowSet"]:
                        try:
                            row_dict = dict(zip(headers, row))
                            game_date = datetime.strptime(
                                row_dict.get("GAME_DATE"),
                                "%b %d, %Y"
                            )
                            stats.append(RawPlayerGameStats(
                                game_id=str(row_dict.get("Game_ID")),
                                player_id=player_id,
                                team_id=row_dict.get("Team_ID"),
                                game_date=game_date,
                                minutes_played=self._parse_minutes(row_dict.get("MIN")),
                                points=row_dict.get("PTS"),
                                rebounds=row_dict.get("REB"),
                                assists=row_dict.get("AST"),
                                steals=row_dict.get("STL"),
                                blocks=row_dict.get("BLK"),
                                turnovers=row_dict.get("TOV"),
                                field_goals_made=row_dict.get("FGM"),
                                field_goals_attempted=row_dict.get("FGA"),
                                three_pointers_made=row_dict.get("FG3M"),
                                three_pointers_attempted=row_dict.get("FG3A"),
                                free_throws_made=row_dict.get("FTM"),
                                free_throws_attempted=row_dict.get("FTA"),
                                usage_rate=None,  # Not in game log
                                true_shooting_pct=None,  # Not in game log
                                started=row_dict.get("START_POSITION") is not None
                            ))
                        except (KeyError, ValueError) as e:
                            logger.warning(f"Error parsing game log entry: {e}")
                            continue
            
            logger.info(f"Fetched {len(stats)} games for player {player_id}")
            return stats
        except Exception as e:
            logger.error(f"Error fetching game log for player {player_id}: {e}")
            return []
    
    def close(self):
        """Clean up executor"""
        self.executor.shutdown(wait=True)
