"""Async NBA API client for fetching box scores and game data"""

import asyncio
import logging
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import requests

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
                lambda: commonplayerinfo.CommonPlayerInfo(player_id=player_id)
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
            date_str = game_date.strftime("%m/%d/%Y")
            logger.debug(f"Fetching scoreboard for date: {date_str}")
            
            # Direct API call to bypass nba_api library issues
            url = "https://stats.nba.com/stats/scoreboardV2"
            params = {
                "GameDate": date_str,
                "LeagueID": "00",
                "DayOffset": "0"
            }
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://www.nba.com/',
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            try:
                response = await self._run_in_executor(
                    lambda: requests.get(url, params=params, headers=headers, timeout=10)
                )
                response.raise_for_status()
                data = response.json()
                
                games = []
                if "resultSets" in data and len(data["resultSets"]) > 0:
                    game_header = data["resultSets"][0]
                    if "rowSet" in game_header and game_header["rowSet"]:
                        for game_row in game_header["rowSet"]:
                            try:
                                if len(game_row) < 8:
                                    logger.debug(f"Game row too short: {len(game_row)} columns")
                                    continue
                                
                                # Parse date - API returns '2024-12-15T00:00:00' format
                                game_date_str = str(game_row[0])  # GAME_DATE_EST
                                if 'T' in game_date_str:
                                    # Handle ISO format: '2024-12-15T00:00:00'
                                    game_date_parsed = datetime.strptime(game_date_str.split('T')[0], "%Y-%m-%d")
                                else:
                                    # Fallback to other formats
                                    try:
                                        game_date_parsed = datetime.strptime(game_date_str, "%Y-%m-%d %H:%M:%S")
                                    except ValueError:
                                        game_date_parsed = datetime.strptime(game_date_str, "%Y-%m-%d")
                                
                                # Determine if playoffs from game_id format: 0022401216
                                # Format: 002 + 24 (season) + 0/1 (game type) + 1216 (game num)
                                # 0 = Regular Season, 1 = Playoffs
                                game_id_str = str(game_row[2])
                                is_playoffs = False
                                if len(game_id_str) >= 5:
                                    game_type_char = game_id_str[4]  # 5th character (0-indexed)
                                    is_playoffs = game_type_char == '1'
                                
                                games.append(RawGameData(
                                    game_id=game_id_str,  # GAME_ID (index 2)
                                    game_date=game_date_parsed,
                                    home_team_id=int(game_row[6]) if len(game_row) > 6 and game_row[6] else None,  # HOME_TEAM_ID (index 6)
                                    away_team_id=int(game_row[7]) if len(game_row) > 7 and game_row[7] else None,  # VISITOR_TEAM_ID (index 7)
                                    is_playoffs=is_playoffs,
                                    status=str(game_row[4]) if len(game_row) > 4 and game_row[4] else "Unknown"  # GAME_STATUS_TEXT (index 4)
                                ))
                            except (IndexError, ValueError, TypeError) as e:
                                logger.warning(f"Error parsing game row: {e}, row length: {len(game_row) if hasattr(game_row, '__len__') else 'N/A'}")
                                continue
                
                if games:
                    logger.info(f"Fetched {len(games)} games for {game_date} using direct API call")
                    return games
                else:
                    logger.info(f"No games found for {game_date}")
                    return []
                    
            except requests.RequestException as e:
                logger.warning(f"Direct API call failed: {e}, trying nba_api library as fallback")
                # Fallback to nba_api library (will likely fail but try anyway)
                try:
                    scoreboard = await self._run_in_executor(
                        lambda: scoreboardv2.ScoreboardV2(game_date=date_str)
                    )
                    # If we get here, try to extract data
                    try:
                        dfs = await self._run_in_executor(lambda: scoreboard.get_data_frames())
                        if dfs and len(dfs) > 0 and not dfs[0].empty:
                            games = []
                            for idx, row in dfs[0].iterrows():
                                try:
                                    games.append(RawGameData(
                                        game_id=str(row.get('GAME_ID', '')),
                                        game_date=datetime.strptime(
                                            f"{row.get('GAME_DATE_EST', '')} {row.get('GAME_TIME_EST', '00:00:00')}",
                                            "%Y-%m-%d %H:%M:%S"
                                        ) if row.get('GAME_DATE_EST') else datetime.now(),
                                        home_team_id=int(row.get('HOME_TEAM_ID', 0)) if pd.notna(row.get('HOME_TEAM_ID')) else None,
                                        away_team_id=int(row.get('VISITOR_TEAM_ID', 0)) if pd.notna(row.get('VISITOR_TEAM_ID')) else None,
                                        is_playoffs=int(row.get('GAME_STATUS_ID', 0)) == 1,
                                        status=str(row.get('GAME_STATUS_TEXT', 'Unknown'))
                                    ))
                                except Exception as parse_err:
                                    logger.debug(f"Error parsing row: {parse_err}")
                                    continue
                            if games:
                                logger.info(f"Fetched {len(games)} games using nba_api fallback")
                                return games
                    except Exception:
                        pass
                    return []
                except KeyError as ke:
                    if 'WinProbability' in str(ke):
                        logger.warning(f"nba_api library failed with WinProbability error")
                        return []
                    raise
                        
        except Exception as e:
            logger.error(f"Error fetching scoreboard for {game_date}: {e}")
            return []
    
    async def get_box_score(self, game_id: str, game_date: datetime) -> List[RawPlayerGameStats]:
        """Fetch box score for a specific game"""
        try:
            box_score = await self._run_in_executor(
                lambda: boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
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
                lambda: playergamelog.PlayerGameLog(player_id=player_id, season=season)
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
