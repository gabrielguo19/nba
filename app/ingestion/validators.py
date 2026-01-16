"""Pydantic validators for raw NBA data before DataFrame conversion"""

from datetime import date, datetime
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, ConfigDict


class RawPlayerData(BaseModel):
    """Raw player data from nba_api"""
    model_config = ConfigDict(extra="allow")  # Allow extra fields from API
    
    player_id: Optional[int] = None
    name: str
    position: Optional[str] = None
    height: Optional[str] = None  # Format: "6-8" or "203cm"
    weight: Optional[int] = None  # In pounds
    rookie_season: Optional[int] = None
    
    @field_validator("height")
    @classmethod
    def parse_height(cls, v: Optional[str]) -> Optional[float]:
        """Convert height string to float (meters)"""
        if not v:
            return None
        # Handle "6-8" format (feet-inches)
        if "-" in v:
            parts = v.split("-")
            if len(parts) == 2:
                feet = float(parts[0])
                inches = float(parts[1])
                return (feet * 12 + inches) * 0.0254  # Convert to meters
        # Handle "203cm" format
        if "cm" in v.lower():
            return float(v.replace("cm", "").strip()) / 100
        return None


class RawTeamData(BaseModel):
    """Raw team data from nba_api"""
    model_config = ConfigDict(extra="allow")
    
    team_id: Optional[int] = None
    name: str
    city: Optional[str] = None
    abbreviation: Optional[str] = None


class RawGameData(BaseModel):
    """Raw game data from nba_api"""
    model_config = ConfigDict(extra="allow")
    
    game_id: str
    season_id: Optional[str] = None
    game_date: datetime
    home_team_id: Optional[int] = None
    away_team_id: Optional[int] = None
    is_playoffs: bool = False
    status: str = "Scheduled"
    
    @field_validator("game_date", mode="before")
    @classmethod
    def parse_game_date(cls, v):
        """Parse various date formats"""
        if isinstance(v, str):
            # Try common formats
            for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]:
                try:
                    return datetime.strptime(v, fmt)
                except ValueError:
                    continue
        return v


class RawPlayerGameStats(BaseModel):
    """Raw player game statistics from nba_api"""
    model_config = ConfigDict(extra="allow")
    
    game_id: str
    player_id: Optional[int] = None
    team_id: Optional[int] = None
    game_date: datetime
    minutes_played: Optional[float] = None
    points: Optional[int] = None
    rebounds: Optional[int] = None
    assists: Optional[int] = None
    steals: Optional[int] = None
    blocks: Optional[int] = None
    turnovers: Optional[int] = None
    field_goals_made: Optional[int] = None
    field_goals_attempted: Optional[int] = None
    three_pointers_made: Optional[int] = None
    three_pointers_attempted: Optional[int] = None
    free_throws_made: Optional[int] = None
    free_throws_attempted: Optional[int] = None
    usage_rate: Optional[float] = None
    true_shooting_pct: Optional[float] = None
    started: bool = False
    
    # Advanced metrics (optional, stored in JSONB)
    advanced_metrics: Optional[dict] = None
    
    @field_validator("game_date", mode="before")
    @classmethod
    def parse_game_date(cls, v):
        """Parse various date formats"""
        if isinstance(v, str):
            for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]:
                try:
                    return datetime.strptime(v, fmt)
                except ValueError:
                    continue
        return v


class RawInjuryReport(BaseModel):
    """Raw injury report data from scraping"""
    model_config = ConfigDict(extra="allow")
    
    player_id: Optional[int] = None
    player_name: str
    team_id: Optional[int] = None
    team_name: Optional[str] = None
    reported_at: datetime
    injury_type: Optional[str] = None
    body_area: Optional[str] = None
    diagnosis: Optional[str] = None
    status: str  # Out, Questionable, Probable, Available
    effective_from: Optional[date] = None
    effective_until: Optional[date] = None
    source_url: Optional[str] = None
    
    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Normalize injury status"""
        v = v.strip().title()
        valid_statuses = ["Out", "Questionable", "Probable", "Available", "Day-To-Day"]
        if v not in valid_statuses:
            # Try to map common variations
            status_map = {
                "doubtful": "Questionable",
                "dtd": "Day-To-Day",
                "injured": "Out",
                "healthy": "Available"
            }
            v = status_map.get(v.lower(), "Questionable")
        return v


class ValidatedSeason(BaseModel):
    """Validated season data"""
    season_id: UUID = Field(default_factory=uuid4)
    year_start: int
    year_end: int
    season_type: str = "Regular"
    
    @field_validator("year_end")
    @classmethod
    def validate_year_end(cls, v: int, info) -> int:
        """Ensure year_end is year_start + 1"""
        if hasattr(info, "data") and "year_start" in info.data:
            expected = info.data["year_start"] + 1
            if v != expected:
                return expected
        return v
