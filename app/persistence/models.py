"""SQLAlchemy models for NBA Prop-Variance Engine"""

from datetime import date, datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    Date,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    Text,
    UniqueConstraint,
    JSON,
    Index,
)
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    """Base class for all models"""
    pass


class Player(Base):
    """Player model"""
    __tablename__ = "players"

    player_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        index=True
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    position: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    height: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    weight: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rookie_season: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )

    # Relationships
    game_stats: Mapped[list["PlayerGameStats"]] = relationship(
        back_populates="player",
        cascade="all, delete-orphan"
    )
    injuries: Mapped[list["InjuryReport"]] = relationship(
        back_populates="player",
        cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Player(player_id={self.player_id}, name={self.name})>"


class Team(Base):
    """Team model"""
    __tablename__ = "teams"

    team_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        index=True
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    city: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    abbreviation: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )

    # Relationships
    home_games: Mapped[list["Game"]] = relationship(
        "Game",
        foreign_keys="Game.home_team_id",
        back_populates="home_team"
    )
    away_games: Mapped[list["Game"]] = relationship(
        "Game",
        foreign_keys="Game.away_team_id",
        back_populates="away_team"
    )
    player_stats: Mapped[list["PlayerGameStats"]] = relationship(
        back_populates="team",
        cascade="all, delete-orphan"
    )
    injuries: Mapped[list["InjuryReport"]] = relationship(
        back_populates="team",
        cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Team(team_id={self.team_id}, name={self.name})>"


class Season(Base):
    """Season model"""
    __tablename__ = "seasons"

    season_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        index=True
    )
    year_start: Mapped[int] = mapped_column(Integer, nullable=False)
    year_end: Mapped[int] = mapped_column(Integer, nullable=False)
    season_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="Regular"
    )  # Regular or Playoffs

    # Relationships
    games: Mapped[list["Game"]] = relationship(
        back_populates="season",
        cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Season(season_id={self.season_id}, {self.year_start}-{self.year_end})>"


class Game(Base):
    """Game model - TimescaleDB hypertable"""
    __tablename__ = "games"

    game_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        index=True
    )
    season_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("seasons.season_id"),
        nullable=False,
        index=True
    )
    game_date: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        primary_key=True,  # Part of composite PK for TimescaleDB
        nullable=False,
        index=True
    )
    home_team_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("teams.team_id"),
        nullable=False,
        index=True
    )
    away_team_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("teams.team_id"),
        nullable=False,
        index=True
    )
    is_playoffs: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="Scheduled"
    )  # Scheduled, Final, Postponed
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False
    )

    # Relationships
    season: Mapped["Season"] = relationship(back_populates="games")
    home_team: Mapped["Team"] = relationship(
        foreign_keys=[home_team_id],
        back_populates="home_games"
    )
    away_team: Mapped["Team"] = relationship(
        foreign_keys=[away_team_id],
        back_populates="away_games"
    )
    player_stats: Mapped[list["PlayerGameStats"]] = relationship(
        back_populates="game",
        cascade="all, delete-orphan"
    )

    # Composite index for time-series queries
    # Note: No unique constraint on game_id alone - TimescaleDB requires partitioning column in unique constraints
    # Foreign keys will reference the composite key (game_id, game_date)
    __table_args__ = (
        Index("idx_games_date_team", "game_date", "home_team_id", "away_team_id"),
    )

    def __repr__(self) -> str:
        return f"<Game(game_id={self.game_id}, date={self.game_date})>"


class PlayerGameStats(Base):
    """Player game statistics - TimescaleDB hypertable"""
    __tablename__ = "player_game_stats"

    stat_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4
    )
    game_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        # Note: No FK constraint - TimescaleDB hypertables don't support FKs to non-partitioning columns
        # Referential integrity maintained at application level
        nullable=False,
        index=True
    )
    player_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("players.player_id"),
        nullable=False,
        index=True
    )
    team_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("teams.team_id"),
        nullable=False,
        index=True
    )
    game_date: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        primary_key=True,  # Part of composite PK for TimescaleDB
        nullable=False,
        index=True
    )
    minutes_played: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    points: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    rebounds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    assists: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    steals: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    blocks: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    turnovers: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    field_goals_made: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    field_goals_attempted: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    three_pointers_made: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    three_pointers_attempted: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    free_throws_made: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    free_throws_attempted: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    usage_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    true_shooting_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    started: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    advanced_metrics: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    # Relationships
    game: Mapped["Game"] = relationship(back_populates="player_stats")
    player: Mapped["Player"] = relationship(back_populates="game_stats")
    team: Mapped["Team"] = relationship(back_populates="player_stats")

    # Composite foreign key to games table (both game_id and game_date required for TimescaleDB)
    # Unique constraint to prevent duplicate stats (game_date is now in PK)
    __table_args__ = (
        ForeignKeyConstraint(
            ["game_id", "game_date"],
            ["games.game_id", "games.game_date"],
            name="fk_player_game_stats_game"
        ),
        UniqueConstraint("player_id", "game_id", name="uq_player_game_stats"),
        Index("idx_player_game_stats_player_date", "player_id", "game_date"),
    )

    def __repr__(self) -> str:
        return f"<PlayerGameStats(stat_id={self.stat_id}, player_id={self.player_id}, game_id={self.game_id})>"


class InjuryReport(Base):
    """Injury report model - TimescaleDB hypertable"""
    __tablename__ = "injury_reports"

    injury_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4
    )
    player_id: Mapped[Optional[UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("players.player_id"),
        nullable=True,  # Allow NULL for unmatched players
        index=True
    )
    team_id: Mapped[Optional[UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("teams.team_id"),
        nullable=True,  # Allow NULL for unmatched teams
        index=True
    )
    reported_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        primary_key=True,  # Part of composite PK for TimescaleDB
        nullable=False,
        index=True
    )
    injury_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    body_area: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    diagnosis: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False
    )  # Out, Questionable, Probable, Available
    effective_from: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    effective_until: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    validated_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True
    )

    # Relationships
    player: Mapped["Player"] = relationship(back_populates="injuries")
    team: Mapped["Team"] = relationship(back_populates="injuries")

    # Composite index for temporal joins
    __table_args__ = (
        Index("idx_injury_reports_player_reported", "player_id", "reported_at"),
    )

    def __repr__(self) -> str:
        return f"<InjuryReport(injury_id={self.injury_id}, player_id={self.player_id}, status={self.status})>"


class VarianceSnapshot(Base):
    """Variance snapshot model"""
    __tablename__ = "variance_snapshots"

    snapshot_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4
    )
    player_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("players.player_id"),
        nullable=False,
        index=True
    )
    game_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        # Note: No FK constraint - TimescaleDB hypertables don't support FKs to non-partitioning columns
        # Referential integrity maintained at application level
        nullable=False,
        index=True
    )
    metric_name: Mapped[str] = mapped_column(String(100), nullable=False)
    metric_value: Mapped[float] = mapped_column(Float, nullable=False)
    window_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    computed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True
    )

    def __repr__(self) -> str:
        return f"<VarianceSnapshot(snapshot_id={self.snapshot_id}, metric={self.metric_name})>"


class UsageRateChange(Base):
    """Usage rate change model"""
    __tablename__ = "usage_rate_changes"

    change_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4
    )
    player_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("players.player_id"),
        nullable=False,
        index=True
    )
    game_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        # Note: No FK constraint - TimescaleDB hypertables don't support FKs to non-partitioning columns
        # Referential integrity maintained at application level
        nullable=False,
        index=True
    )
    trigger_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False
    )  # TeammateInjury, LineupChange
    trigger_player_id: Mapped[Optional[UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("players.player_id"),
        nullable=True
    )
    usage_before: Mapped[float] = mapped_column(Float, nullable=False)
    usage_after: Mapped[float] = mapped_column(Float, nullable=False)
    change_pct: Mapped[float] = mapped_column(Float, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True
    )

    def __repr__(self) -> str:
        return f"<UsageRateChange(change_id={self.change_id}, player_id={self.player_id}, change_pct={self.change_pct})>"
