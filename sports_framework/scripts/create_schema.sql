-- Sports Betting Database Schema
-- Run this to initialize the database

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- CORE TABLES (Sport-agnostic)
-- =============================================================================

-- Betting lines from all sportsbooks
CREATE TABLE IF NOT EXISTS betting_lines (
    game_id VARCHAR(50) NOT NULL,
    sportsbook VARCHAR(100) NOT NULL,
    sport VARCHAR(10) NOT NULL,
    source VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP NOT NULL,
    
    -- Spread
    home_spread DECIMAL(5,2),
    home_spread_odds INTEGER,
    away_spread DECIMAL(5,2),
    away_spread_odds INTEGER,
    
    -- Total
    total DECIMAL(5,1),
    over_odds INTEGER,
    under_odds INTEGER,
    
    -- Moneyline
    home_moneyline INTEGER,
    away_moneyline INTEGER,
    
    -- Metadata
    last_updated TIMESTAMP,
    is_open BOOLEAN DEFAULT TRUE,
    
    PRIMARY KEY (game_id, sportsbook)
);

-- Weather forecasts
CREATE TABLE IF NOT EXISTS weather_forecasts (
    game_id VARCHAR(50) NOT NULL,
    venue_id VARCHAR(50),
    sport VARCHAR(10) NOT NULL,
    source VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP NOT NULL,
    
    -- Conditions
    temperature_f DECIMAL(5,1),
    feels_like_f DECIMAL(5,1),
    humidity_pct DECIMAL(5,1),
    wind_speed_mph DECIMAL(5,1),
    wind_gusts_mph DECIMAL(5,1),
    wind_direction VARCHAR(10),
    
    -- Precipitation
    precipitation_pct DECIMAL(5,1),
    precipitation_type VARCHAR(50),
    precipitation_amount DECIMAL(5,2),
    
    -- Visibility and conditions
    visibility_mi DECIMAL(5,1),
    conditions VARCHAR(100),
    
    -- Impact assessment
    passing_impact_score DECIMAL(5,1),
    kicking_impact_score DECIMAL(5,1),
    overall_impact VARCHAR(20),
    
    -- Betting implications
    under_favorable BOOLEAN DEFAULT FALSE,
    rushing_favorable BOOLEAN DEFAULT FALSE,
    
    -- Forecast metadata
    forecast_hour TIMESTAMP NOT NULL,
    is_dome BOOLEAN DEFAULT FALSE,
    
    PRIMARY KEY (game_id, forecast_hour)
);

-- =============================================================================
-- NFL TABLES
-- =============================================================================

-- NFL games
CREATE TABLE IF NOT EXISTS nfl_games (
    game_id VARCHAR(50) NOT NULL PRIMARY KEY,
    sport VARCHAR(10) NOT NULL DEFAULT 'nfl',
    source VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP NOT NULL,
    
    -- Game info
    game_date DATE NOT NULL,
    game_time TIME,
    game_datetime TIMESTAMP,
    
    -- Teams
    home_team_id VARCHAR(10) NOT NULL,
    away_team_id VARCHAR(10) NOT NULL,
    
    -- Season info
    season INTEGER NOT NULL,
    week INTEGER,
    game_type VARCHAR(20) DEFAULT 'regular',
    
    -- Venue
    venue_id VARCHAR(50),
    venue_name VARCHAR(200),
    is_neutral_site BOOLEAN DEFAULT FALSE,
    
    -- Status
    status VARCHAR(20) DEFAULT 'scheduled',
    is_completed BOOLEAN DEFAULT FALSE,
    
    -- Scores
    home_score INTEGER,
    away_score INTEGER,
    
    -- Periods
    current_period INTEGER,
    time_remaining VARCHAR(20),
    
    -- Metadata
    attendance INTEGER,
    duration VARCHAR(20),
    
    -- Betting context
    is_primetime BOOLEAN DEFAULT FALSE,
    is_division_game BOOLEAN DEFAULT FALSE,
    is_conference_game BOOLEAN DEFAULT FALSE
);

-- NFL teams
CREATE TABLE IF NOT EXISTS nfl_teams (
    team_id VARCHAR(10) NOT NULL PRIMARY KEY,
    sport VARCHAR(10) NOT NULL DEFAULT 'nfl',
    source VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP NOT NULL,
    
    -- Team info
    name VARCHAR(100) NOT NULL,
    abbreviation VARCHAR(10) NOT NULL,
    city VARCHAR(100) NOT NULL,
    conference VARCHAR(10),
    division VARCHAR(10),
    
    -- Venue
    venue_name VARCHAR(200),
    venue_city VARCHAR(100),
    venue_state VARCHAR(50),
    venue_capacity INTEGER,
    
    -- Location for weather
    venue_lat DECIMAL(10,6),
    venue_lon DECIMAL(10,6),
    is_dome BOOLEAN DEFAULT FALSE,
    
    -- Branding
    primary_color VARCHAR(7),
    secondary_color VARCHAR(7),
    logo_url VARCHAR(500),
    
    -- Current season stats
    wins INTEGER,
    losses INTEGER,
    win_pct DECIMAL(5,3)
);

-- NFL players
CREATE TABLE IF NOT EXISTS nfl_players (
    player_id VARCHAR(50) NOT NULL PRIMARY KEY,
    sport VARCHAR(10) NOT NULL DEFAULT 'nfl',
    source VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP NOT NULL,
    
    -- Personal info
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    full_name VARCHAR(200) NOT NULL,
    jersey_number INTEGER,
    
    -- Team
    team_id VARCHAR(10),
    
    -- Position
    position VARCHAR(20) NOT NULL,
    position_category VARCHAR(20),
    
    -- Physical
    height VARCHAR(10),
    weight INTEGER,
    age INTEGER,
    
    -- Status
    status VARCHAR(20) DEFAULT 'active',
    injury_status VARCHAR(20),
    
    -- Experience
    years_experience INTEGER,
    is_rookie BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    headshot_url VARCHAR(500)
);

-- NFL injury reports
CREATE TABLE IF NOT EXISTS nfl_injury_reports (
    player_id VARCHAR(50) NOT NULL,
    team_id VARCHAR(10) NOT NULL,
    sport VARCHAR(10) NOT NULL DEFAULT 'nfl',
    source VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP NOT NULL,
    
    -- Injury details
    injury_type VARCHAR(100) NOT NULL,
    body_part VARCHAR(50),
    severity VARCHAR(50),
    
    -- Status
    status VARCHAR(20) NOT NULL,
    practice_status VARCHAR(50),
    
    -- Timeline
    report_date DATE NOT NULL,
    expected_return DATE,
    
    -- Impact
    impact_score INTEGER,
    is_starter BOOLEAN DEFAULT FALSE,
    position_depth INTEGER,
    
    PRIMARY KEY (player_id, report_date)
);

-- NFL team statistics
CREATE TABLE IF NOT EXISTS nfl_team_stats (
    team_id VARCHAR(10) NOT NULL,
    season INTEGER NOT NULL,
    sport VARCHAR(10) NOT NULL DEFAULT 'nfl',
    source VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP NOT NULL,
    
    -- Offense
    points_per_game DECIMAL(5,1),
    total_yards_per_game DECIMAL(5,1),
    passing_yards_per_game DECIMAL(5,1),
    rushing_yards_per_game DECIMAL(5,1),
    yards_per_play DECIMAL(5,1),
    
    -- Efficiency
    third_down_pct DECIMAL(5,1),
    red_zone_td_pct DECIMAL(5,1),
    fourth_down_pct DECIMAL(5,1),
    
    -- Possession
    avg_time_of_possession VARCHAR(10),
    plays_per_game DECIMAL(5,1),
    
    -- Turnovers
    turnovers INTEGER,
    fumbles_lost INTEGER,
    interceptions_thrown INTEGER,
    
    -- Defense
    points_allowed_per_game DECIMAL(5,1),
    yards_allowed_per_game DECIMAL(5,1),
    passing_yards_allowed_per_game DECIMAL(5,1),
    rushing_yards_allowed_per_game DECIMAL(5,1),
    turnovers_forced INTEGER,
    sacks INTEGER,
    interceptions INTEGER,
    
    PRIMARY KEY (team_id, season)
);

-- NFL player statistics
CREATE TABLE IF NOT EXISTS nfl_player_stats (
    player_id VARCHAR(50) NOT NULL,
    season INTEGER NOT NULL,
    sport VARCHAR(10) NOT NULL DEFAULT 'nfl',
    source VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP NOT NULL,
    
    -- Passing
    passing_yards INTEGER,
    passing_tds INTEGER,
    interceptions INTEGER,
    completion_pct DECIMAL(5,1),
    passer_rating DECIMAL(5,1),
    
    -- Rushing
    rushing_yards INTEGER,
    rushing_tds INTEGER,
    rushing_attempts INTEGER,
    yards_per_carry DECIMAL(5,1),
    
    -- Receiving
    receiving_yards INTEGER,
    receiving_tds INTEGER,
    receptions INTEGER,
    targets INTEGER,
    
    -- Defense
    tackles INTEGER,
    sacks DECIMAL(5,1),
    interceptions_def INTEGER,
    forced_fumbles INTEGER,
    
    -- Kicking
    field_goals_made INTEGER,
    field_goals_attempted INTEGER,
    extra_points_made INTEGER,
    extra_points_attempted INTEGER,
    
    PRIMARY KEY (player_id, season)
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Betting lines indexes
CREATE INDEX IF NOT EXISTS idx_betting_lines_game_date ON betting_lines(game_id, last_updated);
CREATE INDEX IF NOT EXISTS idx_betting_lines_sport ON betting_lines(sport);

-- Weather forecasts indexes
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_game ON weather_forecasts(game_id, forecast_hour);
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_sport ON weather_forecasts(sport);

-- NFL games indexes
CREATE INDEX IF NOT EXISTS idx_nfl_games_date ON nfl_games(game_date, status);
CREATE INDEX IF NOT EXISTS idx_nfl_games_season_week ON nfl_games(season, week);
CREATE INDEX IF NOT EXISTS idx_nfl_games_teams ON nfl_games(home_team_id, away_team_id);

-- NFL teams indexes
CREATE INDEX IF NOT EXISTS idx_nfl_teams_conference ON nfl_teams(conference, division);

-- NFL players indexes
CREATE INDEX IF NOT EXISTS idx_nfl_players_team ON nfl_players(team_id, status);
CREATE INDEX IF NOT EXISTS idx_nfl_players_position ON nfl_players(position);

-- NFL injury reports indexes
CREATE INDEX IF NOT EXISTS idx_nfl_injuries_team ON nfl_injury_reports(team_id, report_date);
CREATE INDEX IF NOT EXISTS idx_nfl_injuries_player ON nfl_injury_reports(player_id, report_date);
CREATE INDEX IF NOT EXISTS idx_nfl_injuries_status ON nfl_injury_reports(status, report_date);

-- NFL stats indexes
CREATE INDEX IF NOT EXISTS idx_nfl_team_stats_season ON nfl_team_stats(season);
CREATE INDEX IF NOT EXISTS idx_nfl_player_stats_season ON nfl_player_stats(season);

-- =============================================================================
-- VIEWS FOR COMMON QUERIES
-- =============================================================================

-- Today's games with betting lines and weather
CREATE OR REPLACE VIEW today_games AS
SELECT 
    g.*,
    bl.home_spread,
    bl.away_spread,
    bl.total,
    wf.temperature_f,
    wf.conditions,
    wf.overall_impact as weather_impact
FROM nfl_games g
LEFT JOIN betting_lines bl ON g.game_id = bl.game_id
LEFT JOIN weather_forecasts wf ON g.game_id = wf.game_id 
    AND wf.forecast_hour = (g.game_date + interval '1 day')
WHERE g.game_date = CURRENT_DATE
    AND g.status != 'cancelled';

-- Team performance summary
CREATE OR REPLACE VIEW team_performance AS
SELECT 
    t.*,
    ts.points_per_game,
    ts.total_yards_per_game,
    ts.wins,
    ts.losses,
    ts.win_pct
FROM nfl_teams t
LEFT JOIN nfl_team_stats ts ON t.team_id = ts.team_id 
    AND ts.season = EXTRACT(YEAR FROM CURRENT_DATE);

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE betting_lines IS 'Betting lines from multiple sportsbooks for all sports';
COMMENT ON TABLE weather_forecasts IS 'Weather forecasts for upcoming games';
COMMENT ON TABLE nfl_games IS 'NFL game schedule and results';
COMMENT ON TABLE nfl_teams IS 'NFL team information and metadata';
COMMENT ON TABLE nfl_players IS 'NFL player profiles and current status';
COMMENT ON TABLE nfl_injury_reports IS 'NFL injury reports with player status';
COMMENT ON TABLE nfl_team_stats IS 'NFL team statistics for offense and defense';
COMMENT ON TABLE nfl_player_stats IS 'NFL player statistics by season';

-- =============================================================================
-- END
-- =============================================================================