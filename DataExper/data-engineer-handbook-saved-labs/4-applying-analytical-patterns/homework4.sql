--------------------------------------------------------------------------------
-- Assignment 1: State Change Tracking for Players
-- Requirements:
--  - A new player is 'New'
--  - A player leaving the league is 'Retired'
--  - A player staying in the league is 'Continued Playing'
--  - A player coming out of retirement is 'Returned from Retirement'
--  - A player staying out is 'Stayed Retired'
--
-- This query uses a FULL OUTER JOIN between yesterday's snapshot and the current season.
--------------------------------------------------------------------------------

-- Create the target table (if not already created) for tracking players’ state.
CREATE TABLE IF NOT EXISTS players_growth_accounting (
    player_name TEXT,
    current_year INT,
    first_active_year INT,
    last_active_year INT,
    player_state TEXT,
    years_active INT[],
    PRIMARY KEY (player_name, current_year)
);

-- Insert state change data into players_growth_accounting
WITH yesterday AS (
    -- Last year's snapshot (e.g., 2005)
    SELECT 
        player_name,
        current_year,
        first_active_year,
        last_active_year,
        years_active
    FROM players_growth_accounting
    WHERE current_year = 2005
),
today AS (
    -- Current season's players (e.g., 2006)
    SELECT
        player_name,
        season AS current_year
    FROM player_seasons
    WHERE season = 2006
)
INSERT INTO players_growth_accounting (
    player_name,
    current_year,
    first_active_year,
    last_active_year,
    player_state,
    years_active
)
SELECT
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.current_year, y.current_year + 1) AS current_year,
    -- If the player did not exist in yesterday, the first active year is the current year.
    COALESCE(y.first_active_year, t.current_year) AS first_active_year,
    -- If present today, update last_active_year to current year; else retain yesterday’s last active year.
    COALESCE(t.current_year, y.last_active_year) AS last_active_year,
    CASE
        WHEN y.player_name IS NULL THEN 'New'
        -- Player existed before, but is missing now; if their last active year was already set to an earlier year,
        -- assume they have been retired for some time so "Stayed Retired"
        WHEN t.player_name IS NULL AND y.current_year <> y.last_active_year THEN 'Stayed Retired'
        WHEN t.player_name IS NULL THEN 'Retired'
        WHEN t.current_year - y.last_active_year = 1 THEN 'Continued Playing'
        WHEN t.current_year - y.last_active_year > 1 THEN 'Returned from Retirement'
        ELSE 'Unknown'
    END AS player_state,
    CASE
        WHEN t.current_year IS NULL THEN y.years_active
        WHEN y.years_active IS NULL THEN ARRAY[t.current_year]
        ELSE y.years_active || ARRAY[t.current_year]
    END AS years_active
FROM yesterday y
FULL OUTER JOIN today t
    ON y.player_name = t.player_name;

--------------------------------------------------------------------------------
-- Assignment 2: Aggregations Using GROUPING SETS
-- Requirements:
--  - Aggregate by player and team (e.g., who scored the most points for one team)
--  - Aggregate by player and season (e.g., who scored the most points in one season)
--  - Aggregate by team (e.g., which team won the most games)
--
-- This query uses GROUPING SETS to produce three aggregation levels.
--------------------------------------------------------------------------------

WITH games_augmented AS (
    SELECT
        g.game_id,
        g.season,
        gd.player_name,
        gd.team_id,
        gd.pts
    FROM games g
    LEFT JOIN game_details gd
        ON g.game_id = gd.game_id
)
SELECT
    CASE
        WHEN GROUPING(player_name) = 0 AND GROUPING(team_id) = 0 THEN 'Aggregation: Player & Team'
        WHEN GROUPING(player_name) = 0 AND GROUPING(season) = 0 THEN 'Aggregation: Player & Season'
        WHEN GROUPING(team_id) = 0 THEN 'Aggregation: Team'
        ELSE 'Other'
    END AS aggregation_level,
    player_name,
    season,
    team_id,
    COUNT(*) AS games_played,
    SUM(pts) AS total_points
FROM games_augmented
GROUP BY GROUPING SETS (
    -- Aggregation by player and team
    (player_name, team_id),
    -- Aggregation by player and season
    (player_name, season),
    -- Aggregation by team
    (team_id)
)
ORDER BY aggregation_level;

--------------------------------------------------------------------------------
-- Assignment 3: Window Functions on game_details
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Part A: Maximum Wins in a 90-game Stretch for a Team
-- Requirements: Identify the most wins a team had in any 90-game period.
-- Approach: Create a union of home and away game records, then use a window SUM with a frame covering 90 games.
--------------------------------------------------------------------------------

WITH all_games AS (
    -- Uniform schema: game_date_est, game_id, team_id, and win (1 for win, 0 for loss)
    SELECT
        game_date_est,
        game_id,
        home_team_id AS team_id,
        home_team_wins AS win
    FROM games
    UNION ALL
    SELECT
        game_date_est,
        game_id,
        visitor_team_id AS team_id,
        1 - home_team_wins AS win
    FROM games
),
rolling_wins AS (
    SELECT
        team_id,
        game_date_est,
        -- Window frame covering the current game and the previous 89 games (a total of 90 games)
        SUM(win) OVER(
            PARTITION BY team_id 
            ORDER BY game_date_est 
            ROWS 89 PRECEDING
        ) AS wins_in_90_games
    FROM all_games
)
SELECT
    team_id,
    MAX(wins_in_90_games) AS max_wins_in_90_games
FROM rolling_wins
GROUP BY team_id
ORDER BY max_wins_in_90_games DESC
LIMIT 1;

--------------------------------------------------------------------------------
-- Part B: Longest Streak for LeBron James (Scoring > 10 Points per Game)
-- Requirements: Determine the longest sequence of consecutive games where LeBron James scored over 10 points.
-- Approach: Use a technique that computes row numbers to group consecutive streaks.
--------------------------------------------------------------------------------

WITH lebron_games AS (
    SELECT
        g.game_date_est,
        gd.game_id,
        gd.player_name,
        gd.pts,
        CASE
            WHEN gd.pts > 10 THEN 1
            ELSE 0
        END AS over_10_pts
    FROM game_details gd
    JOIN games g
        ON gd.game_id = g.game_id
    WHERE gd.player_name = 'LeBron James'
),
streaks AS (
    SELECT
        game_date_est,
        game_id,
        player_name,
        pts,
        over_10_pts,
        -- Generate a grouping number by subtracting row numbers computed over different partitions.
        ROW_NUMBER() OVER(PARTITION BY player_name ORDER BY game_date_est) -
        ROW_NUMBER() OVER(PARTITION BY player_name, over_10_pts ORDER BY game_date_est) AS streak_group
    FROM lebron_games
)
SELECT
    player_name,
    streak_group,
    COUNT(*) AS streak_length
FROM streaks
WHERE over_10_pts = 1
GROUP BY player_name, streak_group
ORDER BY streak_length DESC
LIMIT 1;
