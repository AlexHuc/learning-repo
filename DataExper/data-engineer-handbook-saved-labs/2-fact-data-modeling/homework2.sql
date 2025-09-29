---------------------------
-- Assignment 1: De-duplication Query
---------------------------
/*
  Deduplicate the game_details table.
  For each game_id, team_id, and player_id combination, we take the first occurrence (ROW_NUMBER = 1)
  based on game_date_est. Additional flags (DNP, DND, NWT) and computed minutes are included.
*/
WITH deduped AS (
    SELECT
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        ROW_NUMBER() OVER(
            PARTITION BY gd.game_id, gd.team_id, gd.player_id 
            ORDER BY g.game_date_est
        ) AS row_num
    FROM game_details gd
    JOIN games g
        ON gd.game_id = g.game_id
)
SELECT 
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    -- Create a boolean flag to indicate if the player is playing at home:
    (team_id = home_team_id) AS dim_is_playing_at_home,
    -- Flag if player did not play based on comment substring search:
    (COALESCE(POSITION('DNP' IN comment), 0) > 0) AS dim_did_not_play,
    (COALESCE(POSITION('DND' IN comment), 0) > 0) AS dim_did_not_dress,
    (COALESCE(POSITION('NWT' IN comment), 0) > 0) AS dim_did_not_team,
    -- Compute game minutes (minutes and seconds conversion to a single REAL value):
    CAST(SPLIT_PART(min, ':', 1) AS REAL)
        + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    "TO" AS m_turnovers,
    pf AS m_pf,
    plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1;

---------------------------
-- Assignment 2: DDL for User Devices Cumulated Table
---------------------------
/*
  This table tracks each user’s device activity.
  For a given user/device/browser combination and date, we store a cumulative list of active dates.
  A NOT NULL constraint is added where needed for data integrity.
*/
CREATE TABLE user_devices_cumulated (
    user_id NUMERIC NOT NULL,
    device_id NUMERIC NOT NULL,
    browser_type TEXT NOT NULL,
    date DATE NOT NULL,
    device_activity_datelist DATE[] NOT NULL,  -- Array of active dates
    PRIMARY KEY (user_id, device_id, browser_type, date)
);

---------------------------
-- Assignment 3: Incremental Query for Device Activity
---------------------------
/*
  This query increments the cumulative device activity.
  It uses today's events (for a specific day, e.g., '2023-01-31') and combines them with yesterday’s cumulative record.
  A FULL OUTER JOIN is used to cover both new and existing user/device/browser combinations.
*/
INSERT INTO user_devices_cumulated
WITH today AS (
    SELECT
        e.user_id,
        DATE(e.event_time) AS date,
        e.device_id,
        d.browser_type,
        ROW_NUMBER() OVER(
            PARTITION BY e.user_id, e.device_id, d.browser_type 
            ORDER BY e.event_time
        ) AS row_num
    FROM events AS e
    LEFT JOIN devices AS d
        ON e.device_id = d.device_id
    WHERE DATE(e.event_time) = '2023-01-31'
      AND e.user_id IS NOT NULL
      AND e.device_id IS NOT NULL
),
deduped_today AS (
    SELECT *
    FROM today
    WHERE row_num = 1
),
yesterday AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE date = '2023-01-30'
)
SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.device_id, y.device_id) AS device_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    -- Use today's date if exists; otherwise, assume yesterday's date incremented by one day
    COALESCE(t.date, y.date + INTERVAL '1 DAY')::DATE AS date,
    CASE
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date]
        WHEN t.date IS NULL THEN y.device_activity_datelist
        ELSE y.device_activity_datelist || ARRAY[t.date]
    END AS device_activity_datelist
FROM deduped_today t
FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id
    AND t.device_id = y.device_id
    AND t.browser_type = y.browser_type;

---------------------------
-- Assignment 4: Generate Device Activity Int Datelist
---------------------------
/*
  Convert the device_activity_datelist into an integer using a bitwise representation.
  Each date in the cumulative list corresponds to a bit in a 32-bit integer,
  where the bit position is determined by the difference between the current date and the series date.
*/
WITH user_devices AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE date = '2023-01-31'
),
series AS (
    -- Generate a series of dates for the full month (for bit positions)
    SELECT generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 DAY') AS series_date
),
place_holder_ints AS (
    SELECT
        ud.user_id,
        ud.device_id,
        ud.browser_type,
        ud.device_activity_datelist,
        s.series_date,
        -- If the device_activity_datelist contains the generated series_date,
        -- then compute the bit value using POWER(2, (bit position)).
        CASE
            WHEN ud.device_activity_datelist @> ARRAY[s.series_date]
                THEN CAST(POWER(2, 32 - (EXTRACT(DAY FROM (ud.date - s.series_date))::INTEGER) - 1) AS BIGINT)
            ELSE 0
        END AS placeholder_int_value
    FROM user_devices ud
    CROSS JOIN series s
)
SELECT
    user_id,
    device_id,
    browser_type,
    device_activity_datelist,
    CAST(SUM(placeholder_int_value) AS BIGINT) AS datelist_int
FROM place_holder_ints
GROUP BY user_id, device_id, browser_type, device_activity_datelist;

---------------------------
-- Assignment 5: DDL for Hosts Cumulated Table
---------------------------
/*
  This table logs cumulative host activity.
  For each host per month (month_start), we store an array of dates during which the host had activity.
*/
CREATE TABLE hosts_cumulated (
    host TEXT NOT NULL,
    month_start DATE NOT NULL,
    host_activity_datelist DATE[] NOT NULL,
    PRIMARY KEY (host, month_start)
);

---------------------------
-- Assignment 6: Incremental Query for Host Activity
---------------------------
/*
  Incrementally update the hosts_cumulated table.
  Today's events are combined with yesterday's cumulative host activity.
  The ON CONFLICT clause updates the host_activity_datelist if the record already exists.
*/
INSERT INTO hosts_cumulated
WITH today AS (
    SELECT
        host,
        DATE(event_time) AS date
    FROM events
    WHERE DATE(event_time) = '2023-01-08'
    GROUP BY host, DATE(event_time)
),
yesterday AS (
    SELECT *
    FROM hosts_cumulated
    WHERE month_start = DATE_TRUNC('month', '2023-01-08'::DATE)
)
SELECT
    COALESCE(t.host, y.host) AS host,
    COALESCE(DATE_TRUNC('month', t.date), y.month_start) AS month_start,
    CASE
        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date]
        WHEN t.date IS NULL THEN y.host_activity_datelist
        ELSE y.host_activity_datelist || ARRAY[t.date]
    END AS host_activity_datelist
FROM today t
FULL OUTER JOIN yesterday y
    ON t.host = y.host
ON CONFLICT (host, month_start)
DO UPDATE SET host_activity_datelist = EXCLUDED.host_activity_datelist;

---------------------------
-- Assignment 7: Reduced Host Fact Array DDL
---------------------------
/*
  Define a reduced fact table for host activity aggregated by month.
  Each record includes:
    - hit_array: an array where each entry represents the count of hits (COUNT(1)) for a day.
    - unique_visitors: an array where each entry represents the count of distinct user_id per day.
*/
CREATE TABLE host_activity_reduced (
    host TEXT NOT NULL,
    month_start DATE NOT NULL,
    hit_array BIGINT[] NOT NULL,
    unique_visitors BIGINT[] NOT NULL,
    PRIMARY KEY (host, month_start)
);

---------------------------
-- Assignment 8: Incremental Query for Reduced Host Fact
---------------------------
/*
  Incrementally update the host_activity_reduced table on a day-by-day basis.
  For the specific day (e.g., '2023-01-01'), the query aggregates hits and unique visitors by host.
  It concatenates today's counts to the existing arrays.
*/
INSERT INTO host_activity_reduced
WITH today AS (
    SELECT
        host,
        DATE(event_time) AS date,
        COUNT(1) AS hits,
        COUNT(DISTINCT user_id) AS unique_visitors
    FROM events
    WHERE DATE(event_time) = '2023-01-01'
    GROUP BY host, DATE(event_time)
),
yesterday AS (
    SELECT *
    FROM host_activity_reduced
    WHERE month_start = DATE_TRUNC('month', '2023-01-01'::DATE)
)
SELECT
    COALESCE(t.host, y.host) AS host,
    COALESCE(DATE_TRUNC('month', t.date), y.month_start) AS month_start,
    CASE
        WHEN y.hit_array IS NOT NULL
            THEN y.hit_array || ARRAY[t.hits]
        ELSE ARRAY[t.hits]
    END AS hit_array,
    CASE
        WHEN y.unique_visitors IS NOT NULL
            THEN y.unique_visitors || ARRAY[t.unique_visitors]
        ELSE ARRAY[t.unique_visitors]
    END AS unique_visitors
FROM today t
FULL OUTER JOIN yesterday y
    ON t.host = y.host
ON CONFLICT (host, month_start)
DO UPDATE
    SET hit_array = EXCLUDED.hit_array,
        unique_visitors = EXCLUDED.unique_visitors;
