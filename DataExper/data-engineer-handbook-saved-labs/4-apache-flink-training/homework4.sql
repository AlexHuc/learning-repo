-- *******************************************************************
-- SQL Script: Sessionized Events Analysis
--
-- This script performs analytical queries on the sessionized events
-- stored in the 'sessionized_events' table. This table is populated 
-- by the Flink job that sessionizes events based on IP address and host,
-- using a 5 minute inactivity gap to define sessions.
--
-- Table schema for sessionized_events:
--   - ip           : VARCHAR    -- The IP address from which the events originated.
--   - host         : VARCHAR    -- The host (e.g., website or subdomain) where the event occurred.
--   - session_start: TIMESTAMP(3) -- The start time of the session.
--   - session_end  : TIMESTAMP(3) -- The end time of the session.
--   - num_events   : BIGINT     -- The number of events in the session.
-- *******************************************************************

-- Query 1:
-- Calculate the average number of web events per session for a specific host.
-- In this case, we're examining the sessions for the host 'zachwilson.techcreator.io'.
SELECT 
    AVG(num_events)::numeric(10,2) AS avg_events
FROM 
    sessionized_events
WHERE 
    host = 'zachwilson.techcreator.io';

-- Expected Result:
-- A single row with the column "avg_events" displaying the average number of events per session 
-- for users on zachwilson.techcreator.io.


-- Query 2:
-- Compare session metrics across different hosts by grouping the sessionized data.
-- For each host, we calculate:
--   - total_sessions: the total number of sessions recorded.
--   - avg_events    : the average number of events per session.
--   - total_events  : the total count of events across all sessions.
SELECT 
    host,
    COUNT(*) AS total_sessions,
    AVG(num_events)::numeric(10,2) AS avg_events,
    SUM(num_events) AS total_events
FROM 
    sessionized_events
GROUP BY 
    host
ORDER BY 
    host;

-- Expected Result:
-- Multiple rows, one per host, with session metrics that allow you to compare:
--   - Which host generates more sessions,
--   - The average session activity,
--   - And the overall volume of events.
