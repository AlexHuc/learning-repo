import os
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import col
from pyflink.table.window import Session

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_events_source(t_env):
    """
    Creates a source table from Kafka for web events.
    Assumes the table has:
      - ip: string,
      - host: string,
      - event_time: string (in ISO format, e.g. 2025-01-18T19:08:13.123Z)
    
    The event_time is converted into a TIMESTAMP column named event_timestamp.
    A watermark is generated 5 seconds behind the event_timestamp.
    """
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    ddl = f"""
        CREATE TABLE events (
            ip VARCHAR,
            host VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.environ.get("KAFKA_WEB_TRAFFIC_KEY")}" password="{os.environ.get("KAFKA_WEB_TRAFFIC_SECRET")}";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        )
    """
    t_env.execute_sql(ddl)
    logger.info("Created source table 'events'.")
    return "events"

def create_sessions_sink(t_env):
    """
    Creates a JDBC sink table for sessionized events.
    
    In Flink’s catalog we create a table named 'sessionized_events'
    and map it to the target JDBC table 'public.sessionized_events' in PostgreSQL.
    """
    flink_table_name = "sessionized_events"          # Flink's internal table name
    jdbc_table_name = "public.sessionized_events"      # Target table in PostgreSQL
    
    ddl = f"""
        CREATE TABLE {flink_table_name} (
            ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            num_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{jdbc_table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        )
    """
    t_env.execute_sql(ddl)
    logger.info(f"Created sink table '{flink_table_name}' mapped to JDBC table '{jdbc_table_name}'.")
    return flink_table_name

def sessionize_events():
    try:
        # Set up the execution environment and enable checkpointing
        env = StreamExecutionEnvironment.get_execution_environment()
        env.enable_checkpointing(10 * 1000)  # every 10 seconds
        # Note: setCheckpointTimeout is not used, as it is not available in this version.
        
        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        t_env = StreamTableEnvironment.create(env, environment_settings=settings)

        source_table = create_events_source(t_env)
        sink_table = create_sessions_sink(t_env)

        # For debugging, print a few rows from the source table to verify that
        # the timestamp conversion is working and event_timestamp is not null.
        logger.info("Printing sample records from source table (first 5 rows):")
        sample_result = t_env.execute_sql(
            "SELECT ip, host, event_time, event_timestamp FROM events LIMIT 5"
        )
        sample_result.print()

        # Define the session gap.
        # For production, use "INTERVAL '5' MINUTE".
        # For testing, you may reduce it (e.g., to "INTERVAL '1' MINUTE") to force session closure.
        session_gap = "INTERVAL '1' MINUTE"
        # If necessary for testing:
        # session_gap = "INTERVAL '1' MINUTE"

        # Build the INSERT query.
        # We filter out records where event_timestamp is null to prevent errors in the sink operator.
        query = f"""
           INSERT INTO {sink_table}
           SELECT
             ip,
             host,
             SESSION_START(SESSION(event_timestamp, {session_gap})) AS session_start,
             SESSION_END(SESSION(event_timestamp, {session_gap})) AS session_end,
             COUNT(*) AS num_events
           FROM {source_table}
           WHERE event_timestamp IS NOT NULL
           GROUP BY
             ip,
             host,
             SESSION(event_timestamp, {session_gap})
        """
        logger.info("Sessionization Query:")
        logger.info(query)

        result = t_env.execute_sql(query)
        result.wait()
        logger.info("Sessionization job completed successfully.")
    except Exception as e:
        logger.error("Error in sessionization job:", exc_info=True)

if __name__ == '__main__':
    sessionize_events()
