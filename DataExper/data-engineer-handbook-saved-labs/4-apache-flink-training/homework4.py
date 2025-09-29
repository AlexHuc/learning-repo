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
      - event_time: string (ISO format)
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
    """
    table_name = "sessionized_events"
    ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            num_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        )
    """
    t_env.execute_sql(ddl)
    logger.info(f"Created sink table '{table_name}'.")
    return table_name

def sessionize_events():
    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        env.enable_checkpointing(10 * 1000)
        env.get_checkpoint_config().setCheckpointTimeout(60000)
        # Uncomment and configure a state backend if needed:
        # env.setStateBackend(RocksDBStateBackend("file:///path/to/checkpoint/dir", True))

        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        t_env = StreamTableEnvironment.create(env, environment_settings=settings)

        source_table = create_events_source(t_env)
        sink_table = create_sessions_sink(t_env)

        # Debug: Print a sample of the source data
        logger.info("Printing sample records from source table:")
        t_env.from_path(source_table).execute().print()

        # Define session gap as 5 minutes
        session_gap = "INTERVAL '5' MINUTE"
        query = f"""
           INSERT INTO {sink_table}
           SELECT
             ip,
             host,
             SESSION_START(SESSION(event_timestamp, {session_gap})) as session_start,
             SESSION_END(SESSION(event_timestamp, {session_gap})) as session_end,
             COUNT(*) as num_events
           FROM {source_table}
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
