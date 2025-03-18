from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_events_aggregated_sink(t_env):
    table_name = 'taxi_events_aggregated'
    sink_ddl = f"""
        CREATE OR REPLACE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_streak BIGINT NOT NULL,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            PRIMARY KEY (PULocationID, DOLocationID) NOT ENFORCED  
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

# Define Kafka Source(green_taxi_events)
# The table reads real-time trip events from a Kafka topic.
def create_events_source_kafka(t_env):
    table_name = "green_taxi_events"
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        CREATE OR REPLACE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,            
            PULocationID VARCHAR,
            DOLocationID VARCHAR,
            passenger_count VARCHAR,
            trip_distance VARCHAR,            
            tip_amount VARCHAR,
            dropoff_timestamp AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'), 
            WATERMARK for dropoff_timestamp as dropoff_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            # This lambda is your timestamp assigner:
            #   event -> The data record
            #   timestamp -> The previously assigned (or default) timestamp
            lambda event, timestamp: event[2]  # We treat the second tuple element as the event-time (ms).
        )
    )
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        aggregation_ddl = f"""
            INSERT INTO {aggregated_table}
            SELECT
                CAST(PULocationID AS INTEGER) AS PULocationID,
                CAST(DOLocationID AS INTEGER) AS DOLocationID,
                COUNT(*) AS trip_streak,
                SESSION_START(dropoff_timestamp, INTERVAL '5' MINUTE) AS session_start, 
                SESSION_END(dropoff_timestamp, INTERVAL '5' MINUTE) AS session_end
            FROM {source_table}                      
            GROUP BY PULocationID, DOLocationID, SESSION(dropoff_timestamp, INTERVAL '5' MINUTE);
        """

        t_env.execute_sql(aggregation_ddl).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()
