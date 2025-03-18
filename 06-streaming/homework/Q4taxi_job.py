from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment


def create_taxi_events_sink_postgres(t_env):
    table_name = 'taxi_events'
    sink_ddl = f"""
        CREATE OR REPLACE TABLE {table_name} (            
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),            
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance NUMERIC,            
            tip_amount NUMERIC           
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


def create_events_source_kafka(t_env):
    table_name = "taxi_events_source"
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        CREATE OR REPLACE TABLE {table_name} (
            VendorID VARCHAR,
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            store_and_fwd_flag VARCHAR,
            RatecodeID VARCHAR ,
            PULocationID VARCHAR,
            DOLocationID VARCHAR,
            passenger_count VARCHAR,
            trip_distance VARCHAR,
            fare_amount VARCHAR,
            extra VARCHAR,
            mta_tax VARCHAR,
            tip_amount VARCHAR,
            tolls_amount VARCHAR,
            ehail_fee VARCHAR,
            improvement_surcharge VARCHAR,
            total_amount VARCHAR,
            payment_type VARCHAR,
            trip_type VARCHAR,
            congestion_surcharge VARCHAR,
            pickup_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, '{pattern}'),
            WATERMARK FOR pickup_timestamp AS pickup_timestamp - INTERVAL '15' SECOND
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

def log_processing():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    # env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_taxi_events_sink_postgres(t_env)
        # write records to postgres too!
        t_env.execute_sql(
            f"""
                    INSERT INTO {postgres_sink}
                    SELECT
                        CAST(lpep_pickup_datetime AS TIMESTAMP(3)) AS lpep_pickup_datetime,
                        CAST(lpep_dropoff_datetime AS TIMESTAMP(3)) AS lpep_dropoff_datetime,
                        CAST(PULocationID AS INTEGER) AS PULocationID,
                        CAST(DOLocationID AS INTEGER) AS DOLocationID,
                        CAST(passenger_count AS INTEGER) AS passenger_count,
                        CAST(trip_distance AS NUMERIC) AS trip_distance,
                        CAST(total_amount AS NUMERIC) AS total_amount    
                    FROM {source_table}
                    """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
