# TABLE of CONTENT
- [Command Notes](#command-notes)
- [Homework](#homework)
- [Other Study Notes](#other-study-notes)
- 

## Command Notes

``` $ cd 06-streaming/pyflink ```
1. Add pgAdmin service into docker-compose.yml file.  And I run this without installing make.
2. Build the Docker image and deploy the services in the docker-compose.yml file \
``` $ docker compose up --build --remove-orphans  -d ``` \
After the image is built, Docker will automatically start up the job manager and task manager services.

Flink UI: http://localhost:8081/   to see the Flink Job Manager  \
Connect to Postgres with pgcli, pg-admin, DataGrip(install,not-free), DBeaver or any other tool.

The connection credentials are: \
Username postgres
Password postgres
Database postgres
Host localhost
Port 5432

With pgcli, you'll need to run this to connect: \
```bash
    $ pip install pgcli
    $ pgcli -h localhost -p 5432 -u postgres -d postgres
```
OR With pgAdmin, localhost:8080   email:admin@admin.com, password:root.


3. In pgAdmin，Run these query to create the Postgres landing zone for the first events and windows:
```
CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);

CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP,
    test_data INTEGER,
    num_hits INTEGER 
);

SELECT COUNT(*) FROM processed_events;
```
4. Send data(topic/messages) to Kafka
```bash     $ python src/producers/producer.py  ```

5. Run the PyFlink job!  
```bash    $ docker-compose exec jobmanager ./bin/flink run -py ../src/job/start_job.py -d ```

```bash    $ python src/producers/producer.py ``` \
Check table row numbers in Postgres: ``` SELECT COUNT(*) FROM processed_events; ```  (row number raising)

Flink UI: http://localhost:8081/    Check Chekpoints.

__(All the above is talking about how to run insert into, select, start, with Flink)__

6. groupby --src/job/aggregation.py

7. Common Commands
```
bash
$ docker compose up --build --remove-orphans  -d       ## Builds the base Docker image and starts Flink cluster
$ docker compose stop     ## Stops all services in Docker compose      
$ docker compose start    ## Starts all services in Docker compose
$ docker compose down --remove-orphans    ## Shuts down the Flink cluster
## Submit the Flink job
$ docker compose exec jobmanager ./bin/flink run -py ../src/job/start_job.py -d
$ docker compose exec jobmanager ./bin/flink run -py ../src/job/start_job.py --pyFiles ../src -d
## Submit the Flink job-aggregation_job
$ docker compose exec jobmanager ./bin/flink run -py /opt/src/job/aggregation_job.py --pyFiles /opt/src -d
```	



## Homework
Q1: Redpanda version
```
bash
$ docker compose exec redpanda-1 bash
$ rpk help
$ redpanda --verison
```
Q2: Creating a topic
```
bash: after $ docker compose up --build --remove-orphans  -d 
$ docker compose exec redpanda-1 bash
$ rpk help
$ rpk topic create --help 
$ rpk topic create green-trips
```
Q3: Connecting to the Kafka server
- install the kafka connector, so that we can connect to the server, and later we can send some data to its topics.
``` $ pip install kafka-python ```

- jupyter notebook or a script to connect to our server:
```
import json
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)
producer.bootstrap_connected()
```
Q4: Sending the Trip Data
Q4SendGreenTrips.py   Q4taxi_job.py
- pgAdmin：Create Table taxi_events
  ```
  CREATE TABLE taxi_events (
	lpep_pickup_datetime TIMESTAMP(3),
	lpep_dropoff_datetime TIMESTAMP(3), 
	PULocationID INTEGER,
	DOLocationID INTEGER,
	passenger_count INTEGER,
	trip_distance NUMERIC,
	tip_amount NUMERIC
  );
  ```  
- Run this PyFlink job!
  ``` $ docker compose exec jobmanager ./bin/flink run -py ../src/Q4taxi_job.py --pyFiles ../src -d ```
- Send data(topic/messages) to Kafka
  ``` $ python src/Q4SendGreenTrips.py ```

Q5:Build a Sessionization Window (2 points)  --> I failed! Wrote 0 data into sink table.
- "Which pickup and drop off locations have the longest unbroken streak of taxi trips?"   --see more details in StudyNote.doc

- An "unbroken streak of taxi trips" means a continuous series of taxi rides without interruption.


- pgAdmin run:
  ```
  CREATE TABLE taxi_events_aggregated(
  	PULocationID INTEGER,
  	DOLocationID INTEGER,
  	trip_streak INTEGER,
  	session_start TIMESTAMP(3),
  	session_end TIMESTAMP(3),
  	PRIMARY KEY (PULocationID, DOLocationID)  
  );           
  ```
- Run this PyFlink job!
  ``` $ docker compose exec jobmanager ./bin/flink run -py ../src/Q5session_job.py --pyFiles ../src -d   ```
- Send data(topic/messages) to Kafka
  ``` $ python src/Q4SendGreenTrips.py ```



## Other Study Notes
1.	What is Watermarker in flink (see code in xxx_job.py)？
A watermark in Flink is a mechanism that tracks event time progress and handles late-arriving data in stream processing.          Watermark tells Flink that no events with timestamps earlier than the watermark will arrive.
- Why Do We Need Watermarks?     ->Use case: Handles late-arriving data in stream processing.
In real-time streaming, events may arrive late due to network delays or out-of-order events. Watermarks help Flink decide when to trigger computations (like windows) while handling late data correctly.
- "Flink DROPS it" means Flink ignores or discards events that arrive after the watermark has advanced beyond the event's timestamp.
- Watermarks help Flink track event-time processing and avoid issues with late-arriving data that could corrupt computations.

- Example with Watermark:
	- Consider the following scenario with taxi trip events and a 5-second watermark:\
  	  Incoming Events:\
  	  •	Event 1: pickup_time = 10:00:00\
  	  •	Event 2: pickup_time = 10:01:00\
  	  •	Event 3: pickup_time = 10:05:00\
  	  •	Event 4: pickup_time = 10:02:00 (late event), but arrives at 10:06:00\
  	  •	Event 5: pickup_time = 10:10:00\
	- Watermark Progression:\
	Wall Clock Time	Event Processed	Watermark\
|10:00:00	|Event 1 (10:00)	|09:59:55|
|10:01:00	|Event 2 (10:01)	|10:00:55|
|10:05:00	|Event 3 (10:05)	|10:04:55|
|10:06:00	|Event 4 (10:02)	|Dropped|
|10:10:00	|Event 5 (10:10)	|10:09:55|
•	Watermark at 10:05: By the time Event 4 (with pickup_time = 10:02) arrives at 10:06, the watermark has already passed 10:05, so Event 4 is considered too late.\
•	Flink drops Event 4 and will not process it in real-time streams.\
Why Does Flink Drop Late Events?\
•	Windowing and Aggregation: Flink uses watermarks to determine when to trigger window-based aggregations. If an event is too late, it might lead to incorrect window calculations or out-of-order processing. To ensure correct results, late events are ignored or discarded.\
•	Event-Time Semantics: Flink processes events in event-time (not processing-time), and if the watermark indicates that no earlier events are expected, any late arrivals may not fit into the appropriate processing window.\
Handling Late Data:\
•	While Flink drops late events by default, you can configure it to side-output late events to handle them separately. There are also options like allowed lateness to allow a certain window to wait for late arrivals before processing the result.\

•	For example: \
```
	java
	Copy code
	windowedStream
		allowedLateness(Time.seconds(10))  // Allow events to arrive 10 seconds after the window closes
		sideOutputLateData(lateDataOutputTag); // Send late events to a side output
```

__When use Watermarker FOR lpep_pickup_datetime  in source_table, This tells Flink to treat lpep_pickup_datetime as an event-time column.__\
__Ensure lpep_pickup_datetime has a watermark defined before using it in SESSION()!!! Why?  Flink’s session windowing operates on event time, which means it needs a time attribute (ROWTIME). However, your column lpep_pickup_datetime is just a regular TIMESTAMP(3), and Flink does not automatically treat it as event time.    SESSION() windows only work with event time attributes, not regular TIMESTAMP(3).__\

```
source_ddl = f"""
        CREATE OR REPLACE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,            
            PULocationID VARCHAR,
            DOLocationID VARCHAR,
            passenger_count VARCHAR,
            trip_distance VARCHAR,            
            tip_amount VARCHAR, 
            -- Use lpep_dropoff_datetime time as watermark with a 5 second tolerance
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
```



2.	 session window 
What is session window?

“Use a session window with a gap of 5 minutes”

Flink SQL window function examples: Session\
https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/window-agg/

```
-- Flink SQL
	INSERT INTO {aggregated_table}
        SELECT
            CAST(PULocationID AS INTEGER) AS PULocationID,
            CAST(DOLocationID AS INTEGER) AS DOLocationID, 
			-- Use a SESSION Window (5-minute gap) to group trips based on pickup_time.
            -- A new session starts if there's a gap of more than 5 minutes between trips.            
            -- Continuous trips (within 5 minutes) are counted as part of the same session.
            COUNT(*) AS trip_streak,
            -- SESSION_START() and SESSION_END() functions are used calculate the start and
            --end times of each session based on the lpep_pickup_datetime.
            SESSION_START(lpep_pickup_datetime, INTERVAL '5' MINUTE) AS session_start, 
            SESSION_END(lpep_pickup_datetime, INTERVAL '5' MINUTE) AS session_end
        FROM {source_table}
        -- SESSION() function: Create a session window. 
        GROUP BY PULocationID, DOLocationID, SESSION(dropoff_timestamp, INTERVAL '5' MINUTE);        
```


3.	Study notes for homework Q5: I failed. wrote 0 data into postgres table.
"Which pickup and drop off locations have the longest unbroken streak of taxi trips?"   is asking for pairs of locations where taxis have continuously picked up and dropped off passengers without long interruptions.\
- e.g. Most Consecutive Trips Between Two Locations:\
•	Identifying pickup and drop-off location pairs that have seen the longest continuous sequence of taxi rides.\
•	Example: If taxis keep running between JFK Airport and Times Square without stopping for long gaps, that route would have a long unbroken streak.\
- e.g. Minimal Time Gaps Between Trips:\
•	If one taxi drops off a passenger at Location A and another taxi quickly picks up a new passenger from the same location, it extends the streak.\
•	Example: A busy airport or train station where taxis keep coming and going without idle periods.\

An "unbroken streak of taxi trips" means a continuous series of taxi rides without interruption.\
- e.g. A driver completing 100 consecutive trips without taking a long break.\
- e.g. A customer taking daily taxi rides for a certain number of days without skipping.\







