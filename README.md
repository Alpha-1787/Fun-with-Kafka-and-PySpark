# Project
you are a data scientist at a game development company. Your latest mobile game has two events you are intereted in tracking: "buy a sword" and 'join guild'. Each event have metadata associated with it. 

You want to instrument your API server to catch and analyze these two event types.

# 1. Setup

## 1.1. Create Directory
Create new directories to hold all the files that are being used for the project.

```
mkdir ~/w205/project3_fullstack
cd ~/w205/project3_fullstack
```

## 1.2. Create .yml File
Create the docker-compose.yml file that will be used to setup the cluster.

```
vi docker-compose.yml
---
version: '2'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 32181
      ZOOKEEPER_TICK_TIME: 2000
    expose:
      - "2181"
      - "2888"
      - "32181"
      - "3888"
    extra_hosts:
      - "moby:127.0.0.1"

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:32181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    expose:
      - "9092"
      - "29092"
    extra_hosts:
      - "moby:127.0.0.1"

  cloudera:
    image: midsw205/cdh-minimal:latest
    expose:
      - "8020" # nn
      - "50070" # nn http
      - "8888" # hue
    #ports:
    #- "8888:8888"
    extra_hosts:
      - "moby:127.0.0.1"

  spark:
    image: midsw205/spark-python:0.0.5
    stdin_open: true
    tty: true
    volumes:
      - /home/science/w205:/w205
    expose:
      - "8888"
    ports:
      - "8888:8888"
    depends_on:
      - cloudera
    environment:
      HADOOP_NAMENODE: cloudera
    extra_hosts:
      - "moby:127.0.0.1"
    command: bash

  mids:
    image: midsw205/base:0.1.9
    stdin_open: true
    tty: true
    volumes:
      - /home/science/w205:/w205
    expose:
      - "5000"
    ports:
      - "5000:5000"
    extra_hosts:
      - "moby:127.0.0.1"

```

## 1.3. Start Cluster
Start up the docker cluster.

```
docker-compose up -d
```
## 1.4. Create Kafka Topic
Create kafka topic that will be  used for the messages being generated via API calls to flask.

```
docker-compose exec kafka \
  kafka-topics \
    --create \
    --topic events \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists --zookeeper zookeeper:32181
```

## 1.5. Create an API using Flask
this will return the events in a .json format that will provide the event type and the event time stamp in UTC unix epoch. In addition the events will be sent to a kafka producer prior to being consumed.

```
vi game_api.py
```
```python
#game_api.py file
#!/usr/bin/env python
import json
import time
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    time_stamp = time.time()
    default_event = {'event_type': 'default', 'event_ts': time_stamp}
    log_to_kafka('events', default_event)
    return "\nThis is the default response!\n"


@app.route("/purchase_a_sword")
def purchase_a_sword():
    purchase_sword_event = {'event_type': 'purchase_sword'}
    log_to_kafka('events', purchase_sword_event)
    return "Sword Purchased!\n"


@app.route("/purchase_a_shield")
def purchase_a_shield():
    time_stamp = time.time()
    purchase_shield_event = {'event_type': 'purchase_shield', 'event_ts': time_stamp}
    log_to_kafka('events', purchase_shield_event)
    return "\nShield Purchased!\n"

@app.route("/purchase_a_spear")
def purchase_a_spear():
    time_stamp = time.time()
    purchase_spear_event = {'event_type': 'purchase_spear', 'event_ts': time_stamp}
    log_to_kafka('events', purchase_spear_event)
    return "\nSpear Purchased!\n"

@app.route("/purchase_a_potion")
def purchase_a_potion():
    time_stamp = time.time()
    purchase_potion_event = {'event_type': 'purchase_potion', 'event_ts': time_stamp, 'description': 'heals you for 50 hp'}
    log_to_kafka('events', purchase_potion_event)
    return "\nPotion Purchased"
```

## 1.6. Start Flask
Start flask so that messages can be generated.

```
docker-compose exec mids \
  env FLASK_APP=/w205/project3_fullstack/game_api.py \
  flask run --host 0.0.0.0
```

## 1.7. Run kafkacat
Run kafkacat in continuous modes so that we can get the events as they come through.
```
docker-compose exec mids kafkacat -C -b kafka:29092 -t events -o beginning
```
# 2. Generating Messages

## 2.1. Generate Messages
Use apache bench
```
docker-compose exec mids \
  ab \
    -n 10 \
    -H "Host: user1.comcast.com" \
    http://localhost:5000/

docker-compose exec mids \
  ab \
    -n 10 \
    -H "Host: user1.comcast.com" \
    http://localhost:5000/purchase_a_sword

docker-compose exec mids \
  ab \
    -n 10 \
    -H "Host: user2.att.com" \
    http://localhost:5000/purchase_a_spear

docker-compose exec mids \
  ab \
    -n 10 \
    -H "Host: user2.att.com" \
    http://localhost:5000/purchase_a_shield

docker-compose exec mids \
  ab \
    -n 10 \
    -H "Host: user3.att.com" \
    http://localhost:5000/purchase_a_potion

```

# 3. Spark

## 3.1. Spark Submit
Instead of transforming the data from jupyter notebook, a .py file will be used to do a spark submit job, which will create a parquet file in hdfs. Note that we have two different schemas that will be imposed based on the different event types.

```python
#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType

def purchase_equipment_event_schema():
    """
    root
    |-- Accept: string (nullable = true)
    |-- Host: string (nullable = true)
    |-- User-Agent: string (nullable = true)
    |-- event_ts: string (nullable = true)
    |-- event_type: string (nullable = true)
    |-- timestamp: string (nullable = true)
    """
    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("event_type", StringType(), True)
    ])

def purchase_consumable_event_schema():
    """
    root
    |-- Accept: string (nullable = true)
    |-- Host: string (nullable = true)
    |-- User-Agent: string (nullable = true)
    |-- description: string (nullable = true)
    |-- event_ts: string (nullable = true)
    |-- event_type: string (nullable = true)
    |-- timestamp: string (nullable = true)
    """
    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        Structfield("description", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("event_type", StringType(), True)
    ])


@udf('boolean')
def is_equipment_purchase(event_as_json):
    """filters sword,  shield and spear purchases
    """
    event = json.loads(event_as_json)
    if event['event_type'] in ['purchase_spear', 'purchase_shield', 'purchase_sword']:
        return True
    return False

@udf('boolean')
def is_consumable_purchase(event_as_json):
    """filters potion purchases
    """
    event = json.loads(event_as_json)
    if event['event_type'] in ['purchase_potion']:
        return True
    return False


def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .getOrCreate()

    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    purchase_equipment_events = raw_events \
        .filter(is_equipment_purchase(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          purchase_equipment_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')

    purchase_equipment_events.printSchema()
    purchase_equipment_events.show()

    purchase_equipment_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/equipment_purchases')

    purchase_consumable_events = raw_events \
        .filter(is_consumable_purchase(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
               raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          purchase_consumable_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')

    purchase_consumable_events.printSchema()
    purchase_consumable_events.show()

    purchase_consumable_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/consumable_purchases')

if __name__ == "__main__":
    main()

```

## 3.2. Run Spark Submit
exec into the spark container and run the event_writes.py 
```
docker-compose exec spark spark-submit /w205/project3_fullstack/event_writes.py
```

## 3.3. Check The Finished Job
Check to see if the desired files are there.
```
docker-compose exec cloudera hadoop fs -ls /tmp/
```

## 3.4.  Open Jupyter Notebook
Opening up jupyter notebook for the spark container.
```
docker-compose exec spark \
  env \
    PYSPARK_DRIVER_PYTHON=jupyter \
    PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port 8888 --ip 0.0.0.0 --allow-root' \
  pyspark
```

## 3.5. Create symbolic link
Creating symlink so that the jupyter notebook can be saved outside of the spark container
```
docker-compose exec spark bash -c "ln -s /w205/project3_fullstack"
```
## 3.6. Reading HDFS
See "data_exploration.ipynb"

# 4. Wind Down

## 4.1. ctrl-c
ctrl-c out of everything (i.e. jupyter notebook and flask, kafkacat).

## 4.2. Shut Down Cluster

```
docker-compose down
```

## 4.3. Check Droplet
Check that there are no more containers running.

```
docker ps -a
```

## 4.4. Enhancements Done
1. added new metadatas to the json.
2. added two separate schemas to be used with the spark submit .py file.

