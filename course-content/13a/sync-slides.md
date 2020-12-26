---
title: Fundamentals of Data Engineering
author: Week 13 - sync session
...

---

#
## Get Started

We keep working in the same dir as last week:
```
cd  ~/w205/full-stack

```
But we still need to copy over a few files:
```
cp ~/w205/course-content/13a/docker-compose.yml .
docker-compose pull
cp ~/w205/course-content/13a/*.py .

```

::: notes
:::

#


## { data-background="images/pipeline-steel-thread-for-mobile-app.svg" } 

::: notes
Let's walk through this
- user interacts with mobile app
- mobile app makes API calls to web services
- API server handles requests:
    - handles actual business requirements (e.g., process purchase)
    - logs events to kafka
- spark then:
    - pulls events from kafka
    - filters/flattens/transforms events
    - writes them to storage
- presto then queries those events
:::


# 
## Flask-Kafka-Spark-Hadoop-Presto Part II
::: notes
- last week we did spark put together the whole pipeline including the connection to Presto
- still missing: streaming
:::

#
## Setup

## The `docker-compose.yml` 

Create a `docker-compose.yml` with the following
```
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

  cloudera:
    image: midsw205/hadoop:0.0.2
    hostname: cloudera
    expose:
      - "8020" # nn
      - "8888" # hue
      - "9083" # hive thrift
      - "10000" # hive jdbc
      - "50070" # nn http
      #ports:
      #- "8888:8888"

  spark:
    image: midsw205/spark-python:0.0.6
    stdin_open: true
    tty: true
    volumes:
      - ~/w205:/w205
    expose:
      - "8888"
    ports:
      - "8888:8888"
    depends_on:
      - cloudera
    environment:
      HADOOP_NAMENODE: cloudera
      HIVE_THRIFTSERVER: cloudera:9083
    command: bash

  presto:
    image: midsw205/presto:0.0.1
    hostname: presto
    volumes:
      - ~/w205:/w205
    expose:
      - "8080"
    environment:
      HIVE_THRIFTSERVER: cloudera:9083

  mids:
    image: midsw205/base:0.1.9
    stdin_open: true
    tty: true
    volumes:
      - ~/w205:/w205
    expose:
      - "5000"
    ports:
      - "5000:5000"
```

::: notes
k, z same
- cloudera a little thinner, a few extra environment variables
- presto new conainer
:::

## Spin up the cluster

```
docker-compose up -d
```

::: notes
- Now spin up the cluster
```
docker-compose up -d
```
- Notice we didn't actually create a topic as the broker does this for you
:::

## Web-app

- Take our instrumented web-app from before
`~/w205/full-stack2/game_api.py`

```python
#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"


@app.route("/purchase_a_sword")
def purchase_a_sword():
    purchase_sword_event = {'event_type': 'purchase_sword'}
    log_to_kafka('events', purchase_sword_event)
    return "Sword Purchased!\n"

@app.route("/purchase_armor")
def purchase_armor():
    purchase_armor_event = {'event_type': 'purchase_armor'}
    log_to_kafka('events', purchase_armor_event)
    return "Armor Purchased!\n"

```

::: notes
- same web app as before plus one extra event
:::

## run flask
```
docker-compose exec mids \
  env FLASK_APP=/w205/full-stack/game_api.py \
  flask run --host 0.0.0.0
```

::: notes

```
docker-compose exec mids env FLASK_APP=/w205/full-stack/game_api.py flask run --host 0.0.0.0
```

:::

## Set up to watch kafka

```
docker-compose exec mids \
  kafkacat -C -b kafka:29092 -t events -o beginning
```


::: notes
- new terminal window, leave up
- running kafkacat without -e so it will run continuously

```
docker-compose exec mids kafkacat -C -b kafka:29092 -t events -o beginning
```
:::



# 
## Streaming

```python
#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType


def purchase_sword_event_schema():
    """
    root
    |-- Accept: string (nullable = true)
    |-- Host: string (nullable = true)
    |-- User-Agent: string (nullable = true)
    |-- event_type: string (nullable = true)
    |-- timestamp: string (nullable = true)
    """
    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
    ])


@udf('boolean')
def is_sword_purchase(event_as_json):
    """udf for filtering events
    """
    event = json.loads(event_as_json)
    if event['event_type'] == 'purchase_sword':
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
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .load()

    sword_purchases = raw_events \
        .filter(is_sword_purchase(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          purchase_sword_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')

    sink = sword_purchases \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_sword_purchases") \
        .option("path", "/tmp/sword_purchases") \
        .trigger(processingTime="10 seconds") \
        .start()

    sink.awaitTermination()


if __name__ == "__main__":
    main()
```

::: notes
- specify purchase_sword_event_schema schema in the job (before were inferring schema - doing that in the '.rdd \ .map(etc) \ .toDF()' this is fine but fairly brittle. In a production pipeline you want to explictly specify the schema.)
- rename is_purchase so specific to swords
- Combining a bunch of steps from before
- Not going to the rdd
- Start with raw events, filter whether they're sword purchases, 
- Doing a lot (both the select and the filter we did in 2 steps before) in that one step for filtering sword purchases
- will have a column in the output of keep the raw event, can always go back and reexamine
- The from json has two pieces - the actual column and the schema
- select 3 columns: value, timestamp, the json created
- Best practices
- Robust against different schema, but that can be a gotcha if you end up with data that isn't formatted how you thought it was
- Streaming datasets don't allow you to access the rdd as we did before
- This isn't writing the hive table for us
- `raw_events = spark.readStream ...` instead of just read
- In batch one, we tell it offsets, and tell it to read
- In streaming, no offsets
- `query =` section & `query.awaitTermination` sections are differences
:::

## Run it

```
docker-compose exec spark spark-submit /w205/full-stack/write_swords_stream.py
```

## Feed it

```bash
while true; do
  docker-compose exec mids \
    ab -n 10 -H "Host: user1.comcast.com" \
      http://localhost:5000/purchase_a_sword
  sleep 10
done
```
::: notes

```
while true; do docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/purchase_a_sword; sleep 10; done
```
:::

## Check what it wrote to Hadoop

```
docker-compose exec cloudera hadoop fs -ls /tmp/sword_purchases
```
::: notes
:::

# 

## Register the table(s) with hive

## Deprecated Way


```
docker-compose exec cloudera hive
```


::: notes
- Run hive in the hadoop container using the hive command line
- This is what you would do, don't need to actually do it, skip to easier way
- This is deprecated at this point
:::

## 

```sql
create external table if not exists default.purchases2 (
    Accept string,
    Host string,
    User_Agent string,
    event_type string,
    timestamp string
  )
  stored as parquet 
  location '/tmp/purchases'
  tblproperties ("parquet.compress"="SNAPPY");
```

::: notes
```
create external table if not exists default.purchases2 (Accept string, Host string, User_Agent string, event_type string, timestamp string) stored as parquet location '/tmp/purchases'  tblproperties ("parquet.compress"="SNAPPY");
```
:::

#
## or include the code with your spark file

```python

#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType


def purchase_sword_event_schema():
    """
    root
    |-- Accept: string (nullable = true)
    |-- Host: string (nullable = true)
    |-- User-Agent: string (nullable = true)
    |-- event_type: string (nullable = true)
    """
    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
    ])


@udf('boolean')
def is_sword_purchase(event_as_json):
    """udf for filtering events
    """
    event = json.loads(event_as_json)
    if event['event_type'] == 'purchase_sword':
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
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .load()

    sword_purchases = raw_events \
        .filter(is_sword_purchase(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          purchase_sword_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')

    spark.sql("drop table if exists default.events")
    sql_string = """
        create external table if not exists default.events (
            raw_event string,
            timestamp string,
            Accept string,
            Host string,
            `User-Agent` string,
            event_type string
            )
            stored as parquet
            location '/tmp/events'
            tblproperties ("parquet.compress"="SNAPPY")
            """
    spark.sql(sql_string)

    sink = sword_purchases \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_sword_purchases") \
        .option("path", "/tmp/sword_purchases") \
        .trigger(processingTime="10 seconds") \
        .start()

    sink.awaitTermination()


if __name__ == "__main__":
    main()
```
## Run it

```
docker-compose exec spark spark-submit /w205/full-stack/stream_and_hive.py
```

#
## Read it with Presto

```
docker-compose exec presto presto --server presto:8080 --catalog hive --schema default
```
## What tables do we have in Presto?

```
presto:default> show tables;
```

## Describe `sword_purchases` table

```
presto:default> describe sword_purchases;
```

## Query `purchases` table

```
presto:default> select * from sword_purchases;
```
## Query `purchases` table

```
presto:default> select * from sword_purchases;
```

::: notes
wait a stream cycle for data to be ingested
:::

# 
## down

    docker-compose down

::: notes
:::



#
## summary

## { data-background="images/pipeline-steel-thread-for-mobile-app.svg" } 




#

<img class="logo" src="images/berkeley-school-of-information-logo.png"/>

