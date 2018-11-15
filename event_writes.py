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
        StructField("description", StringType(), True),
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

