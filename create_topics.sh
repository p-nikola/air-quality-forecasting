#!/bin/bash

for topic in FullPm10WeatherData FullPm25WeatherData
do
  
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1
done

awk -F',' 'NR>1 {print $2}' data/raw/bitola_sensor_weather_features_online.csv | sort -u | while read id
do
  topic="sensor_$id"

  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1

done

echo "Topics created"