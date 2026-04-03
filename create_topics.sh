#!/bin/bash

topics=(
pm10
pm25
temperature
humidity
pressure
no2
co
o3
wind_speed
rawSensorWeatherData
FullPm10WeatherData
FullPm25WeatherData
)

for topic in "${topics[@]}"
do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic "$topic" \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
done

echo "Topics created."