from kafka import KafkaConsumer

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic = 'FullPm10WeatherData_ZeroShot' # FullPm10WeatherData

# Create a Kafka consumer
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',  # Start reading from the beginning if no offset is stored
                         enable_auto_commit=False)  # Disable auto-commit to manually control offsets
print("Consuming messages")

try:
    for message in consumer:
        print(f"Received message: {message.value.decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    # Close the consumer
    consumer.close()
