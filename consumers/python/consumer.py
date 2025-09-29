from kafka import KafkaConsumer
consumer = KafkaConsumer('topico-teste', bootstrap_servers='localhost:29092', auto_offset_reset='earliest', group_id='grupo-teste')
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")