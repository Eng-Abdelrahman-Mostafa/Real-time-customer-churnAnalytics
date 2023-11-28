from confluent_kafka import Producer,Consumer, KafkaError
import json

class Kafka:
    topics = ['customer-info-topic']
    # Set up Kafka producer configuration
    producer_conf = {'bootstrap.servers': 'localhost:9092'}

    # Create Kafka producer instance
    producer = Producer(producer_conf)

    # Produce messages to a Kafka topic
    @staticmethod
    def produce_message(topic, message):
        Kafka.producer.produce(topic, value=json.dumps(message))
        Kafka.producer.flush()  # Flush to ensure the message is sent immediately

    # Close Kafka producer instance
    @staticmethod
    def close_producer():
        Kafka.producer.close()

    consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'latest'
    }
    # Create Kafka consumer instance
    consumer = Consumer(consumer_conf)
    # Subscribe to Kafka topics
    
    #Consume messages from Kafka topics
    @staticmethod
    def consume_message(topics):
        #if topics is None add default topic
        if topics is None:
            topics = Kafka.topics

        Kafka.consumer.subscribe(topics)
        # Poll for new messages
        try:
            while True:
                msg = Kafka.consumer.poll(1.0)  # Timeout in seconds

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event - not an error
                        continue
                    else:
                        print(msg.error())
                        break

                # Process the received message
                print(f"Received message: {msg.value().decode('utf-8')} from topic {msg.topic()} partition {msg.partition()} offset {msg.offset()}")

        except KeyboardInterrupt:
            pass

        finally:
            # Close down consumer to commit final offsets.
            Kafka.consumer.close()

