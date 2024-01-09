# Importing the necessary modules
from kafka import KafkaProducer
import json

# Creating a KafkaProducer instance with a custom value serializer
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Sending the first message to the "bankbranch" topic
producer.send("bankbranch", {'atmid':1, 'transid':100})

# Sending the second message to the "bankbranch" topic
producer.send("bankbranch", {'atmid':2, 'transid':101})

# Flushing any pending messages to ensure they are sent
producer.flush()

# Closing the KafkaProducer instance
producer.close()