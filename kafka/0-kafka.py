from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(bootstrap_servers="localhost:8080", client_id='test')

# Create a topic list
topic_list = []

new_topic = NewTopic(name="bankbranch", num_partitions=2, replication_factor=1)
topic_list.append(new_topic)

admin.create_topics(new_topics=topic_list, validate_only=False)