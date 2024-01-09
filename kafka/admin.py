from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')
topic_list = []

newTopic = NewTopic(name="bankbranch", num_partitions=2, replication_factor=1)
topic_list.append(newTopic)

admin_client.create_topics(new_topics=topic_list, validate_only=False)
