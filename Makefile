create topic:
	docker exec -it kafka-0 kafka-topics.sh --create --topic test-topic --partitions 3 --replication-factor 3 --bootstrap-server localhost:9092
produce message:
	docker exec -it kafka-0 kafka-console-producer.sh --topic test-topic --bootstrap-server localhost:9092
consume message:
	docker exec -it kafka-1 kafka-console-consumer.sh --topic test-topic --from-beginning --bootstrap-server localhost:9092
describe topic:
	docker exec -it kafka-0 kafka-topics.sh --describe --topic test-topic --bootstrap-server localhost:9092
describe all topics:
	docker exec -it kafka-0 kafka-topics.sh --describe --bootstrap-server localhost:9092
