# Kafka

Configure kafka with the following commands

```
./kafka-topics --create --bootstrap-server localhost:9092  --topic topicA
./kafka-topics --create --bootstrap-server localhost:9092  --topic topicB

./kafka-acls.sh --bootstrap-server=127.0.0.1:9092 --add --allow-principal User:'user_principal'  --operation READ --operation DESCRIBE --topic topicA
```

# Service

Control turning on/off of consumers with properties.

# Reproducing steps
- run kafka and configure accotding to the steps before
- build service
- run the first service with the following configuration
```
turnOnFirstConsumer=false
turnOnSecondConsumer=true
```
- run the second service with the following configuration
```
turnOnFirstConsumer=true
turnOnSecondConsumer=false
```
- wait for UnathorizedException in the second service (about 1 minute)
- check logs in the first service
