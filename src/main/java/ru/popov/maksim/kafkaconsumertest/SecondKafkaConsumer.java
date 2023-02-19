package ru.popov.maksim.kafkaconsumertest;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

public class SecondKafkaConsumer implements Runnable{

    private static final Logger log = LoggerFactory.getLogger(SecondKafkaConsumer.class);

    private final String topic;
    private final Consumer<String, String> consumer;


    public SecondKafkaConsumer(String topic, Consumer<String, String> consumer) {
        this.topic = topic;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        log.info("Starting Kafka consumer: {}", topic);
        try {
            consumer.subscribe(Collections.singleton(topic));
            while (true) {
                tryToConsume();
            }
        } catch (Exception e) {
            log.error("error", e);
        } finally {
            consumer.close();
        }
    }

    private void tryToConsume() {
        ConsumerRecords<String, String> records = null;
        try {
            records = consumer.poll(Duration.ofMillis(1000));
            if (records.count() == 0) {
                log.debug("No records: topic={}", topic);
            } else {
                log.debug("polled records: topic={}, count={}", topic, records.count());

                for (ConsumerRecord<String, String> record : records) {
                    log.info("received message: {}", record.value());
                }
                consumer.commitSync();
                log.info("Finished: topic={}", topic);
            }
        } catch (WakeupException e) {
            log.info("WakeupException caught, ignore.");
        } catch (RuntimeException e) {
                log.warn("error on polling records from topic: topic={}", topic, e);
        }
    }


}
