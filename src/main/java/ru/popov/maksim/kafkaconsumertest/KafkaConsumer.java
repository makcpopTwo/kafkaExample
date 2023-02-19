package ru.popov.maksim.kafkaconsumertest;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumer implements Runnable{

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private final String topic;
    private final Consumer<String, String> consumer;


    public KafkaConsumer(String topic, Consumer<String, String> consumer) {
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
        }  catch (TopicAuthorizationException e) {
            log.error("Reading from topic not authorized: topic={}", e.unauthorizedTopics());
            throw e;
        } catch (Exception e) {
            log.error("error", e);
        } finally {
            consumer.close();
        }
    }

    private void tryToConsume() {
        ConsumerRecords<String, String> records = null;
        try {
            log.info("start polling");
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
//        } catch (TopicAuthorizationException e) {
//            log.error("Reading from topic not authorized: topic={}", e.unauthorizedTopics());
//            throw e;
        } catch (RuntimeException e) {
            log.warn("error on polling records from topic: topic={}", topic, e);
            waitForTimeout(Duration.ofSeconds(10));
        }
    }

    private void waitForTimeout(Duration timeoutDuration) {
        try {
            Thread.sleep(timeoutDuration.toMillis());
        } catch (InterruptedException e) {
            log.error("unexpectedly interrupted", e);
            Thread.currentThread().interrupt();
        }
    }


}
