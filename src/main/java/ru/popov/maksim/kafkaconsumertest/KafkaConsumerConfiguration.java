package ru.popov.maksim.kafkaconsumertest;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Configuration
public class KafkaConsumerConfiguration {

    @Bean
    @ConditionalOnProperty(name = "turnOnFirstConsumer", havingValue = "true")
    public KafkaConsumer firstConsumer() {
        KafkaConsumer kafkaConsumer = new KafkaConsumer("topicA",
                createApacheKafkaConsumer("topicA", "localhost:9092"));
        new Thread(kafkaConsumer).start();
        return kafkaConsumer;
    }

    @Bean
    @ConditionalOnProperty(name = "turnOnSecondConsumer", havingValue = "true")
    public SecondKafkaConsumer secondConsumer() {
        SecondKafkaConsumer kafkaConsumer = new SecondKafkaConsumer("topicB",
                createApacheKafkaConsumer("topicB", "localhost:9092"));
        new Thread(kafkaConsumer).start();
        return kafkaConsumer;
    }

    private static org.apache.kafka.clients.consumer.KafkaConsumer<String, String> createApacheKafkaConsumer(String kafkaTopic,
                                                                                                             String url) {
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(
                asMap(kafkaTopic, url),
                new StringDeserializer(),
                new StringDeserializer());
    }

    public static Map<String, Object> asMap(String topicName, String url) {
        requireNonNull(topicName, "topicName");

        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, url);

        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, " ");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_app_2");
        props.put("session.timeout.ms", (int) Duration.ofSeconds(60).toMillis());
        props.put("heartbeat.interval.ms", (int) Duration.ofSeconds(10).toMillis());
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");

        props.put("fetch.min.bytes", 1);
        props.put("max.partition.fetch.bytes", 1048576);
        props.put("connections.max.idle.ms", 540000L);
        props.put("default.api.timeout.ms", 60000);
        props.put("fetch.max.bytes", 52428800);
        props.put("max.poll.interval.ms", 300000);
        props.put("max.poll.records", 500);
        props.put("partition.assignment.strategy", List.of(RangeAssignor.class));
        props.put("receive.buffer.bytes", 65536);
        props.put("request.timeout.ms", 30000);
        props.put("send.buffer.bytes", 131072);
        props.put("check.crcs", true);
        props.put("fetch.max.wait.ms", 500);
        props.put("metadata.max.age.ms", 300000L);
        props.put("reconnect.backoff.max.ms", 1000L);
        props.put("reconnect.backoff.ms", 50L);
        props.put("retry.backoff.ms", 100);

        props.put("ssl.enabled.protocols", List.of("TLSv1.3", "TLSv1.2", "TLSv1.1", "TLSv1"));
        props.put("ssl.keystore.type", "JKS");
        props.put("ssl.protocol", "TLS");
        props.put("ssl.provider", null);
        props.put("ssl.truststore.type", "JKS");
        props.put("ssl.keymanager.algorithm", "SunX509");
        props.put("ssl.trustmanager.algorithm", "PKIX");

        props.put("enabled", "true");
        props.put("auto.create.topics.enable", "false");

        return Collections.unmodifiableMap(props);
    }
}
