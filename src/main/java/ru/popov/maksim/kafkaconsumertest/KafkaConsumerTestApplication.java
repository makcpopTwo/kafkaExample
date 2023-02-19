package ru.popov.maksim.kafkaconsumertest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(KafkaConsumerConfiguration.class)
public class KafkaConsumerTestApplication implements CommandLineRunner {

    private final static Logger LOG = LoggerFactory
            .getLogger(KafkaConsumerTestApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerTestApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("EXECUTING : command line runner");
    }
}
