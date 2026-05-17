package com.keycloak.spi.roleevent.kafka;

import com.keycloak.spi.roleevent.config.PropertyLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

@Slf4j
public class KafkaProducerManager {

    private static volatile KafkaProducerManager instance;
    private final KafkaProducer<String, String> producer;
    private final String topic;

    private KafkaProducerManager() {
        log.info("Initializing KafkaProducerManager");
        PropertyLoader propertyLoader = new PropertyLoader();
        Properties producerProps = propertyLoader.getKafkaProducerProperties();
        this.topic = propertyLoader.getKafkaTopic();
        this.producer = new KafkaProducer<>(producerProps);
        log.info("KafkaProducer initialized for topic {}", topic);
    }

    public static KafkaProducerManager getInstance() {
        if (instance == null) {
            synchronized (KafkaProducerManager.class) {
                if (instance == null) {
                    instance = new KafkaProducerManager();
                }
            }
        }
        return instance;
    }

    public Future<RecordMetadata> sendAsync(String key, String value) {
        log.debug("Sending message asynchronously to topic {}: key={}, value={}", topic, key, value);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        
        return producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send message to Kafka topic {}", topic, exception);
            } else {
                log.debug("Message successfully sent to Kafka topic {} partition {} at offset {}", 
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    public void close() {
        log.info("Closing KafkaProducer...");
        if (producer != null) {
            producer.close();
        }
        log.info("KafkaProducer closed.");
    }
}
