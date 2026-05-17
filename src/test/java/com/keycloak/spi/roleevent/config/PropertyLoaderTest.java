package com.keycloak.spi.roleevent.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PropertyLoaderTest {

    @Test
    void testPropertyLoaderInitializationAndDefaults() {
        PropertyLoader propertyLoader = new PropertyLoader();
        
        // Since APP_ENV might not be set, it should default to 'dev' and load application-dev.properties
        Properties properties = propertyLoader.getProperties();
        assertNotNull(properties);
        
        // Verify default topic
        assertEquals("role-events-dev", propertyLoader.getKafkaTopic());
    }

    @Test
    void testKafkaProducerPropertiesHaveRequiredDefaults() {
        PropertyLoader propertyLoader = new PropertyLoader();
        Properties producerProps = propertyLoader.getKafkaProducerProperties();

        assertNotNull(producerProps);
        
        // Idempotence and retry-safe
        assertEquals("true", producerProps.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("all", producerProps.getProperty(ProducerConfig.ACKS_CONFIG));
        assertEquals(Integer.toString(Integer.MAX_VALUE), producerProps.getProperty(ProducerConfig.RETRIES_CONFIG));
        assertEquals("5", producerProps.getProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
        
        // Serializers
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", producerProps.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", producerProps.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        
        // Topic should NOT be in producer properties
        assertTrue(!producerProps.containsKey("kafka.topic"));
    }
}
