package com.keycloak.spi.roleevent.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class PropertyLoader {

    private static final String DEFAULT_ENV = "dev";
    private static final String ENV_VARIABLE_PATTERN = "\\$\\{([^}]+)\\}";

    private final Properties properties = new Properties();

    public PropertyLoader() {
        loadProperties();
    }

    private void loadProperties() {
        String env = System.getenv("APP_ENV");
        if (env == null || env.trim().isEmpty()) {
            env = DEFAULT_ENV;
            log.warn("APP_ENV environment variable is not set. Defaulting to '{}'", DEFAULT_ENV);
        } else {
            log.info("Active environment: '{}'", env);
        }

        String propertyFileName = String.format("application-%s.properties", env);

        try (InputStream input = getClass().getClassLoader().getResourceAsStream(propertyFileName)) {
            if (input == null) {
                log.error("Sorry, unable to find {}", propertyFileName);
                return;
            }

            // Load a properties file from class path
            Properties rawProperties = new Properties();
            rawProperties.load(input);

            // Resolve environment variables in property values
            for (String key : rawProperties.stringPropertyNames()) {
                String value = rawProperties.getProperty(key);
                String resolvedValue = resolveEnvVariables(value);
                properties.setProperty(key, resolvedValue);
            }

            log.info("Successfully loaded properties from {}", propertyFileName);
        } catch (IOException ex) {
            log.error("Error loading properties file {}", propertyFileName, ex);
        }
    }

    private String resolveEnvVariables(String value) {
        if (value == null) {
            return null;
        }

        Pattern pattern = Pattern.compile(ENV_VARIABLE_PATTERN);
        Matcher matcher = pattern.matcher(value);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            String envVarName = matcher.group(1);
            String envVarValue = System.getenv(envVarName);
            if (envVarValue == null) {
                log.warn("Environment variable {} is not set, leaving placeholder as is.", envVarName);
                matcher.appendReplacement(sb, "\\$\\{" + envVarName + "\\}");
            } else {
                // Escape backslashes and dollar signs for appendReplacement
                matcher.appendReplacement(sb, Matcher.quoteReplacement(envVarValue));
            }
        }
        matcher.appendTail(sb);

        return sb.toString();
    }

    public Properties getProperties() {
        return properties;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }
    
    public String getKafkaTopic() {
        return properties.getProperty("kafka.topic", "role-events");
    }

    public Properties getKafkaProducerProperties() {
        Properties producerProps = new Properties();
        
        // Copy relevant Kafka properties
        for (String key : properties.stringPropertyNames()) {
            if (!key.equals("kafka.topic")) {
                producerProps.setProperty(key, properties.getProperty(key));
            }
        }

        // Add idempotence, retry-safe configs if not present in the properties file
        if (!producerProps.containsKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)) {
            producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        }
        if (!producerProps.containsKey(ProducerConfig.ACKS_CONFIG)) {
            producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        }
        if (!producerProps.containsKey(ProducerConfig.RETRIES_CONFIG)) {
            producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        }
        if (!producerProps.containsKey(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)) {
            producerProps.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        }
        
        // Serializers if not present in the properties file
        if (!producerProps.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        }
        if (!producerProps.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        }

        return producerProps;
    }
}
