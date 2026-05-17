package com.keycloak.spi.roleevent.provider;

import com.keycloak.spi.roleevent.kafka.KafkaProducerManager;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

@Slf4j
public class RoleEventListenerProviderFactory implements EventListenerProviderFactory {

    private static final String PROVIDER_ID = "role-event-kafka-publisher";
    private KafkaProducerManager kafkaProducerManager;

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        return new RoleEventListenerProvider(kafkaProducerManager);
    }

    @Override
    public void init(Config.Scope config) {
        log.info("Initializing RoleEventListenerProviderFactory");
        // Initialize the singleton Kafka Producer Manager upon Keycloak start
        this.kafkaProducerManager = KafkaProducerManager.getInstance();
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        // Nothing to do after initialization
    }

    @Override
    public void close() {
        log.info("Closing RoleEventListenerProviderFactory");
        if (kafkaProducerManager != null) {
            kafkaProducerManager.close();
        }
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }
}
