package com.keycloak.spi.roleevent.provider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.keycloak.spi.roleevent.kafka.KafkaProducerManager;
import com.keycloak.spi.roleevent.model.RoleEventMessage;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;

@Slf4j
public class RoleEventListenerProvider implements EventListenerProvider {

    private final KafkaProducerManager kafkaProducerManager;
    private final ObjectMapper objectMapper;

    public RoleEventListenerProvider(KafkaProducerManager kafkaProducerManager) {
        this.kafkaProducerManager = kafkaProducerManager;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void onEvent(Event event) {
        // User events (like LOGIN, LOGOUT) - we don't care about these for role assignments.
    }

    @Override
    public void onEvent(AdminEvent adminEvent, boolean includeRepresentation) {
        ResourceType resourceType = adminEvent.getResourceType();
        OperationType operationType = adminEvent.getOperationType();

        // Check if the event is related to role mappings
        if ((resourceType == ResourceType.REALM_ROLE_MAPPING || resourceType == ResourceType.CLIENT_ROLE_MAPPING) &&
            (operationType == OperationType.CREATE || operationType == OperationType.DELETE)) {
            
            handleRoleEvent(adminEvent);
        }
    }

    private void handleRoleEvent(AdminEvent adminEvent) {
        try {
            log.debug("Processing role event: ResourceType={}, OperationType={}", 
                      adminEvent.getResourceType(), adminEvent.getOperationType());
            
            String eventType = adminEvent.getOperationType() == OperationType.CREATE ? "ROLE_ASSIGNED" : "ROLE_UNASSIGNED";
            
            // Resource path is typically users/{userId}/role-mappings/realm or users/{userId}/role-mappings/clients/{clientId}
            String resourcePath = adminEvent.getResourcePath();
            String userId = extractUserId(resourcePath);
            
            RoleEventMessage message = RoleEventMessage.builder()
                    .eventType(eventType)
                    .realmId(adminEvent.getRealmId())
                    .userId(userId)
                    // The representation could contain role details if includeRepresentation=true
                    // But typically, the resource path or representation has the info. 
                    // Let's attach the raw resource path and let consumers parse it, or parse it here if possible.
                    .timestamp(adminEvent.getTime())
                    .build();

            // Additional details can be extracted from adminEvent.getRepresentation() if available.
            // For simplicity, we serialize what we have.
            
            String jsonMessage = objectMapper.writeValueAsString(message);
            
            // Use user ID as the partition key if available, otherwise realm ID
            String key = userId != null ? userId : adminEvent.getRealmId();
            
            kafkaProducerManager.sendAsync(key, jsonMessage);
            
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize role event message", e);
        } catch (Exception e) {
            log.error("Error processing admin event for role mapping", e);
        }
    }
    
    private String extractUserId(String resourcePath) {
        if (resourcePath == null) return null;
        
        // Example: "users/7bb56c2d-9eb5-407a-bf1e-b87332308a0d/role-mappings/realm"
        String[] parts = resourcePath.split("/");
        if (parts.length > 1 && "users".equals(parts[0])) {
            return parts[1];
        }
        return null;
    }

    @Override
    public void close() {
        // Factory manages the producer lifecycle, no need to close anything here.
    }
}
