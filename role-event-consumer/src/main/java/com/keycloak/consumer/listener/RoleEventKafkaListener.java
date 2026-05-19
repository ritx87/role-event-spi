package com.keycloak.consumer.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.keycloak.consumer.dto.RoleEventMessage;
import com.keycloak.consumer.entity.RoleAudit;
import com.keycloak.consumer.repository.RoleAuditRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
@Slf4j
@RequiredArgsConstructor
public class RoleEventKafkaListener {

    private final RoleAuditRepository roleAuditRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "role-events-dev", groupId = "role-audit-group")
    public void consume(String message) {
        log.info("Received role event message: {}", message);
        try {
            RoleEventMessage event = objectMapper.readValue(message, RoleEventMessage.class);
            
            RoleAudit audit = new RoleAudit();
            audit.setEventType(event.getEventType());
            audit.setRealmId(event.getRealmId());
            audit.setClientId(event.getClientId());
            audit.setUserId(event.getUserId());
            audit.setRoleId(event.getRoleId());
            audit.setRoleName(event.getRoleName());
            audit.setEventTimestamp(event.getTimestamp());
            audit.setProcessedAt(Instant.now());
            
            roleAuditRepository.save(audit);
            log.info("Successfully persisted role audit event for user: {}", event.getUserId());
            
        } catch (Exception e) {
            log.error("Failed to process role event message", e);
        }
    }
}
