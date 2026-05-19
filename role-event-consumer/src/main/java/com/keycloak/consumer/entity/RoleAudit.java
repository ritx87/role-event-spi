package com.keycloak.consumer.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import java.time.Instant;

@Entity
@Table(name = "role_audit")
@Data
public class RoleAudit {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private String eventType;
    private String realmId;
    private String clientId;
    private String userId;
    private String roleId;
    private String roleName;
    private Long eventTimestamp;
    private Instant processedAt;
}
