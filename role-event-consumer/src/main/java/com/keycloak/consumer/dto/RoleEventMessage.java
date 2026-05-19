package com.keycloak.consumer.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RoleEventMessage {
    private String eventType;
    private String realmId;
    private String clientId;
    private String userId;
    private String roleId;
    private String roleName;
    private long timestamp;
}
