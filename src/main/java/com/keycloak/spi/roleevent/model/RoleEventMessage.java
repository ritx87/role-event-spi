package com.keycloak.spi.roleevent.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
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
