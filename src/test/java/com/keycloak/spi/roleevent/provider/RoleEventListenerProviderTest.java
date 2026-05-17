package com.keycloak.spi.roleevent.provider;

import com.keycloak.spi.roleevent.kafka.KafkaProducerManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.keycloak.events.Event;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RoleEventListenerProviderTest {

    @Mock
    private KafkaProducerManager kafkaProducerManager;

    private RoleEventListenerProvider provider;

    @BeforeEach
    void setUp() {
        provider = new RoleEventListenerProvider(kafkaProducerManager);
    }

    @Test
    void testOnEvent_UserEvent_ShouldBeIgnored() {
        Event event = new Event();
        provider.onEvent(event);
        verify(kafkaProducerManager, never()).sendAsync(anyString(), anyString());
    }

    @Test
    void testOnEvent_AdminEvent_NotRoleMapping_ShouldBeIgnored() {
        AdminEvent adminEvent = new AdminEvent();
        adminEvent.setResourceType(ResourceType.USER);
        adminEvent.setOperationType(OperationType.CREATE);

        provider.onEvent(adminEvent, false);
        verify(kafkaProducerManager, never()).sendAsync(anyString(), anyString());
    }

    @Test
    void testOnEvent_AdminEvent_RealmRoleAssigned_ShouldPublishMessage() {
        AdminEvent adminEvent = new AdminEvent();
        adminEvent.setRealmId("test-realm");
        adminEvent.setResourceType(ResourceType.REALM_ROLE_MAPPING);
        adminEvent.setOperationType(OperationType.CREATE);
        adminEvent.setResourcePath("users/user123/role-mappings/realm");
        adminEvent.setTime(System.currentTimeMillis());

        provider.onEvent(adminEvent, false);

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);

        verify(kafkaProducerManager).sendAsync(keyCaptor.capture(), messageCaptor.capture());

        String key = keyCaptor.getValue();
        String message = messageCaptor.getValue();

        assertTrue(key.equals("user123"));
        assertTrue(message.contains("\"eventType\":\"ROLE_ASSIGNED\""));
        assertTrue(message.contains("\"realmId\":\"test-realm\""));
        assertTrue(message.contains("\"userId\":\"user123\""));
    }

    @Test
    void testOnEvent_AdminEvent_ClientRoleUnassigned_ShouldPublishMessage() {
        AdminEvent adminEvent = new AdminEvent();
        adminEvent.setRealmId("test-realm");
        adminEvent.setResourceType(ResourceType.CLIENT_ROLE_MAPPING);
        adminEvent.setOperationType(OperationType.DELETE);
        adminEvent.setResourcePath("users/user456/role-mappings/clients/client789");
        adminEvent.setTime(System.currentTimeMillis());

        provider.onEvent(adminEvent, false);

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);

        verify(kafkaProducerManager).sendAsync(keyCaptor.capture(), messageCaptor.capture());

        String key = keyCaptor.getValue();
        String message = messageCaptor.getValue();

        assertTrue(key.equals("user456"));
        assertTrue(message.contains("\"eventType\":\"ROLE_UNASSIGNED\""));
        assertTrue(message.contains("\"realmId\":\"test-realm\""));
        assertTrue(message.contains("\"userId\":\"user456\""));
    }
}
