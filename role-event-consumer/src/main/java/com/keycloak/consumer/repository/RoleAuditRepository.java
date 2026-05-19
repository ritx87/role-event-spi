package com.keycloak.consumer.repository;

import com.keycloak.consumer.entity.RoleAudit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RoleAuditRepository extends JpaRepository<RoleAudit, Long> {
}
