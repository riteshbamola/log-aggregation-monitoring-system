package com.ritesh.log_aggregation_montioring_system_server.repository;

import com.ritesh.log_aggregation_montioring_system_server.model.LogMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LogMetadataRepository extends JpaRepository<LogMetadata, String> {
}
