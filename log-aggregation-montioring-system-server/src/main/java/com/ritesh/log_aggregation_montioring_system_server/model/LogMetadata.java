package com.ritesh.log_aggregation_montioring_system_server.model;


import com.ritesh.log_aggregation_montioring_system_server.enums.KafkaStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;

@Entity
@Table(name = "log_metadata")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder

public class LogMetadata {

    @Id
    private String logId;
    private String level;
    private String serviceName;
    private String traceId;
    private Instant timestamp;
    private Boolean indexed;
    private KafkaStatus kafkaStatus;
    private Instant createdAt = Instant.now();
}