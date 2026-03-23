package com.ritesh.log_aggregation_montioring_system_server.model;


import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDateTime;

@Entity
@Table(name = "log_metadata")
@Data
public class LogMetadata {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String logId;
    private String level;
    private String serviceName;
    private String traceId;
    private LocalDateTime timestamp;
    private Boolean indexed;
    private Boolean kafkaAcked;
    private LocalDateTime createdAt;
}