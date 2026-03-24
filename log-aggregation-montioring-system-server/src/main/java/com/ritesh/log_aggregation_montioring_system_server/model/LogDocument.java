package com.ritesh.log_aggregation_montioring_system_server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Document(indexName = "logs", createIndex = true)
public class LogDocument {

    @Id
    @Field(name = "log_id", type = FieldType.Keyword)
    private String logId;

    @Field(name = "level", type = FieldType.Keyword)
    private String level;

    @Field(name = "service_name", type = FieldType.Keyword)
    private String serviceName;

    @Field(name = "trace_id", type = FieldType.Keyword)
    private String traceId;

    @Field(name = "message", type = FieldType.Text)
    private String message;

    @Field(name = "timestamp", type = FieldType.Date,
            format = {}, pattern = "uuuu-MM-dd'T'HH:mm:ss.SSSXXX||epoch_millis")
    private Instant timestamp;
}