package com.ritesh.log_aggregation_montioring_system_server.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ritesh.log_aggregation_montioring_system_server.elasticsearch.LogIndexService;
import com.ritesh.log_aggregation_montioring_system_server.model.LogDocument;
import com.ritesh.log_aggregation_proto.LogRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class LogConsumer {

    @Autowired
    private LogIndexService logIndexService;


    @KafkaListener(topics = "my_topic", groupId = "group_id")
    public void consumeLog(byte[] message, Acknowledgment acknowledgment) {

        try {
            LogRequest logRequest = LogRequest.parseFrom(message);
            LogDocument logDocument = buildLogDocument(logRequest);

            logIndexService.saveIndex(logDocument);
            acknowledgment.acknowledge();

        } catch (Exception e) {
            System.err.println("❌ FAILED: ");
            e.printStackTrace();
        }
    }

    private LogDocument buildLogDocument(LogRequest logRequest) {
        return LogDocument.builder()
                .logId(logRequest.getLogId())
                .level(logRequest.getLevel())
                .serviceName(logRequest.getServiceName())
                .traceId(logRequest.getTraceId())
                .message(logRequest.getMessage())
                .timestamp(logRequest.getTimestamp().isBlank()
                        ? Instant.now()
                        : Instant.parse(logRequest.getTimestamp()))
                .build();
    }


}
