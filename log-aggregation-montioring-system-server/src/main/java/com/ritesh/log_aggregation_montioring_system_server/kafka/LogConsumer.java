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
import java.util.ArrayList;
import java.util.List;

@Service
public class LogConsumer {

    @Autowired
    private LogIndexService logIndexService;
    private long  start = 0;
    private int count=0;
    private List<LogDocument> batch = new ArrayList<>();

    @KafkaListener(topics = "logs.ingestion", groupId = "log-consumer")
    public void consumeLog(byte[] message, Acknowledgment acknowledgment) {
        try {

            if(count == 0){
                start = System.currentTimeMillis();
            }

            LogRequest logRequest = LogRequest.parseFrom(message);
            LogDocument logDocument = buildLogDocument(logRequest);
            batch.add(logDocument);
            count++;
//            logIndexService.saveIndex(logDocument);

            acknowledgment.acknowledge();
            if(batch.size() >= 50){
                logIndexService.saveAll(batch);
                batch.clear();
            }


            if(count >= 500){
                long end = System.currentTimeMillis();
                System.out.println("Time Taken : " + (end-start) + " ms");
                System.out.println("Logs in ElasticSearch: "+ count);
            }

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
