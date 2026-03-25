package com.ritesh.log_aggregation_montioring_system_server.grpc;

import com.ritesh.log_aggregation_montioring_system_server.enums.KafkaStatus;
import com.ritesh.log_aggregation_montioring_system_server.kafka.LogProducer;
import com.ritesh.log_aggregation_montioring_system_server.model.LogDocument;
import com.ritesh.log_aggregation_montioring_system_server.model.LogMetadata;
import com.ritesh.log_aggregation_montioring_system_server.repository.LogMetadataRepository;
import com.ritesh.log_aggregation_proto.LogRequest;
import com.ritesh.log_aggregation_proto.LogResponse;
import com.ritesh.log_aggregation_proto.LogServiceGrpc;
import com.ritesh.log_aggregation_proto.LogSummary;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.grpc.server.service.GrpcService;

import java.time.Instant;
import java.time.LocalDateTime;

@Slf4j
@GrpcService
public class LogIngestionServiceImpl extends LogServiceGrpc.LogServiceImplBase {

    @Autowired
    private LogMetadataRepository logMetadataRepo;
    @Autowired
    private LogProducer logProducer;

    @Override
    public void ingestLog(LogRequest request, StreamObserver<LogResponse> responseObserver) {
        try {

            LogMetadata logMetadata = buildLogMetaData(request);;

            //kafka publish
            logMetadataRepo.save(logMetadata);
            logProducer.sendMessage(request,logMetadata);
            responseObserver.onNext(LogResponse.newBuilder()
                    .setLogId(logMetadata.getLogId())
                    .setSuccess(true)
                    .setMessage("Log ingested successfully")
                    .setErrorCode("")
                    .build());
        } catch (Exception e) {
            responseObserver.onNext(LogResponse.newBuilder()
                    .setLogId(request.getLogId())
                    .setSuccess(false)
                    .setMessage("Ingestion failed: " + e.getMessage())
                    .setErrorCode("INGESTION_ERROR")
                    .build());
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public StreamObserver<LogRequest> bulkIngest(StreamObserver<LogSummary> responseObserver) {
        return new StreamObserver<LogRequest>() {

            private int logsReceived = 0;
            private int logsinKafka = 0;
            private int logsinDB=0;

            @Override
            public void onNext(LogRequest logRequest) {
                logsReceived++;

                try {
                    LogMetadata logMetadata = buildLogMetaData(logRequest);
                    logMetadataRepo.save(logMetadata);
                    logsinDB++;
                    logProducer.sendMessage(logRequest,logMetadata);
                    logsinKafka++;

                } catch (Exception e) {}
            }

            @Override
            public void onError(Throwable throwable) {
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(LogSummary.newBuilder()
                                .setLogsDb(logsinDB)
                                .setLogsKafka(logsinKafka)
                                .setLogsSent(logsReceived)
                        .build());
                responseObserver.onCompleted();
            }
        };
    }

    private LogMetadata buildLogMetaData(LogRequest request) {
        return LogMetadata.builder()
                .logId(request.getLogId())
                .level(request.getLevel())
                .serviceName(request.getServiceName())
                .traceId(request.getTraceId())
                .timestamp(request.getTimestamp().isBlank()
                        ? Instant.now()
                        : Instant.parse(request.getTimestamp()))
                .indexed(false)
                .kafkaStatus(KafkaStatus.PENDING)
                .build();
    }
}
