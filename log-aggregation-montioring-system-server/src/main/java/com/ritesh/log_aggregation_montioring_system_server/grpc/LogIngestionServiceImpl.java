package com.ritesh.log_aggregation_montioring_system_server.grpc;

import com.ritesh.log_aggregation_montioring_system_server.enums.KafkaStatus;
import com.ritesh.log_aggregation_montioring_system_server.kafka.LogProducer;
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

            private int totalReceived = 0;
            private int totalSuccess = 0;
            private int totalFailed = 0;

            @Override
            public void onNext(LogRequest logRequest) {
                totalReceived++;

                try {
                    LogMetadata logMetadata = buildLogMetaData(logRequest);

                    // Publish to Kafka
                    logProducer.sendMessage(logRequest,logMetadata);
                    totalSuccess++;

                } catch (Exception e) {
                    totalFailed++;
                }
            }

            @Override
            public void onError(Throwable throwable) {
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(LogSummary.newBuilder()
                        .setTotalReceived(totalReceived)
                        .setTotalFailed(totalFailed)
                        .setTotalSuccess(totalSuccess)
                        .setMessage("Bulk ingestion complete")
                        .build());
                responseObserver.onCompleted();
            }
        };
    }

    private LogMetadata buildLogMetaData(LogRequest request) {
        LogMetadata logMetadata = new LogMetadata();
        logMetadata.setLogId(request.getLogId());
        logMetadata.setLevel(request.getLevel());
        logMetadata.setServiceName(request.getServiceName());
        logMetadata.setTraceId(request.getTraceId());
        logMetadata.setTimestamp(LocalDateTime.parse(request.getTimestamp()));
        logMetadata.setIndexed(false);
        logMetadata.setKafkaStatus(KafkaStatus.PENDING);

        return logMetadata;
    }
}
