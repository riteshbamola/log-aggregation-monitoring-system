package com.ritesh.log_aggregation_montioring_system_server.grpc;

import com.ritesh.log_aggregation_montioring_system_server.model.LogMetadata;
import com.ritesh.log_aggregation_montioring_system_server.repository.LogMetadataRepository;
import com.ritesh.log_aggregation_proto.LogRequest;
import com.ritesh.log_aggregation_proto.LogResponse;
import com.ritesh.log_aggregation_proto.LogServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.grpc.server.service.GrpcService;

import java.time.LocalDateTime;

@GrpcService
public class LogIngestionServiceImpl extends LogServiceGrpc.LogServiceImplBase {


    @Autowired
    private LogMetadataRepository logMetadataRepo;

    @Override
    public void ingestLog(LogRequest request, StreamObserver<LogResponse> responseObserver) {
        LogMetadata logMetadata = new LogMetadata();

        logMetadata.setLogId(request.getLogId());
        logMetadata.setLevel(request.getLevel());
        logMetadata.setServiceName(request.getServiceName());
        logMetadata.setTraceId(request.getTraceId());
        logMetadata.setTimestamp(LocalDateTime.parse(request.getTimestamp()));
        logMetadata.setIndexed(false);
        logMetadata.setKafkaAcked(false);

        logMetadataRepo.save(logMetadata);
        LogResponse logResponse = LogResponse.newBuilder()
                .setLogId(logMetadata.getLogId()).setSuccess(true)
                .setMessage("MetaData Saved Succesfully")
                .setErrorCode("")
                .build();

        responseObserver.onNext(logResponse);
        responseObserver.onCompleted();
    }
}
