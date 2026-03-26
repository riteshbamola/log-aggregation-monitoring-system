package com.ritesh.log_aggregation_client.grpc;

import com.ritesh.log_aggregation_proto.LogRequest;
import com.ritesh.log_aggregation_proto.LogResponse;
import com.ritesh.log_aggregation_proto.LogServiceGrpc;
import com.ritesh.log_aggregation_proto.LogSummary;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.apache.commons.logging.Log;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

@Service
public class LogIngester {

    @GrpcClient("stockServer")
   private LogServiceGrpc.LogServiceStub logServiceStub;

    //Unary
    public void ingestLog(){

        //HardCoded further Dyanamic  //always gonna use Bulk Ingest
        LogRequest logRequest = LogRequest.newBuilder().setLogId("log-701")
                .setLevel("ERROR").setServiceName("Order-Service")
                .setTraceId("trace-701")
                .setMessage("Order Creation Failed")
                .setTimestamp(Instant.now().toString())
                .build();

        logServiceStub.ingestLog(logRequest, new StreamObserver<LogResponse>() {
            @Override
            public void onNext(LogResponse logResponse) {
                String response = "Log_Id: " + logResponse.getLogId() + "Success :" + logResponse.getSuccess()
                        + "Message: " + logResponse.getMessage() + "Error Code: " + logResponse.getErrorCode() ;

                System.out.println(response);
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println("Error : "+ throwable);
            }

            @Override
            public void onCompleted() {
                System.out.println("Response Ended");
            }
        });
    }


    //Client Streaming
    public void bulkIngest(List<LogRequest> batch){

        StreamObserver<LogSummary> logSummary = new StreamObserver<LogSummary>() {
            @Override
            public void onNext(LogSummary logSummary) {
                String response = "Logs Received: " + logSummary.getLogsSent()
                        + "Logs in Kafka : " + logSummary.getLogsKafka()
                        + "Logs in DB: " + logSummary.getLogsDb();

                System.out.println(response);
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println("Error: " + throwable);
            }

            @Override
            public void onCompleted() {
                System.out.println("Response Ended");
            }
        };

        StreamObserver<LogRequest> requestObserver = logServiceStub.bulkIngest(logSummary);
        try{
            for(LogRequest logRequest: batch){
                requestObserver.onNext(logRequest);
            }
            requestObserver.onCompleted();

        }catch (Exception e){
            requestObserver.onError(e);
        }


    }

}
