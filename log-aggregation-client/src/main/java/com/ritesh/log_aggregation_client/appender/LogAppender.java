package com.ritesh.log_aggregation_client.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.ritesh.log_aggregation_client.grpc.LogIngester;
import com.ritesh.log_aggregation_client.worker.LogQueueManager;
import com.ritesh.log_aggregation_proto.LogRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class LogAppender extends AppenderBase<ILoggingEvent> {

    @Override
    protected void append(ILoggingEvent iLoggingEvent) {
        String logid = UUID.randomUUID().toString();

        LogRequest logRequest = LogRequest.newBuilder()
                .setLogId(logid)
                .setMessage(iLoggingEvent.getMessage())
                .setLevel(iLoggingEvent.getLevel().toString())
                .setTraceId(logid)   //will later change it
                .setServiceName("Order-Service")
                .setTimestamp(Instant.now().toString())
                .build();

        LogQueueManager.getInstance().enqueue(logRequest);
    }


}
