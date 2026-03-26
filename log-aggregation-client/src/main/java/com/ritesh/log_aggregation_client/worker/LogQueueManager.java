package com.ritesh.log_aggregation_client.worker;

import com.ritesh.log_aggregation_client.appender.LogAppender;
import com.ritesh.log_aggregation_client.grpc.LogIngester;
import com.ritesh.log_aggregation_proto.LogRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@Service
public class LogQueueManager {

    private static final LogQueueManager INSTANCE = new LogQueueManager();

    private final BlockingQueue<LogRequest> queue =
            new ArrayBlockingQueue<>(10000);


    @Autowired
    private LogIngester logIngester;

    private LogQueueManager() {
        startWorker();
    }

    public static LogQueueManager getInstance() {
        return INSTANCE;
    }

    public void enqueue(LogRequest log) {
        queue.offer(log);
    }

    private void startWorker() {
        new Thread(() -> {
            List<LogRequest> batch = new ArrayList<>();

            while (true) {
                try {
                    LogRequest log = queue.poll(1, TimeUnit.SECONDS);
                    if (log != null) {
                        batch.add(log);
                    }
                    if (batch.size() >= 50 || (!batch.isEmpty() && log == null)) {
                        logIngester.bulkIngest(batch);
                        batch.clear();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}