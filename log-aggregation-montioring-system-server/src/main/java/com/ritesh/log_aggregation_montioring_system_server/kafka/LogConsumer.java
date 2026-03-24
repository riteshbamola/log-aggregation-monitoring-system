package com.ritesh.log_aggregation_montioring_system_server.kafka;

import org.springframework.kafka.annotation.KafkaListener;


public class LogConsumer {

    @KafkaListener(topics = "my_topic", groupId = "group_id")
    public void consume(String message) {
        System.out.println("Message received: " + message);
    }

    @KafkaListener(topics = "ASD", groupId = "group_id")
    public void consumeLog(String message) {
        System.out.println("Message received: " + message);
    }
}
