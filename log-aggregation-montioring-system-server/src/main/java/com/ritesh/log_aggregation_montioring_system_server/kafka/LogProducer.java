package com.ritesh.log_aggregation_montioring_system_server.kafka;
import com.ritesh.log_aggregation_montioring_system_server.enums.KafkaStatus;
import com.ritesh.log_aggregation_montioring_system_server.model.LogDocument;
import com.ritesh.log_aggregation_montioring_system_server.model.LogMetadata;
import com.ritesh.log_aggregation_montioring_system_server.repository.LogMetadataRepository;
import com.ritesh.log_aggregation_proto.LogRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class LogProducer {


    @Autowired
    private LogMetadataRepository logMetadataRepo;



    private static final String TOPIC = "my_topic";
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public LogProducer(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(LogRequest message, LogMetadata logMetadata) {
        kafkaTemplate.send(TOPIC, logMetadata.getLogId(), message.toByteArray()).whenComplete((result,ex) ->{
            if(ex==null){
                logMetadata.setKafkaStatus(KafkaStatus.SENT);
            }
            else{
                logMetadata.setKafkaStatus(KafkaStatus.FAILED);
            }
            logMetadataRepo.save(logMetadata);
        });
    }
}
