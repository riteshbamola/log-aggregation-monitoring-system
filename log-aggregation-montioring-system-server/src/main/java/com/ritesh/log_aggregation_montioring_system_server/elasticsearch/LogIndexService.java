package com.ritesh.log_aggregation_montioring_system_server.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.CreateRequest;
import com.ritesh.log_aggregation_montioring_system_server.model.LogDocument;
import com.ritesh.log_aggregation_montioring_system_server.repository.LogDocumentRepository;
import com.ritesh.log_aggregation_proto.LogRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.stereotype.Service;

@Service
public class LogIndexService {

    @Autowired
    private ElasticsearchClient client;

    @Autowired
    private LogDocumentRepository logDocumentRepo;

    public void saveIndex(LogDocument logDocument){
        logDocumentRepo.save(logDocument);
    }

}
