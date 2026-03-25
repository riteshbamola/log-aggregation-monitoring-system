package com.ritesh.log_aggregation_montioring_system_server.repository;

import com.ritesh.log_aggregation_montioring_system_server.model.LogDocument;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;


public interface LogDocumentRepository extends ElasticsearchRepository<LogDocument,String> {
}
