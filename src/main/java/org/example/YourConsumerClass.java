package org.example;

import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.client.RestHighLevelClient;
import org.example.service.ElasticsearchService;
import java.util.ArrayList;
import java.util.Collection;

public class YourConsumerClass {
    private final ElasticsearchService elasticsearchService;

    public YourConsumerClass(RestHighLevelClient client) {
        this.elasticsearchService = new ElasticsearchService(client);
    }

    public void processRecords(Collection<SinkRecord> records) {
        elasticsearchService.processSinkRecords(records);
    }

    // Your other consumer logic here...
} 