package org.example.service;

import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.apache.http.HttpHost;
import java.util.Collection;
import java.util.Map;

public class ElasticsearchSinkTask extends SinkTask {
    private ElasticsearchService elasticsearchService;
    private static final String VERSION = "1.0";

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        // Initialize Elasticsearch client
        RestClientBuilder builder = RestClient.builder(
            new HttpHost(props.getOrDefault("elasticsearch.host", "localhost"),
                        Integer.parseInt(props.getOrDefault("elasticsearch.port", "9200")), "http"));
        
        RestHighLevelClient client = new RestHighLevelClient(builder);
        int batchSize = Integer.parseInt(props.getOrDefault("batch.size", "10"));
        long flushIntervalMs = Long.parseLong(props.getOrDefault("flush.interval.ms", "5000"));
        elasticsearchService = new ElasticsearchService(client, batchSize, flushIntervalMs);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        elasticsearchService.processSinkRecords(records);
    }

    @Override
    public void stop() {
        try {
            if (elasticsearchService != null) {
                elasticsearchService.close();
            }
        } catch (Exception e) {
            System.err.println("Error closing Elasticsearch client: " + e.getMessage());
        }
    }
} 