package org.example.service;

import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ElasticsearchSinkTaskTest {

    private ElasticsearchSinkTask sinkTask;
    private Map<String, String> props;

    @Mock
    private SinkRecord sinkRecord;

    @BeforeEach
    void setUp() {
        props = new HashMap<>();
        props.put("elasticsearch.host", "localhost");
        props.put("elasticsearch.port", "9200");
        props.put("batch.size", "10");
        props.put("flush.interval.ms", "5000");
        
        sinkTask = new ElasticsearchSinkTask();
    }

    @Test
    void testPutRecords() {
        // Start the task
        sinkTask.start(props);

        // Create test records
        List<SinkRecord> records = new ArrayList<>();
        records.add(createSinkRecord("test-topic", 0, 
            """
            {
                "ProductType": "book",
                "title": "Test Book"
            }
            """));

        // Put records
        sinkTask.put(records);

        // Clean up
        sinkTask.stop();
    }

    private SinkRecord createSinkRecord(String topic, int partition, String value) {
        return new SinkRecord(
            topic,                  // topic
            partition,             // partition
            null,                  // keySchema
            null,                  // key
            null,                  // valueSchema
            value,                 // value
            System.currentTimeMillis() // timestamp
        );
    }
} 