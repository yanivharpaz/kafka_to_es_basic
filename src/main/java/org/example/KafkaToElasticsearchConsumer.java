package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.sink.SinkRecord;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class KafkaToElasticsearchConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaToElasticsearchConsumer.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final String SOURCE_TOPIC = "my-topic";

    private final KafkaConsumer<String, String> consumer;
    private final org.example.ElasticsearchSinkTask sinkTask;
    private volatile boolean running = true;

    // Constructor for production use
    public KafkaToElasticsearchConsumer() {
        this(KafkaConfig.createConsumer("kafka-to-elasticsearch-consumer"));
    }

    // Constructor for testing
    public KafkaToElasticsearchConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
        this.sinkTask = new org.example.ElasticsearchSinkTask();
        
        // Initialize the sink task
        Map<String, String> props = new HashMap<>();
        // Add any necessary configuration
        props.put("elasticsearch.url", "http://localhost:9200");
        this.sinkTask.start(props);
        
        this.consumer.subscribe(Collections.singletonList(SOURCE_TOPIC));
    }

    private void processRecords(ConsumerRecords<String, String> records) {
        try {
            List<SinkRecord> sinkRecords = new ArrayList<>();
            
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Processing message - Topic: {}, Partition: {}, Offset: {}",
                        record.topic(), record.partition(), record.offset());

                SinkRecord sinkRecord = new SinkRecord(
                    record.topic(),
                    record.partition(),
                    null, // keySchema
                    record.key(),
                    null, // valueSchema
                    record.value(),
                    record.offset(),
                    record.timestamp(),
                    record.timestampType()
                );
                sinkRecords.add(sinkRecord);
            }

            if (!sinkRecords.isEmpty()) {
                sinkTask.put(sinkRecords);
            }

            consumer.commitSync();
        } catch (Exception e) {
            logger.error("Error processing batch: {}", e.getMessage(), e);
        }
    }

    public void run() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                
                if (!records.isEmpty()) {
                    processRecords(records);
                }
            }
        } finally {
            sinkTask.stop();
            consumer.close();
        }
    }

    public void shutdown() {
        running = false;
    }

    public static void main(String[] args) {
        KafkaToElasticsearchConsumer consumer = new KafkaToElasticsearchConsumer();
        consumer.run();
    }
}