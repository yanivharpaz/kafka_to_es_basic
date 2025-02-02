package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.example.config.ElasticsearchConfig;
import org.example.config.KafkaConfig;
import org.example.service.DlqService;
import org.example.service.ElasticsearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

public class KafkaToElasticsearchConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaToElasticsearchConsumer.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final String SOURCE_TOPIC = "my-topic";
    private static final String DLQ_TOPIC = "my-topic-dlq";
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 1000;

    private final KafkaConsumer<String, String> consumer;
    private final ElasticsearchService elasticsearchService;
    private final DlqService dlqService;
    private volatile boolean running = true;

    // Constructor for production use
    public KafkaToElasticsearchConsumer() {
        this(
            KafkaConfig.createConsumer("kafka-to-elasticsearch-consumer"),
            new ElasticsearchService(ElasticsearchConfig.createClient()),
            new DlqService(KafkaConfig.createProducer(), DLQ_TOPIC)
        );
    }

    // Constructor for testing
    public KafkaToElasticsearchConsumer(
            KafkaConsumer<String, String> consumer,
            ElasticsearchService elasticsearchService,
            DlqService dlqService) {
        this.consumer = consumer;
        this.elasticsearchService = elasticsearchService;
        this.dlqService = dlqService;
        this.consumer.subscribe(Collections.singletonList(SOURCE_TOPIC));
    }

    // Constructor for simpler testing scenarios
    public KafkaToElasticsearchConsumer(
            KafkaConsumer<String, String> consumer,
            ElasticsearchService elasticsearchService) {
        this(
            consumer,
            elasticsearchService,
            new DlqService(KafkaConfig.createProducer(), DLQ_TOPIC)
        );
    }

    private void indexToElasticsearchWithRetry(ConsumerRecord<String, String> record) throws IOException {
        Exception lastException = null;

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                if (attempt > 0) {
                    Thread.sleep(RETRY_BACKOFF_MS * attempt);
                }
                elasticsearchService.indexDocument(record.value());
                return;
            } catch (Exception e) {
                lastException = e;
                logger.error("Elasticsearch indexing attempt {} failed: {}", attempt + 1, e.getMessage());
            }
        }

        if (lastException != null) {
            dlqService.sendToDlq(record, lastException);
        }
    }

    private void processRecords(ConsumerRecords<String, String> records) {
        try {
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Processing message - Topic: {}, Partition: {}, Offset: {}",
                        record.topic(), record.partition(), record.offset());

                try {
                    elasticsearchService.addToBatch(record.value());
                } catch (Exception e) {
                    logger.error("Error processing record: {}", record.value(), e);
                    dlqService.sendToDlq(record, e);
                }
            }

            // Commit offsets after processing the batch
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
            try {
                elasticsearchService.flushBatch(); // Ensure final batch is flushed
            } catch (Exception e) {
                logger.error("Error flushing final batch: {}", e.getMessage(), e);
            }
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