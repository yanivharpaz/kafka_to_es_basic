package org.example.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.example.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.sink.SinkRecord;
import org.example.service.ElasticsearchService;
import org.example.service.DlqService;
import org.example.config.ElasticsearchConfig;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public class ElasticsearchSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSinkTask.class);
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 1000;
    private static final String DLQ_TOPIC = "my-topic-dlq";

    private ElasticsearchService elasticsearchService;
    private DlqService dlqService;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        elasticsearchService = new ElasticsearchService(ElasticsearchConfig.createClient());
        dlqService = new DlqService(KafkaConfig.createProducer(), DLQ_TOPIC);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        try {
            elasticsearchService.processSinkRecords(records);
        } catch (Exception e) {
            logger.error("Batch processing failed, attempting individual record processing", e);
            for (SinkRecord record : records) {
                try {
                    retryOrSendToDlq(record, e);
                } catch (Exception recordError) {
                    logger.error("Error processing record {}: {}", record.key(), recordError.getMessage());
                }
            }
        }
    }

    private void retryOrSendToDlq(SinkRecord record, Exception originalError) {
        Exception lastException = originalError;

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                if (attempt > 0) {
                    Thread.sleep(RETRY_BACKOFF_MS * attempt);
                }
                elasticsearchService.indexDocument(record.value().toString());
                return;
            } catch (Exception e) {
                lastException = e;
                logger.error("Retry attempt {} failed for record {}: {}",
                        attempt + 1, record.key(), e.getMessage());
            }
        }

        dlqService.sendToDlq(convertToConsumerRecord(record), lastException);
    }


    private ConsumerRecord<String, String> convertToConsumerRecord(SinkRecord record) {
        return new ConsumerRecord<String, String>(
                record.topic(),
                record.kafkaPartition(),
                record.kafkaOffset(),
                record.timestamp(),
                record.timestampType(),
                0L, // checksum
                0, // serialized key size
                0, // serialized value size
                record.key() != null ? record.key().toString() : null,
                record.value() != null ? record.value().toString() : null
        );
    }

    @Override
    public void stop() {
        try {
            elasticsearchService.flushBatch();
            elasticsearchService.close();
        } catch (Exception e) {
            logger.error("Error during shutdown: {}", e.getMessage());
        }
    }
}