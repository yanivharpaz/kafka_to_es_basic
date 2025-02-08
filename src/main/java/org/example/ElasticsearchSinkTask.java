package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.example.config.KafkaConfig;
import org.example.service.ElasticsearchService;
import org.example.service.DlqService;
import org.example.config.ElasticsearchConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public class ElasticsearchSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSinkTask.class);
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
            logger.error("Batch processing failed, sending entire batch to DLQ", e);
            sendBatchToDlq(records, e);
        }
    }

    private void sendBatchToDlq(Collection<SinkRecord> records, Exception error) {
        for (SinkRecord record : records) {
            try {
                ConsumerRecord<String, String> consumerRecord = convertToConsumerRecord(record);
                dlqService.sendToDlq(consumerRecord, error);
            } catch (Exception dlqError) {
                logger.error("Failed to send record to DLQ - Key: {}, Error: {}", 
                    record.key(), dlqError.getMessage(), dlqError);
            }
        }
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