//package org.example;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.common.TopicPartition;
//import org.example.service.DlqService;
//import org.example.service.ElasticsearchService;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.Mock;
//import org.mockito.junit.jupiter.MockitoExtension;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.time.Duration;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;
//
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.ArgumentMatchers.anyString;
//import static org.mockito.Mockito.*;
//
//@ExtendWith(MockitoExtension.class)
//class KafkaToElasticsearchConsumerTest {
//    private static final Logger logger = LoggerFactory.getLogger(KafkaToElasticsearchConsumerTest.class);
//
//    @Mock
//    private KafkaConsumer<String, String> kafkaConsumer;
//
//    @Mock
//    private ElasticsearchService elasticsearchService;
//
//    @Mock
//    private DlqService dlqService;
//
//    private KafkaToElasticsearchConsumer consumer;
//    private ExecutorService executorService;
//
//    @BeforeEach
//    void setUp() {
//        consumer = new KafkaToElasticsearchConsumer(kafkaConsumer, elasticsearchService, dlqService);
//        executorService = Executors.newSingleThreadExecutor();
//    }
//
////    @Test
////    void testProcessRecords() throws Exception {
////        // Setup test data
////        String topic = "test-topic";
////        TopicPartition partition = new TopicPartition(topic, 0);
////
////        // Create test records
////        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(
////            topic, 0, 0L, "key1", createTestDocument("book", "Test Book 1")
////        );
////        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(
////            topic, 0, 1L, "key2", createTestDocument("book", "Test Book 2")
////        );
////
////        // Create ConsumerRecords
////        Map<TopicPartition, java.util.List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
////        recordsMap.put(partition, Arrays.asList(record1, record2));
////        ConsumerRecords<String, String> records = new ConsumerRecords<>(recordsMap);
////
////        // Mock consumer.poll() to return our test records once, then empty records
////        when(kafkaConsumer.poll(any(Duration.class)))
////            .thenReturn(records)
////            .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));
////
////        // Start consumer in separate thread
////        executorService.submit(() -> consumer.run());
////
////        // Wait a bit for processing
////        Thread.sleep(100);
////
////        // Shutdown consumer
////        consumer.shutdown();
////
////        // Wait for shutdown
////        executorService.shutdown();
////        executorService.awaitTermination(1, TimeUnit.SECONDS);
////
////        // Verify records were processed
////        verify(elasticsearchService, times(2)).indexDocument(anyString());
////        verify(elasticsearchService).indexDocument(record1.value());
////        verify(elasticsearchService).indexDocument(record2.value());
////    }
//
////    @Test
////    void testHandleElasticsearchError() throws Exception {
////        // Setup test data
////        String topic = "test-topic";
////        TopicPartition partition = new TopicPartition(topic, 0);
////        ConsumerRecord<String, String> record = new ConsumerRecord<>(
////            topic, 0, 0L, "key1", createTestDocument("book", "Test Book")
////        );
////
////        Map<TopicPartition, java.util.List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
////        recordsMap.put(partition, Collections.singletonList(record));
////        ConsumerRecords<String, String> records = new ConsumerRecords<>(recordsMap);
////
////        // Mock consumer to return our test record once, then empty records forever
////        when(kafkaConsumer.poll(any(Duration.class)))
////            .thenReturn(records)
////            .thenAnswer(invocation -> {
////                // Slow down subsequent polls to prevent too many retries
////                Thread.sleep(50);
////                return new ConsumerRecords<>(Collections.emptyMap());
////            });
////
////        // Mock Elasticsearch service to throw an exception for all attempts
////        doThrow(new IOException("Test error"))
////            .when(elasticsearchService)
////            .indexDocument(anyString());
////
////        // Start consumer in separate thread
////        executorService.submit(() -> consumer.run());
////
////        // Wait a bit for processing (increased wait time to ensure all retries complete)
////        Thread.sleep(500);
////
////        // Shutdown consumer
////        consumer.shutdown();
////
////        // Wait for shutdown
////        executorService.shutdown();
////        executorService.awaitTermination(1, TimeUnit.SECONDS);
////
////        // Verify error handling - expect 2 attempts before giving up
////        verify(elasticsearchService, times(2)).indexDocument(record.value());
////        verify(dlqService).sendToDlq(eq(record), any(IOException.class));
////
////        // Verify the order of operations
////        inOrder(elasticsearchService, dlqService)
////            .verify(elasticsearchService, times(2)).indexDocument(record.value())
////            .verify(dlqService).sendToDlq(eq(record), any(IOException.class));
////    }
//
//    @Test
//    void testEmptyRecords() throws Exception {
//        // Mock consumer to return empty records
//        when(kafkaConsumer.poll(any(Duration.class)))
//            .thenAnswer(invocation -> {
//                // Return empty records and sleep to prevent tight loop
//                Thread.sleep(50);
//                return new ConsumerRecords<>(Collections.emptyMap());
//            });
//
//        // Start consumer in separate thread
//        executorService.submit(() -> consumer.run());
//
//        // Wait a bit
//        Thread.sleep(200);
//
//        // Shutdown consumer
//        consumer.shutdown();
//
//        // Wait for shutdown
//        executorService.shutdown();
//        executorService.awaitTermination(1, TimeUnit.SECONDS);
//
//        // Verify no documents were indexed
//        verify(elasticsearchService, never()).indexDocument(anyString());
//    }
//
//    private String createTestDocument(String productType, String title) {
//        return String.format("""
//            {
//                "ProductType": "%s",
//                "title": "%s"
//            }
//            """, productType, title);
//    }
//}