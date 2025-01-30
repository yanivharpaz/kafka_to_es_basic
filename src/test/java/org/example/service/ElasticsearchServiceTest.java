package org.example.service;

import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.ProtocolVersion;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ElasticsearchServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchServiceTest.class);

    private RestHighLevelClient client;
    private RestClient lowLevelClient;
    private Response response;
    private IndicesClient indicesClient;
    private BulkResponse bulkResponse;
    private CreateIndexResponse createIndexResponse;

    @BeforeEach
    void setUp() throws IOException {
        // Setup mocks
        client = mock(RestHighLevelClient.class);
        lowLevelClient = mock(RestClient.class);
        response = mock(Response.class);
        indicesClient = mock(IndicesClient.class);
        bulkResponse = mock(BulkResponse.class);
        createIndexResponse = mock(CreateIndexResponse.class);

        // Setup client responses
        lenient().when(client.getLowLevelClient()).thenReturn(lowLevelClient);
        lenient().when(client.indices()).thenReturn(indicesClient);
        lenient().when(client.bulk(any(BulkRequest.class), any(RequestOptions.class))).thenReturn(bulkResponse);

        // Setup bulk response
        lenient().when(bulkResponse.hasFailures()).thenReturn(false);
        lenient().when(bulkResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));

        // Setup indices operations
        lenient().when(indicesClient.exists(any(GetIndexRequest.class), any(RequestOptions.class))).thenReturn(false);
        lenient().when(createIndexResponse.isAcknowledged()).thenReturn(true);
        lenient().when(indicesClient.create(any(CreateIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(createIndexResponse);

        // Setup response
        lenient().when(response.getStatusLine())
            .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));

        String aliasJson = """
            {
                "test_index": {
                    "aliases": {
                        "prd_a_book": {}
                    }
                }
            }""";
        lenient().when(response.getEntity())
            .thenReturn(new StringEntity(aliasJson, "UTF-8"));

        lenient().when(lowLevelClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            logger.info("Received request: {} {}", request.getMethod(), request.getEndpoint());
            return response;
        });
    }

    @Test
    void testSingleDocumentIndexing() throws IOException {
        // Create service
        ElasticsearchService service = new ElasticsearchService(client);

        // Test document
        String testDoc = """
            {
                "ProductType": "book",
                "title": "Test Book"
            }
            """;

        // Perform test
        service.indexDocument(testDoc);

        // Verify
        verify(client).bulk(any(BulkRequest.class), any(RequestOptions.class));
        verify(indicesClient, times(2)).exists(any(GetIndexRequest.class), any(RequestOptions.class));
        verify(indicesClient).create(any(CreateIndexRequest.class), any(RequestOptions.class));
    }

    @Test
    void testBatchProcessing() throws IOException {
        // Create service with small batch size
        ElasticsearchService service = new ElasticsearchService(client, 2, 1000);

        // Create and process multiple documents
        service.indexDocument(createTestDocument("book", "Test Book 1"));
        service.indexDocument(createTestDocument("book", "Test Book 2")); // This triggers first batch
        service.indexDocument(createTestDocument("book", "Test Book 3")); // This triggers second batch immediately

        // Verify batch processing
        verify(client, times(3)).bulk(any(BulkRequest.class), any(RequestOptions.class));
        ArgumentCaptor<BulkRequest> bulkRequestCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        verify(client, times(3)).bulk(bulkRequestCaptor.capture(), any(RequestOptions.class));

        // Log the captured requests
        List<BulkRequest> capturedRequests = bulkRequestCaptor.getAllValues();
        for (int i = 0; i < capturedRequests.size(); i++) {
            BulkRequest request = capturedRequests.get(i);
            logger.info("Batch {} details:", i);
            logger.info("  Number of actions: {}", request.numberOfActions());
            logger.info("  Estimated size in bytes: {}", request.estimatedSizeInBytes());
            logger.info("  Request content: {}", request.requests());
        }

        // Verify index creation and alias operations
        verify(indicesClient, times(6)).exists(any(GetIndexRequest.class), any(RequestOptions.class));
        verify(indicesClient, times(3)).create(any(CreateIndexRequest.class), any(RequestOptions.class));
        verify(lowLevelClient, atLeast(3)).performRequest(any(Request.class));

        // Verify bulk response handling
        verify(bulkResponse, times(3)).hasFailures();
        verify(bulkResponse, times(3)).getTook();
    }

    @Test
    void testBatchFlushOnTimeout() throws IOException, InterruptedException {
        // Create service with small timeout
        ElasticsearchService service = new ElasticsearchService(client, 10, 100); // 100ms timeout

        // Index first document
        service.indexDocument(createTestDocument("book", "Test Book 1"));
        
        // Wait for timeout
        Thread.sleep(150);
        
        // Index second document
        service.indexDocument(createTestDocument("book", "Test Book 2"));

        // Verify separate flushes
        verify(client, times(2)).bulk(any(BulkRequest.class), any(RequestOptions.class));
    }

    @Test
    void testHandlingFailedBulkResponse() throws IOException {
        // Setup failed bulk response
        when(bulkResponse.hasFailures()).thenReturn(true);
        when(bulkResponse.buildFailureMessage()).thenReturn("Test failure message");

        // Create service
        ElasticsearchService service = new ElasticsearchService(client);

        // Test document
        String testDoc = createTestDocument("book", "Test Book");

        // Verify no exception is thrown but failure is logged
        assertDoesNotThrow(() -> service.indexDocument(testDoc));
    }

    private String createTestDocument(String productType, String title) {
        return String.format("""
            {
                "ProductType": "%s",
                "title": "%s"
            }
            """, productType, title);
    }
} 