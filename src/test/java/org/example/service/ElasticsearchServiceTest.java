package org.example.service;

import org.apache.http.HttpHost;
import org.apache.http.ProtocolVersion;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicStatusLine;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

@ExtendWith(MockitoExtension.class)
class ElasticsearchServiceTest {

    @Mock
    private RestHighLevelClient client;

    @Mock
    private RestClient lowLevelClient;

    @Mock
    private IndicesClient indicesClient;

    @Mock
    private Response response;

    private ElasticsearchService elasticsearchService;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() throws IOException {
        objectMapper = new ObjectMapper();
        
        // Create and configure mocks
        RestClient mockRestClient = mock(RestClient.class);
        lenient().when(client.getLowLevelClient()).thenReturn(mockRestClient);
        lenient().when(client.indices()).thenReturn(indicesClient);
        
        // Mock index existence check
        lenient().when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(false);
        
        // Mock index creation
        CreateIndexResponse createIndexResponse = mock(CreateIndexResponse.class);
        lenient().when(createIndexResponse.isAcknowledged()).thenReturn(true);
        lenient().when(indicesClient.create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(createIndexResponse);
        
        // Mock alias operations
        Response aliasResponse = mock(Response.class);
        lenient().when(aliasResponse.getStatusLine()).thenReturn(new BasicStatusLine(
            new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
        lenient().when(aliasResponse.getEntity()).thenReturn(new StringEntity("{}"));
        
        // Mock low level client responses
        lenient().when(mockRestClient.performRequest(any(Request.class))).thenReturn(aliasResponse);
        
        // Mock bulk operations
        BulkResponse bulkResponse = mock(BulkResponse.class);
        lenient().when(bulkResponse.hasFailures()).thenReturn(false);
        lenient().when(bulkResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        lenient().when(client.bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(bulkResponse);
        
        // Create service instance after all mocks are configured
        elasticsearchService = new ElasticsearchService(client);
    }

    @Test
    void testEnsureIndexAndAliasExist() throws IOException {
        // Mock successful responses
        Response successResponse = mock(Response.class);
        lenient().when(successResponse.getStatusLine()).thenReturn(new BasicStatusLine(
            new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
        lenient().when(successResponse.getEntity()).thenReturn(new StringEntity("{}"));
        
        // Mock the low level client for alias operations
        lenient().when(client.getLowLevelClient().performRequest(any(Request.class)))
            .thenReturn(successResponse);

        // Mock index existence check
        lenient().when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(false);

        // Execute - use addToBatch which will trigger index/alias creation
        String document = """
            {
                "ProductType": "widget1",
                "title": "Test Widget"
            }
            """;
        elasticsearchService.addToBatch(document);

        // Verify index creation and alias operations were performed
        verify(indicesClient).create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT));
        verify(client.getLowLevelClient()).performRequest(argThat(request -> 
            request.getEndpoint().contains("_aliases")));
    }

    @Test
    void testIndexCreationFailure() throws IOException {
        doReturn(false).when(indicesClient).exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT));

        doThrow(new IOException("Failed to create index"))
            .when(indicesClient).create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT));

        String testDoc = """
            {
                "ProductType": "widget1",
                "title": "Test Widget"
            }
            """;

        assertThrows(IOException.class, () -> elasticsearchService.indexDocument(testDoc));
    }

    @Test
    void testAliasCreationFailure() throws IOException {
        // Mock failure response
        Response failureResponse = mock(Response.class);
        when(failureResponse.getStatusLine()).thenReturn(new BasicStatusLine(
            new ProtocolVersion("HTTP", 1, 1), 500, "Internal Server Error"));
        
        lenient().when(client.getLowLevelClient().performRequest(argThat(request -> 
            request.getEndpoint().contains("_aliases"))))
            .thenReturn(failureResponse);

        String document = """
            {
                "ProductType": "widget1",
                "title": "Test Widget"
            }
            """;

        assertThrows(IOException.class, () -> {
            elasticsearchService.addToBatch(document);
        }, "Should throw IOException when alias creation fails");
    }

    @Test
    void testReuseExistingIndexAndAlias() throws IOException {
        lenient().when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(true);

        Response aliasExistsResponse = mock(Response.class);
        lenient().when(aliasExistsResponse.getStatusLine()).thenReturn(new BasicStatusLine(
            new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
        lenient().when(lowLevelClient.performRequest(argThat(request -> 
            request.getMethod().equals("GET") && 
            request.getEndpoint().startsWith("/_alias/cfp_a_")))).thenReturn(aliasExistsResponse);

        String testDoc = """
            {
                "ProductType": "widget1",
                "title": "Test Widget"
            }
            """;

        elasticsearchService.indexDocument(testDoc);

        verify(indicesClient, never()).create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT));
        verify(lowLevelClient, never()).performRequest(argThat(request -> 
            request.getMethod().equals("POST") && 
            request.getEndpoint().equals("/_aliases")));
    }

    @Test
    void shouldFlush_WhenBatchSizeReachesLimit() throws IOException {
        // Setup - add documents just under the limit
        String document = """
            {
                "ProductType": "widget1",
                "title": "Test Widget"
            }
            """;
        
        // Reset any existing state
        elasticsearchService.flushBatch();
        
        // Add documents up to limit-1 (batch size is 50)
        for (int i = 0; i < 49; i++) {
            elasticsearchService.addToBatch(document);
        }
        
        // Verify not flushing before limit
        assertFalse(elasticsearchService.shouldFlush("widget1"), 
            "Should not flush before reaching limit");
        
        // Add one more document to reach limit
        elasticsearchService.addToBatch(document);
        
        // Verify flush needed at limit
        assertTrue(elasticsearchService.shouldFlush("widget1"), 
            "Should flush when batch size reaches limit of 50");
    }

    @Test
    void shouldFlush_WhenTimeIntervalReached() throws Exception {
        // Setup
        String document = """
            {
                "ProductType": "widget1",
                "title": "Test Widget"
            }
            """;
        
        // Reset any existing state
        elasticsearchService.flushBatch();
        
        // Add a document and record the time
        elasticsearchService.addToBatch(document);
        assertFalse(elasticsearchService.shouldFlush("widget1"), 
            "Should not flush immediately");
        
        // Wait just over the interval
        Thread.sleep(7500); // Wait 7.5 seconds to ensure we're past the 7 second mark
        
        assertTrue(elasticsearchService.shouldFlush("widget1"), 
            "Should flush after 7 seconds");
    }

    @Test
    void addToBatch_ShouldCreateNewBatchIfProductTypeNotExists() throws IOException {
        String document = """
            {
                "ProductType": "widget1",
                "title": "Test Widget"
            }
            """;

        elasticsearchService.addToBatch(document);

        Map<String, StringBuilder> batches = elasticsearchService.getBatches();
        assertTrue(batches.containsKey("widget1"), "Should create new batch for widget1");
        assertTrue(batches.get("widget1").toString().contains("Test Widget"), 
            "Batch should contain document");
    }

    @Test
    void addToBatch_ShouldAppendToExistingBatch() throws IOException {
        String document1 = """
            {
                "ProductType": "widget1",
                "title": "Test Widget 1"
            }
            """;
        String document2 = """
            {
                "ProductType": "widget1",
                "title": "Test Widget 2"
            }
            """;

        elasticsearchService.addToBatch(document1);
        elasticsearchService.addToBatch(document2);

        Map<String, StringBuilder> batches = elasticsearchService.getBatches();
        String batchContent = batches.get("widget1").toString();
        assertTrue(batchContent.contains("Test Widget 1"), "Batch should contain first document");
        assertTrue(batchContent.contains("Test Widget 2"), "Batch should contain second document");
    }

    @Test
    void addDocumentToBatch_ShouldIncrementCounter() throws IOException {
        JsonNode document = objectMapper.readTree("""
            {
                "ProductType": "widget1",
                "title": "Test Widget"
            }
            """);

        elasticsearchService.addDocumentToBatch(document);

        assertEquals(1, elasticsearchService.getDocumentCount("widget1"), 
            "Document count should be incremented");
    }

    @Test
    void flushBatch_ShouldClearBatchAndResetCounter() throws IOException {
        // Setup
        String document = """
            {
                "ProductType": "widget1",
                "title": "Test Widget"
            }
            """;
        elasticsearchService.addToBatch(document);

        // Create fresh mocks for this test
        BulkResponse bulkResponse = mock(BulkResponse.class);
        when(bulkResponse.hasFailures()).thenReturn(false);
        
        // Remove any existing bulk mock from setup
        reset(client);
        when(client.bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(bulkResponse);

        // Act
        elasticsearchService.flushBatch();

        // Verify bulk request was made
        verify(client).bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT));

        // Assert
        assertTrue(elasticsearchService.getBatches().isEmpty(), "Batches should be empty after flush");
        assertEquals(0, elasticsearchService.getDocumentCount("widget1"), 
            "Document count should be reset after flush");
    }

    @Test
    void initializeAliasCache_ShouldPopulateCache() throws IOException {
        elasticsearchService.initializeAliasCache();
    }
}