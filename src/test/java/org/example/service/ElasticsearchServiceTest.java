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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ElasticsearchServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchServiceTest.class);

    @Test
    void testBasicIndexing() throws IOException {
        try {
            // Setup mocks
            RestHighLevelClient client = mock(RestHighLevelClient.class);
            RestClient lowLevelClient = mock(RestClient.class);
            Response response = mock(Response.class);
            IndicesClient indicesClient = mock(IndicesClient.class);
            BulkResponse bulkResponse = mock(BulkResponse.class);
            CreateIndexResponse createIndexResponse = mock(CreateIndexResponse.class);

            // Setup client responses
            lenient().when(client.getLowLevelClient()).thenReturn(lowLevelClient);
            lenient().when(client.indices()).thenReturn(indicesClient);
            lenient().when(client.bulk(any(BulkRequest.class), any(RequestOptions.class))).thenReturn(bulkResponse);

            // Setup bulk response
            lenient().when(bulkResponse.hasFailures()).thenReturn(false);
            lenient().when(bulkResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));

            // Setup indices operations with explicit type
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

            // Verify with explicit type and correct number of invocations
            verify(client).bulk(any(BulkRequest.class), any(RequestOptions.class));
            verify(indicesClient, times(2)).exists(any(GetIndexRequest.class), any(RequestOptions.class));
            verify(indicesClient).create(any(CreateIndexRequest.class), any(RequestOptions.class));
        } catch (Exception e) {
            logger.error("Test failed with exception", e);
            throw e;
        }
    }
} 