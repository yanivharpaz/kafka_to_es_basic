package org.example.service;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RequestOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class ElasticsearchServiceIntegrationTest {

    private RestHighLevelClient client;
    private ElasticsearchService elasticsearchService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private String testIndexName;
    private String testAliasName;

    @BeforeEach
    void setUp() throws IOException {
        // Create a client connected to your Elasticsearch instance
        client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );
        
        // Clean up any existing indices first
        cleanupIndices();
        
        elasticsearchService = new ElasticsearchService(client);
        
        // Generate unique index and alias names for this test run
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        testIndexName = "test_index_" + timestamp;
        testAliasName = "test_alias_" + timestamp;
    }

    @AfterEach
    void tearDown() throws IOException {
        cleanupIndices();
        if (client != null) {
            client.close();
        }
    }

    private void cleanupIndices() {
        try {
            // First, get all indices that start with cfp_a_
            Request getIndicesRequest = new Request("GET", "/_cat/indices/cfp_a_*?format=json");
            Response indicesResponse = client.getLowLevelClient().performRequest(getIndicesRequest);
            JsonNode indices = objectMapper.readTree(indicesResponse.getEntity().getContent());
            
            // Delete each index and its aliases
            for (JsonNode index : indices) {
                String indexName = index.get("index").asText();
                
                // Delete aliases first
                Request getAliasRequest = new Request("GET", "/" + indexName + "/_alias/*");
                Response aliasResponse = client.getLowLevelClient().performRequest(getAliasRequest);
                JsonNode aliases = objectMapper.readTree(aliasResponse.getEntity().getContent());
                
                // Delete the index itself
                Request deleteRequest = new Request("DELETE", "/" + indexName);
                client.getLowLevelClient().performRequest(deleteRequest);
                
                System.out.println("Cleaned up index: " + indexName);
            }
        } catch (IOException e) {
            System.err.println("Error during cleanup: " + e.getMessage());
        }
    }

    private JsonNode searchWithRetry(String indexName, String searchBody, int maxRetries) throws IOException {
        for (int i = 0; i < maxRetries; i++) {
            // Refresh the index
            Request refreshRequest = new Request("POST", "/" + indexName + "/_refresh");
            client.getLowLevelClient().performRequest(refreshRequest);

            // Perform the search
            Request getDocRequest = new Request("GET", "/" + indexName + "/_search");
            getDocRequest.setJsonEntity(searchBody);
            Response docResponse = client.getLowLevelClient().performRequest(getDocRequest);
            JsonNode searchResult = objectMapper.readTree(docResponse.getEntity().getContent());
            
            System.out.println("Search attempt " + (i + 1) + " result: " + searchResult.toPrettyString());
            
            // Check if we have any hits
            JsonNode hits = searchResult.path("hits").path("hits");
            if (hits.isArray() && hits.size() > 0) {
                return searchResult;
            }

            // Wait between retries
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return null;
    }

    @Test
    void testEnsureIndexAndAliasExist() throws IOException {
        // Create a test document
        String testDoc = """
            {
                "ProductType": "widget1",
                "title": "Test Widget"
            }
            """;

        // Index the document which will trigger index and alias creation
        elasticsearchService.indexDocument(testDoc);
        
        // Flush immediately to ensure the index is created
        elasticsearchService.flushBatch();
        
        // Add a delay to ensure Elasticsearch has processed the document
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Add refresh to ensure the index is ready
        Request refreshRequest = new Request("POST", "/cfp_a_widget1/_refresh");
        client.getLowLevelClient().performRequest(refreshRequest);

        // Get the actual index name from the alias
        Request getAliasRequest = new Request("GET", "/_alias/cfp_a_widget1");
        Response aliasResponse = client.getLowLevelClient().performRequest(getAliasRequest);
        
        // Parse the response to get the actual index name
        JsonNode aliasJson = objectMapper.readTree(aliasResponse.getEntity().getContent());
        System.out.println("Alias response: " + aliasJson.toPrettyString());
        
        String actualIndexName = aliasJson.fieldNames().next();
        System.out.println("Actual index name: " + actualIndexName);
        
        // Verify index exists using the actual index name
        GetIndexRequest getIndexRequest = new GetIndexRequest(actualIndexName);
        boolean indexExists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        assertTrue(indexExists, "Index should exist");

        // Verify the alias is pointing to the correct index
        boolean aliasFound = false;
        for (JsonNode indexNode : aliasJson) {
            if (indexNode.has("aliases") && 
                indexNode.get("aliases").has("cfp_a_widget1")) {
                aliasFound = true;
                break;
            }
        }
        assertTrue(aliasFound, "Alias should exist and point to the correct index");

        // Verify the document was indexed with a specific query
        String searchBody = """
            {
                "query": {
                    "match_all": {}
                }
            }""";

        // Try searching with more retries and longer delay
        JsonNode searchResult = searchWithRetry(actualIndexName, searchBody, 10);
        assertNotNull(searchResult, "Search should return results within retry limit");
        
        // Print the full response for debugging
        System.out.println("Final search response: " + searchResult.toPrettyString());
        
        // Get the hits array directly
        JsonNode hits = searchResult.path("hits").path("hits");
        assertTrue(hits.isArray(), "Hits should be an array");
        assertTrue(hits.size() > 0, "Should have at least one hit");
        
        // Get the first hit
        JsonNode firstHit = hits.get(0);
        JsonNode source = firstHit.path("_source");
        
        // Verify the content of the found document
        assertEquals("Test Widget", source.path("title").asText(), "Document should have correct title");
        assertEquals("widget1", source.path("ProductType").asText(), "Document should have correct ProductType");

        // Verify document exists
        Request countRequest = new Request("GET", "/cfp_a_widget1/_count");
        Response countResponse = client.getLowLevelClient().performRequest(countRequest);
        JsonNode countResult = objectMapper.readTree(countResponse.getEntity().getContent());
        System.out.println("Document count after indexing: " + countResult.toPrettyString());
    }
} 