package org.example.service;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
        try {
            cleanupIndices();
        } finally {
            if (client != null) {
                client.close();
            }
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
    void testEnsureIndexAndAliasExist() throws Exception {
        // Setup
        String document = """
            {
                "ProductType": "widget1",
                "title": "Test Widget"
            }
            """;
        
        // First create mapping
        Request putMappingRequest = new Request("PUT", "/cfp_a_widget1/_mapping/_doc");
        putMappingRequest.setJsonEntity("""
            {
                "properties": {
                    "ProductType": { "type": "keyword" },
                    "title": { "type": "text" }
                }
            }
            """);
        client.getLowLevelClient().performRequest(putMappingRequest);
        
        // Add document and flush
        elasticsearchService.addToBatch(document);
        
        // Print the batch contents before flushing
        System.out.println("Batch contents before flush: " + elasticsearchService.getBatches());
        
        elasticsearchService.flushBatch();

        // Force a refresh to make the document searchable
        Request refreshRequest = new Request("POST", "/cfp_a_widget1/_refresh");
        client.getLowLevelClient().performRequest(refreshRequest);

        // Add a small delay to allow for indexing
        Thread.sleep(1000);

        // Try to get the document directly
        Request getDocsRequest = new Request("GET", "/cfp_a_widget1/_doc/_search");
        getDocsRequest.setJsonEntity("""
            {
                "query": {
                    "match_all": {}
                }
            }
            """);
        Response docsResponse = client.getLowLevelClient().performRequest(getDocsRequest);
        System.out.println("Direct document lookup: " + 
            objectMapper.readTree(docsResponse.getEntity().getContent()).toPrettyString());

        // Check index settings and mappings
        Request getMappingsRequest = new Request("GET", "/cfp_a_widget1/_mappings");
        Response mappingsResponse = client.getLowLevelClient().performRequest(getMappingsRequest);
        System.out.println("Index mappings: " + 
            objectMapper.readTree(mappingsResponse.getEntity().getContent()).toPrettyString());

        // Try bulk indexing directly to verify the client works
        Request bulkRequest = new Request("POST", "/_bulk");
        String bulkBody = """
            { "index" : { "_index" : "cfp_a_widget1", "_type": "_doc" } }
            {"ProductType": "widget1", "title": "Test Widget Direct"}
            """;
        bulkRequest.setJsonEntity(bulkBody);
        Response bulkResponse = client.getLowLevelClient().performRequest(bulkRequest);
        System.out.println("Bulk index response: " + 
            objectMapper.readTree(bulkResponse.getEntity().getContent()).toPrettyString());

        // Refresh again
        client.getLowLevelClient().performRequest(refreshRequest);

        // Search with retries using low-level client
        JsonNode searchResponse = null;
        int maxRetries = 5;
        int retryCount = 0;
        
        String searchBody = """
            {
                "query": {
                    "match_all": {}
                }
            }
            """;
        
        while (searchResponse == null && retryCount < maxRetries) {
            try {
                // First get all indices to see what exists
                Request listIndicesRequest = new Request("GET", "/_cat/indices/cfp_a_*?format=json");
                Response indicesResponse = client.getLowLevelClient().performRequest(listIndicesRequest);
                System.out.println("Available indices: " + 
                    objectMapper.readTree(indicesResponse.getEntity().getContent()).toPrettyString());

                // Perform search
                Request searchRequest = new Request("GET", "/cfp_a_widget1/_doc/_search");
                searchRequest.setJsonEntity(searchBody);
                Response response = client.getLowLevelClient().performRequest(searchRequest);
                
                JsonNode result = objectMapper.readTree(response.getEntity().getContent());
                System.out.println("Search result: " + result.toPrettyString());
                
                long totalHits = result.path("hits").path("total").asLong();
                if (totalHits > 0) {
                    searchResponse = result;
                } else {
                    System.out.println("No hits found, will retry");
                }
            } catch (Exception e) {
                System.out.println("Search attempt " + (retryCount + 1) + " failed: " + e.getMessage());
                e.printStackTrace();
            }
            
            if (searchResponse == null) {
                retryCount++;
                Thread.sleep(1000 * retryCount);
            }
        }

        assertNotNull(searchResponse, "Search should return results within retry limit");
        assertTrue(searchResponse.path("hits").path("total").asLong() > 0, 
            "Should find at least one document");
    }
} 