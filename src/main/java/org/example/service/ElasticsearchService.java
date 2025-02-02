package org.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonParseException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.example.model.IndexInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Instant;

public class ElasticsearchService implements AutoCloseable {
    private final RestHighLevelClient client;
    private final Map<String, String> aliasCache;
    private final Map<String, Integer> indexCounters;
    private static final String INDEX_PREFIX = "cfp_a_";
    
    // Batch configuration
    private static final int DEFAULT_BATCH_SIZE = 10;
    private static final long DEFAULT_FLUSH_INTERVAL_MS = 5000; // 5 seconds
    private final int batchSize;
    private final long flushIntervalMs;
    private final BulkRequest currentBatch;
    private final AtomicInteger batchCount;
    private volatile Instant lastFlushTime;

    public ElasticsearchService(RestHighLevelClient client) {
        this(client, DEFAULT_BATCH_SIZE, DEFAULT_FLUSH_INTERVAL_MS);
    }

    public ElasticsearchService(RestHighLevelClient client, int batchSize, long flushIntervalMs) {
        this.client = client;
        this.aliasCache = new ConcurrentHashMap<>();
        this.indexCounters = new ConcurrentHashMap<>();
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.currentBatch = new BulkRequest();
        this.batchCount = new AtomicInteger(0);
        this.lastFlushTime = Instant.now();
        initializeAliasCache();
    }

    private String getIndexName(String productType) {
        return String.format("%s%s_%05d", INDEX_PREFIX, productType, 1);
    }

    private String getAliasName(String productType) {
        return String.format("%s%s", INDEX_PREFIX, productType);
    }

    public void processSinkRecords(Collection<SinkRecord> records) {
        try {
            for (SinkRecord record : records) {
                if (record.value() != null) {
                    addToBatch(record.value().toString());
                }
            }
            // Flush any remaining records in the batch
            flushBatch();
        } catch (Exception e) {
            System.err.println("Error processing batch: " + e.getMessage());
            throw new RuntimeException("Failed to process batch", e);
        }
    }

    public void addToBatch(String message) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode jsonNodes = mapper.readTree(message);
            
            if (jsonNodes.isArray()) {
                for (JsonNode node : jsonNodes) {
                    addDocumentToBatch(node);
                }
            } else {
                addDocumentToBatch(jsonNodes);
            }
        } catch (JsonParseException e) {
            System.err.println("Invalid JSON message: " + message);
            throw new IOException("Failed to parse JSON message", e);
        }
    }

    private boolean shouldFlush() {
        return batchCount.get() >= batchSize || 
               Instant.now().isAfter(lastFlushTime.plusMillis(flushIntervalMs));
    }

    private void addDocumentToBatch(JsonNode node) throws IOException {
        String productType = node.has("ProductType") ?
                node.get("ProductType").asText().toLowerCase() : "unknown";
        String aliasName = getAliasName(productType);

        ensureIndexAndAliasExist(productType);

        IndexRequest indexRequest = new IndexRequest(aliasName, "_doc")
                .id(UUID.randomUUID().toString())
                .source(node.toString(), XContentType.JSON);

        currentBatch.add(indexRequest);
        batchCount.incrementAndGet();

        if (shouldFlush()) {
            flushBatch();
        }
    }

    public void flushBatch() throws IOException {
        if (batchCount.get() > 0) {
            System.out.println("** Starting batch flush");
            try {
                BulkResponse bulkResponse = client.bulk(currentBatch, RequestOptions.DEFAULT);
                if (bulkResponse.hasFailures()) {
                    System.err.println("Bulk indexing has failures: " + bulkResponse.buildFailureMessage());
                }
                long timeTaken = bulkResponse.getTook().getMillis();
                System.out.printf("Indexed batch of %d documents in %d ms (%.2f docs/sec)%n", 
                    batchCount.get(), timeTaken, 
                    (batchCount.get() * 1000.0) / timeTaken);
                
                // Clear the batch
                currentBatch.requests().clear();
                batchCount.set(0);
                lastFlushTime = Instant.now();
            } catch (Exception e) {
                System.err.println("Error flushing batch: " + e.getMessage());
                throw new IOException("Failed to flush batch", e);
            }
            System.out.println("** batch flush complete");

        }
    }

    // Schedule periodic flush (optional, can be called in constructor if needed)
//    public void startPeriodicFlush() {
//        Thread flushThread = new Thread(() -> {
//            while (!Thread.currentThread().isInterrupted()) {
//                try {
//                    if (Instant.now().isAfter(lastFlushTime.plusMillis(flushIntervalMs))) {
//                        flushBatch();
//                    }
//                    Thread.sleep(Math.min(1000, flushIntervalMs / 2));
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                    break;
//                } catch (Exception e) {
//                    System.err.println("Error in periodic flush: " + e.getMessage());
//                }
//            }
//        });
//        flushThread.setDaemon(true);
//        flushThread.setName("ES-Batch-Flush-Thread");
//        flushThread.start();
//    }

    // For backward compatibility and single document indexing
    public void indexDocument(String message) throws IOException {
        addToBatch(message);
        flushBatch(); // Immediately flush for single document indexing
    }

    private void initializeAliasCache() {
        try {
            Response response = client.getLowLevelClient()
                    .performRequest(new Request("GET", "/_alias"));

            String responseBody = EntityUtils.toString(response.getEntity());
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> aliasMap = mapper.readValue(responseBody, Map.class);

            for (Map.Entry<String, Object> entry : aliasMap.entrySet()) {
                String indexName = entry.getKey();
                if (indexName.startsWith(INDEX_PREFIX)) {
                    try {
                        String[] parts = indexName.split("_");
                        if (parts.length >= 4) {
                            String productType = parts[parts.length - 2];
                            int indexNumber = Integer.parseInt(parts[parts.length - 1]);
                            indexCounters.merge(productType, indexNumber, Integer::max);
                        }
                    } catch (NumberFormatException e) {
                        continue;
                    }
                }

                @SuppressWarnings("unchecked")
                Map<String, Object> aliasInfo = (Map<String, Object>) entry.getValue();

                if (aliasInfo.containsKey("aliases")) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> aliases = (Map<String, Object>) aliasInfo.get("aliases");
                    for (String aliasName : aliases.keySet()) {
                        if (aliasName.startsWith(INDEX_PREFIX)) {
                            aliasCache.put(aliasName, indexName);
                            System.out.println("Cached alias mapping: " + aliasName + " -> " + indexName);
                        }
                    }
                }
            }
            System.out.println("Initialized alias cache with " + aliasCache.size() + " entries");
            System.out.println("Initialized index counters: " + indexCounters);
        } catch (Exception e) {
            System.err.println("Error initializing alias cache: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void ensureIndexAndAliasExist(String productType) throws IOException {
        String indexName = getIndexName(productType);
        String aliasName = getAliasName(productType);

        System.out.println("Checking index and alias existence for product type: " + productType);
        System.out.println("Index name: " + indexName + ", Alias name: " + aliasName);

        try {
            String existingIndex = aliasCache.get(aliasName);
            if (existingIndex != null) {
                System.out.println("Found existing alias in cache: " + aliasName + " -> " + existingIndex);

                GetIndexRequest verifyRequest = new GetIndexRequest(existingIndex);
                boolean indexExists = client.indices().exists(verifyRequest, RequestOptions.DEFAULT);
                if (!indexExists) {
                    System.out.println("Warning: Cached index " + existingIndex + " no longer exists. Will create new index and alias.");
                    aliasCache.remove(aliasName);
                } else {
                    System.out.println("Verified cached index exists. Using existing index and alias.");
                    return;
                }
            }

            GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
            boolean indexExists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);

            if (!indexExists) {
                createNewIndexAndAlias(indexName, aliasName);
            } else {
                verifyAndCreateAlias(indexName, aliasName);
            }
        } catch (Exception e) {
            System.err.println("Critical error in ensureIndexAndAliasExist: " + e.getMessage());
            e.printStackTrace();
            throw new IOException("Failed to ensure index and alias existence", e);
        }
    }

    private void createNewIndexAndAlias(String indexName, String aliasName) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.source("{\n" +
                "    \"settings\": {\n" +
                "        \"number_of_shards\": 3,\n" +
                "        \"number_of_replicas\": 1\n" +
                "    }\n" +
                "}", XContentType.JSON);

        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println("Successfully created index: " + indexName);

        createAlias(indexName, aliasName);
    }

    private void verifyAndCreateAlias(String indexName, String aliasName) throws IOException {
        Request checkAliasRequest = new Request("GET", "/_alias/" + aliasName);
        Response checkAliasResponse = client.getLowLevelClient().performRequest(checkAliasRequest);

        if (checkAliasResponse.getStatusLine().getStatusCode() == 404) {
            System.out.println("Alias doesn't exist. Creating alias: " + aliasName);
            createAlias(indexName, aliasName);
        } else {
            System.out.println("Alias " + aliasName + " already exists");
        }

        aliasCache.put(aliasName, indexName);
        System.out.println("Updated alias cache: " + aliasName + " -> " + indexName);
    }

    private void createAlias(String indexName, String aliasName) throws IOException {
        String aliasPayload = String.format("""
            {
                "actions": [
                    {
                        "add": {
                            "index": "%s",
                            "alias": "%s",
                            "is_write_index": true
                        }
                    }
                ]
            }""", indexName, aliasName);

        Request aliasRequest = new Request("POST", "/_aliases");
        aliasRequest.setJsonEntity(aliasPayload);

        Response aliasResponse = client.getLowLevelClient().performRequest(aliasRequest);
        int statusCode = aliasResponse.getStatusLine().getStatusCode();

        if (statusCode >= 200 && statusCode < 300) {
            System.out.println("Successfully created alias: " + aliasName);
            aliasCache.put(aliasName, indexName);
            System.out.println("Updated alias cache: " + aliasName + " -> " + indexName);
        } else {
            throw new IOException("Failed to create alias. Status code: " + statusCode);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            flushBatch(); // Ensure any remaining documents are flushed
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}