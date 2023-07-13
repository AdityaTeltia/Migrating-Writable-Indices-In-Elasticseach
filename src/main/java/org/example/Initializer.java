package org.example;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class Initializer {
    private static final Logger logger = LoggerFactory.getLogger(Initializer.class);

    public static void initializeIndices(RestHighLevelClient client, String[] indexNames) throws IOException {
        for (String indexName : indexNames) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
            CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            logger.info("Index {} created with response: {}", indexName, createIndexResponse);
        }
    }

    public static void addDocuments(RestHighLevelClient client, String[] indexNames) throws IOException {
        for (String indexName : indexNames) {
            for (int docId = 1; docId <= 4; docId++) {
                IndexRequest indexRequest = new IndexRequest(indexName)
                        .id(String.valueOf(docId))
                        .source(XContentType.JSON,
                                "name", "Document " + docId,
                                "id", docId);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info("Document added to index {}: {}", indexName, indexResponse);
            }
        }
    }

    public static void refreshIndices(RestHighLevelClient client, String[] indexNames) throws IOException {
        for (String indexName : indexNames) {
            RefreshRequest refreshRequest = new RefreshRequest(indexName);
            client.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
            logger.info("Index {} refreshed", indexName);
        }
    }

    public static void initialise(RestHighLevelClient client) throws IOException {
        // Create indices
        String[] indexNames = {"test_1", "test_2", "test_3", "test_4"};
        initializeIndices(client, indexNames);

        // Add documents
        addDocuments(client, indexNames);

        // Refresh indices to make the documents searchable
        refreshIndices(client, indexNames);
    }
}
