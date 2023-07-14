package org.example.test;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.example.src.Initializer;
import org.junit.jupiter.api.Test;
import org.example.src.Utils.*;
import org.example.src.Migration;


import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

class MyUnitTest {

    private static final String sourceHost = "http://localhost:9200";
    private static final String destHost = "http://localhost:9201";

    private static final String pipelineName = "adding_hidden_field";
    // S3_repository
    private static final String sourceRepository = "s3testing";
    private final int RANDOM_STRING_LENGTH = 10;
    private AtomicBoolean migrationCompleted = new AtomicBoolean(false);

    String sourceIndex = "test";
    String destIndex = "test";
    int threadCount = 9;

    @Test
    void testConcurrentOperations() throws InterruptedException, IOException {
        try (RestHighLevelClient sourceClient = new RestHighLevelClient(
                RestClient.builder(HttpHost.create(sourceHost)));
             RestHighLevelClient destClient = new RestHighLevelClient(
                     RestClient.builder(HttpHost.create(destHost)))) {

            // Phase One
            phaseOne(sourceClient, destClient);

            Initializer.refreshIndex(sourceClient, sourceIndex);
            Initializer.refreshIndex(destClient, destIndex);
            migrationCompleted.set(false);

            // Phase Two
            phaseTwo(sourceClient, destClient);

        } catch (IOException e) {
            throw e;
        }
    }

    void phaseOne(RestHighLevelClient sourceClient, RestHighLevelClient destClient) throws InterruptedException {
        // Create a thread pool with the desired number of threads
        ExecutorService executor = Executors.newFixedThreadPool(threadCount + 2);

        // Create a countdown latch to synchronize the threads
        CountDownLatch latch = new CountDownLatch(threadCount + 2);

        // Thread 1: Add documents
        executor.execute(() -> {
            try {
                addDocuments(sourceClient, sourceIndex);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                latch.countDown();
            }
        });

        // Thread 2: Update documents
        executor.execute(() -> {
            try {
                updateDocuments(sourceClient, sourceIndex);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                latch.countDown();
            }
        });

        // Thread 3: Phase One Migration
        executor.execute(() -> {
            try {
                Migration.phaseOneMigrateIndices(sourceClient, destClient, sourceIndex, pipelineName, sourceRepository, destIndex);
                migrationCompleted.set(true);
                latch.countDown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // Threads 4-12: Perform the same functionality as Thread 1 and Thread 2
        for (int i = 0; i < threadCount; i++) {
            executor.execute(() -> {
                try {
                    addDocuments(sourceClient, sourceIndex); // Or perform any desired functionality
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            });
        }

        // Wait for all threads to complete
        latch.await();

        // Shutdown the executor
        executor.shutdown();
    }

    void phaseTwo(RestHighLevelClient sourceClient, RestHighLevelClient destClient) throws InterruptedException {
        // Create a thread pool with the desired number of threads
        ExecutorService executor = Executors.newFixedThreadPool(threadCount + 2);

        // Create a countdown latch to synchronize the threads
        CountDownLatch latch = new CountDownLatch(threadCount + 2);

        // Thread 1: Add documents to destination index
        executor.execute(() -> {
            try {
                addDocuments(destClient, destIndex);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                latch.countDown();
            }
        });

        // Thread 2: Update documents in destination index
        executor.execute(() -> {
            try {
                updateDocuments(destClient, destIndex);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                latch.countDown();
            }
        });

        // Thread 3: Phase Two Migration
        executor.execute(() -> {
            try {
                Migration.phaseTwoMigrateIndices(destClient, sourceClient, sourceHost, sourceIndex, destIndex);
                migrationCompleted.set(true);
                latch.countDown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // Threads 4-12: Perform the same functionality as Thread 1 and Thread 2
        for (int i = 0; i < threadCount; i++) {
            executor.execute(() -> {
                try {
                    addDocuments(destClient, destIndex); // Or perform any desired functionality
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            });
        }

        // Wait for all threads to complete
        latch.await();

        // Shutdown the executor
        executor.shutdown();
    }

    public void addDocuments(RestHighLevelClient client, String index) throws IOException, InterruptedException {
        while (!migrationCompleted.get()) {
            DocumentUtils.addDocuments(client, index);
            Thread.sleep(200);
        }
    }
    private String generateRandomString(int length) {
        StringBuilder stringBuilder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            stringBuilder.append(DocumentUtils.randomChar());
        }
        return stringBuilder.toString();
    }

    private void updateDocuments(RestHighLevelClient client, String index) throws IOException, InterruptedException {
        // Fetch a random document ID from the source index
        String randomDocumentId = getRandomDocumentId(client, index);

        // If a document ID is found
        if (randomDocumentId != null) {
            // Generate new data for the document
            String newName = generateRandomString(10);
            int newAge = 35;
            String newEmail = "updated@example.com";

            // Create the JSON document using XContentBuilder
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("name", newName);
            builder.field("age", newAge);
            builder.field("email", newEmail);
            builder.endObject();

            // Create the UpdateRequest and update the document in the index
            UpdateRequest updateRequest = new UpdateRequest(index, randomDocumentId)
                    .doc(builder);
            UpdateResponse updateResponse = client.update(updateRequest , RequestOptions.DEFAULT);
            Thread.sleep(200);
        }
    }

    // Helper function to fetch a random document ID from the Elasticsearch index
    private String getRandomDocumentId(RestHighLevelClient client, String index) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(1);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();

        // Check if there are any hits
        if (hits.getTotalHits().value > 0) {
            SearchHit randomHit = hits.getAt(new Random().nextInt(hits.getHits().length));
            return randomHit.getId();
        }
        return null; // No documents found
    }


}

