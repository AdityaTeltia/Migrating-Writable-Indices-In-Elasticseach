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

import java.io.ByteArrayInputStream;
import java.util.Scanner;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

class MyUnitTest {
    private static final String sourceHost = "http://localhost:9200";
    private static final String destHost = "http://localhost:9201";
    // S3_repository
    private static final String sourceRepository = "s3testing";
    private AtomicBoolean phaseCompleted = new AtomicBoolean(false);
    String sourceIndex = "test";
    String destIndex = "restored_test";
    int threadCount = 9;

    @Test
    void testConcurrentOperations() throws InterruptedException, IOException {
        try (RestHighLevelClient sourceClient = new RestHighLevelClient(
                RestClient.builder(HttpHost.create(sourceHost)));
             RestHighLevelClient destClient = new RestHighLevelClient(
                     RestClient.builder(HttpHost.create(destHost)))) {

            DocumentUtils.addDocuments(sourceClient , sourceIndex, "1");

            // Phase One
            double maxSeqNoValue = phaseOne(sourceClient, destClient).get();
            Initializer.refreshIndex(sourceClient, sourceIndex);
            Initializer.refreshIndex(destClient, destIndex);
            phaseCompleted.set(false);
            updateDocuments(sourceClient, sourceIndex , "1");

            // Input to either skip or run phase two
            String data = "Yes";
            System.setIn(new ByteArrayInputStream(data.getBytes()));

            try (Scanner scanner = new Scanner(System.in)) {
                System.out.println("Do you want to proceed with Phase Two? (Yes/No)");
                String userInput = scanner.nextLine().trim();
                if ("Yes".equalsIgnoreCase(userInput)) {
                    phaseCompleted.set(false);
                    // Phase Two
                    phaseTwo(sourceClient, destClient, maxSeqNoValue);
                } else {
                    System.out.println("Phase Two skipped as per user input.");
                }
            }
        } catch (IOException e) {
            throw e;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    Future<Double> phaseOne(RestHighLevelClient sourceClient, RestHighLevelClient destClient) throws InterruptedException, IOException {
        // Create a thread pool with the desired number of threads
        ExecutorService executor = Executors.newFixedThreadPool(threadCount + 2);

        // Create a countdown latch to synchronize the threads
        CountDownLatch latch = new CountDownLatch(threadCount + 2);

        // Use CompletableFuture to capture the result
        CompletableFuture<Double> maxSeqNoValueFuture = new CompletableFuture<>();

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
                updateDocuments(sourceClient, sourceIndex, null);
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
                double maxSeqNoValue = Migration.phaseOneMigrateIndices(sourceClient, destClient, sourceIndex, sourceRepository, destIndex);
                phaseCompleted.set(true);
                maxSeqNoValueFuture.complete(maxSeqNoValue); // Complete the CompletableFuture with the value
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
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

        // Return the Future containing the maxSeqNoValue
        return maxSeqNoValueFuture;
    }

    void phaseTwo(RestHighLevelClient sourceClient, RestHighLevelClient destClient, double maxSeqNoValue) throws InterruptedException {
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
                updateDocuments(destClient, destIndex, null);
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
                Migration.phaseTwoMigrateIndices(destClient, sourceClient, sourceHost, sourceIndex, destIndex, maxSeqNoValue);
                Thread.sleep(5000);
                phaseCompleted.set(true);
                latch.countDown();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
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
        while (!phaseCompleted.get()) {
            DocumentUtils.addDocuments(client, index, null);
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

    private void updateDocuments(RestHighLevelClient client, String index , String id) throws IOException, InterruptedException {
        // Fetch a random document ID from the source index
        String randomDocumentId = getRandomDocumentId(client, index);

        if(id != null){
            randomDocumentId = id;
        }

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

