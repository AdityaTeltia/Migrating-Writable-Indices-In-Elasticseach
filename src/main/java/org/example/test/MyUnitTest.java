package org.example.test;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.example.src.Initializer;
import org.example.src.Migration;
import org.example.src.Utils.DocumentUtils;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

class MyUnitTest {
    private static final String sourceHost = "http://localhost:9200";
    private static final String destHost = "http://localhost:9201";
    // S3_repository
    private static final String sourceRepository = "Snapshot_S3";
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
            DocumentUtils.updateDocuments(sourceClient, sourceIndex , "1");

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
                DocumentUtils.updateDocuments(sourceClient, sourceIndex, null);
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
                DocumentUtils.updateDocuments(destClient, destIndex, null);
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
}

