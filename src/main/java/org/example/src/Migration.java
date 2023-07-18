package org.example.src;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestHighLevelClient;
import org.example.src.Utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Migration {
    private static final int BATCH_SIZE = 2;
    private static final Logger logger = LoggerFactory.getLogger(Migration.class);
    public static double phaseOneMigrateIndices(RestHighLevelClient sourceClient, RestHighLevelClient destClient,
                                              String sourceIndex, String sourceRepository,
                                              String destIndex) throws IOException {
        try {
            // Step 0: Pre-migration checks
            Checks.preMigrationCheck(destClient, sourceClient, sourceIndex, destIndex);

            //Step 1: Get Last Sequence Number
            double maxSeqNoValue = DocumentUtils.getLastSequenceNumber(sourceClient, sourceIndex);

            // Step 2: Snapshot and restore
            String snapshotName = SnapshotUtils.snaps(sourceClient, sourceRepository, sourceIndex, destClient, destIndex);

            // Step 2.1: Verifying snapshot and restore
            Checks.verifyDocumentsCount(sourceClient, sourceIndex, destClient, destIndex, true);

            // Deleting snapshot to release memory
            CleanupUtils.deleteSnapshot(sourceClient, sourceRepository, snapshotName);

            return maxSeqNoValue;
        } catch (ElasticsearchException e) {
            logger.error("Migration failed: ", e);
            throw e;
        }
    }

    public static void phaseTwoMigrateIndices(RestHighLevelClient destClient, RestHighLevelClient sourceClient, String sourceHost,
                                              String sourceIndex, String destIndex, double maxSeqNoValue)
            throws IOException {
        try {
            // Step 1: Reindexing new index
            ReindexUtils.reindexData(destClient, sourceHost, sourceIndex, destIndex, maxSeqNoValue);

            // Step 1.1: Post second pass verification
            Checks.verifyDocumentsCount(sourceClient, sourceIndex, destClient, destIndex, false);

            // Step 2: Cleanup [ Can delete or Can make the index read only ]
            CleanupUtils.makeIndexReadOnly(sourceClient, sourceIndex);
//            CleanupUtils.deleteIndex(sourceClient, sourceIndex);
        } catch (ElasticsearchException e) {
            logger.error("Migration failed: ", e);
            throw e;
        }
    }

    public static void migrateIndices(RestHighLevelClient sourceClient, RestHighLevelClient destClient, String sourceHost, List<String> sourceIndices,
                                      List<String> destIndices, String sourceRepository) throws IOException, InterruptedException {
        try {
            HashMap<String , Double> sequenceMap = new HashMap<>();
            for (int i = 0; i < sourceIndices.size(); i += BATCH_SIZE) {
                List<String> batchSourceIndices = sourceIndices.subList(i,
                        Math.min(i + BATCH_SIZE, sourceIndices.size()));
                List<String> batchDestIndices = destIndices.subList(i,
                        Math.min(i + BATCH_SIZE, destIndices.size()));

                logger.info("Migrating batch {}: {} to {}", ((i / BATCH_SIZE) + 1),
                        batchSourceIndices, batchDestIndices);
                ExecutorService executorService = Executors.newFixedThreadPool(BATCH_SIZE);
                List<Callable<Void>> tasks = new ArrayList<>();
                for (int j = 0; j < BATCH_SIZE; j++) {
                    final String sourceIndex = batchSourceIndices.get(j);
                    final String destIndex = batchDestIndices.get(j);

                    Callable<Void> task = () -> {
                        double maxSeqNoValue = 0;
                        try {
                            maxSeqNoValue = phaseOneMigrateIndices(sourceClient, destClient, sourceIndex, sourceRepository, destIndex);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        sequenceMap.put(sourceIndex, maxSeqNoValue);
                        return null;
                    };
                    tasks.add(task);
                }

                // Execute all tasks concurrently
                executorService.invokeAll(tasks);
                executorService.shutdown();

            }
            logger.info("First Phase Completed!");
//              ---------------------------------- SHIFT OPERATIONS -----------------------------------------------

            Scanner scanner = new Scanner(System.in);
            System.out.print("Do you want to run phase two? (Yes/No): ");
            String input = scanner.nextLine().trim();
            scanner.close();

            if (!input.equalsIgnoreCase("Yes")) {
                System.out.println("Phase two skipped.");
                return;
            }

            for(int i = 0;i<sourceIndices.size();i++){
                String sourceIndex = sourceIndices.get(i);
                String destIndex = destIndices.get(i);
                double maxSeqNoValue = sequenceMap.get(sourceIndex);
                phaseTwoMigrateIndices(destClient, sourceClient, sourceHost, sourceIndex, destIndex, maxSeqNoValue);
            }

            logger.info("All indices migrated successfully!");
        } catch (ElasticsearchException | InterruptedException e) {
            logger.error("Migration failed: ", e);
            throw e;
        }
    }
}
