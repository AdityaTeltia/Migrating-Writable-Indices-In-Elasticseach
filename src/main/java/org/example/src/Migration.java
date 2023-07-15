package org.example.src;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestHighLevelClient;
import org.example.src.Utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class Migration {
    private static final int BATCH_SIZE = 2;
    private static int count = 1;
    private static final Logger logger = LoggerFactory.getLogger(Migration.class);

    public static double phaseOneMigrateIndices(RestHighLevelClient sourceClient, RestHighLevelClient destClient,
                                              String sourceIndex, String pipelineName, String sourceRepository,
                                              String destIndex) throws IOException {
        try {
            // Step 0: Pre-migration checks
            Checks.preMigrationCheck(destClient, sourceClient, sourceIndex, destIndex);

            /*
            // Step 1: Setting mapping for the new field we are adding
            MappingUtils.setMapping(sourceClient, sourceIndex);

            // Step 1.1: Change write pipeline
            PipelineUtils.changePipeline(sourceClient, pipelineName, count, sourceIndex); */

            //Step 1: Get Last Sequence Number
            double maxSeqNoValue = DocumentUtils.getLastSequenceNumber(sourceClient, sourceIndex);

            // Step 2: Snapshot and restore
            String snapshotName = SnapshotUtils.snaps(sourceClient, sourceRepository, sourceIndex, destClient, destIndex);

            // Step 2.1: Verifying snapshot and restore
            Checks.verifyDocumentsCount(sourceClient, sourceIndex, destClient, destIndex);

            // Making the pipeline null in the destination index
            PipelineUtils.makeNull(destClient, destIndex);

            // Deleting snapshot to release memory
            CleanupUtils.deleteSnapshot(sourceClient, sourceRepository, snapshotName);

            count++;
            logger.info("First Phase Completed!");
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
            Checks.verifyDocumentsCount(sourceClient, sourceIndex, destClient, destIndex);

            // Step 2: Cleanup
            CleanupUtils.deleteIndex(sourceClient, sourceIndex);
        } catch (ElasticsearchException e) {
            logger.error("Migration failed: ", e);
            throw e;
        }
    }

    public static void migrateIndices(RestHighLevelClient sourceClient, RestHighLevelClient destClient,
                                      String pipelineName, String sourceHost, List<String> sourceIndices,
                                      List<String> destIndices, String sourceRepository) throws IOException {
        try {
            for (int i = 0; i < sourceIndices.size(); i += BATCH_SIZE) {
                List<String> batchSourceIndices = sourceIndices.subList(i,
                        Math.min(i + BATCH_SIZE, sourceIndices.size()));
                List<String> batchDestIndices = destIndices.subList(i,
                        Math.min(i + BATCH_SIZE, destIndices.size()));

                HashMap<String , Double> sequenceMap = new HashMap<String, Double>();

                logger.info("Migrating batch {}: {} to {}", ((i / BATCH_SIZE) + 1),
                        batchSourceIndices, batchDestIndices);
                for (int j = 0; j < batchSourceIndices.size(); j++) {
                    String sourceIndex = batchSourceIndices.get(j);
                    String destIndex = batchDestIndices.get(j);

                    double maxSeqNoValue = phaseOneMigrateIndices(sourceClient, destClient, sourceIndex,
                            pipelineName, sourceRepository, destIndex);

                    sequenceMap.put(sourceIndex, maxSeqNoValue);
                }

//              ---------------------------------- SHIFT OPERATIONS -----------------------------------------------

                for (int j = 0; j < batchSourceIndices.size(); j++) {
                    String sourceIndex = batchSourceIndices.get(j);
                    String destIndex = batchDestIndices.get(j);
                    double maxSeqNoValue = sequenceMap.get(sourceIndex);
                    phaseTwoMigrateIndices(destClient, sourceClient, sourceHost, sourceIndex, destIndex, maxSeqNoValue);
                }
            }

            logger.info("All indices migrated successfully!");
        } catch (ElasticsearchException e) {
            logger.error("Migration failed: ", e);
            throw e;
        }
    }
}
