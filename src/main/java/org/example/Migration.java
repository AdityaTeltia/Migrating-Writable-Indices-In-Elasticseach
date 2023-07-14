package org.example;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class Migration {
    private static final int BATCH_SIZE = 2;
    private static int count = 1;
    private static final Logger logger = LoggerFactory.getLogger(Migration.class);

    public static void phaseOneMigrateIndices(RestHighLevelClient sourceClient, RestHighLevelClient destClient,
                                              String sourceIndex, String pipelineName, String sourceRepository,
                                              String destIndex) throws IOException {
        try {
            // Phase 0: Pre-migration checks
            Checks.preMigrationCheck(destClient, sourceClient, sourceIndex, destIndex);

            // Phase 1: Setting mapping for the new field we are adding
            MappingUtils.setMapping(sourceClient, sourceIndex);

            // Phase 1.1: Change write pipeline
            PipelineUtils.changePipeline(sourceClient, pipelineName, count, sourceIndex);

            // Phase 2: Snapshot and restore
            SnapshotUtils.snaps(sourceClient, sourceRepository, sourceIndex, destClient, destIndex);

            // Phase 2.1: Verifying snapshot and restore
            Checks.verifyDocumentsCount(sourceClient, sourceIndex, destClient, destIndex);

            // Adding documents to the source index to see if they get migrated in the second pass.
            DocumentUtils.addDocuments(sourceClient, sourceIndex);

            // Making the pipeline null in the destination index
            PipelineUtils.makeNull(destClient, destIndex);
            count++;
            logger.info("First Phase Completed!");
        } catch (ElasticsearchException e) {
            logger.error("Migration failed: ", e);
            throw e;
        }
    }

    public static void phaseTwoMigrateIndices(RestHighLevelClient destClient, RestHighLevelClient sourceClient,
                                              String sourceHost, String sourceIndex, String destIndex)
            throws IOException {
        try {
            // Phase 3: Reindexing new index
            ReindexUtils.reindexData(destClient, sourceIndex, destIndex);

            // Phase 3.1: Post second pass verification
            Checks.verifyDocumentsCount(sourceClient, sourceIndex, destClient, destIndex);

            // Phase 4: Cleanup
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

                logger.info("Migrating batch {}: {} to {}", ((i / BATCH_SIZE) + 1),
                        batchSourceIndices, batchDestIndices);
                for (int j = 0; j < batchSourceIndices.size(); j++) {
                    String sourceIndex = batchSourceIndices.get(j);
                    String destIndex = batchDestIndices.get(j);

                    phaseOneMigrateIndices(sourceClient, destClient, sourceIndex,
                            pipelineName, sourceRepository, destIndex);
                }

//              ---------------------------------- SHIFT OPERATIONS -----------------------------------------------

                for (int j = 0; j < batchSourceIndices.size(); j++) {
                    String sourceIndex = batchSourceIndices.get(j);
                    String destIndex = batchDestIndices.get(j);

                    phaseTwoMigrateIndices(destClient, sourceClient, sourceHost, sourceIndex, destIndex);
                }
            }

            logger.info("All indices migrated successfully!");
        } catch (ElasticsearchException e) {
            logger.error("Migration failed: ", e);
            throw e;
        }
    }
}
