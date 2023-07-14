package org.example.src.Utils;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CleanupUtils {
    private static final Logger logger = LoggerFactory.getLogger(CleanupUtils.class);
    public static void deleteIndex(RestHighLevelClient client, String index) throws IOException {
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
            client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            logger.info("Index {} deleted successfully", index);
        } catch (IOException e) {
            logger.error("Failed to delete index {}: {}", index, e.getMessage());
            throw e;
        } catch (ElasticsearchException e) {
            logger.error("Failed to delete index {}: {}", index, e.getMessage());
            throw new IOException("Failed to delete index", e);
        }
    }

    public static void deleteSnapshot(RestHighLevelClient client, String repository, String snapshotName) throws IOException {
        try {
            DeleteSnapshotRequest deleteSnapshotRequest = new DeleteSnapshotRequest(repository, snapshotName);
            client.snapshot().delete(deleteSnapshotRequest, RequestOptions.DEFAULT);
            logger.info("Snapshot {} deleted successfully!", snapshotName);
        } catch (IOException e) {
            logger.error("Failed to snapshot {}: {}", snapshotName, e.getMessage());
            throw e;
        } catch (ElasticsearchException e) {
            logger.error("Failed to snapshot {}: {}", snapshotName, e.getMessage());
            throw new IOException("Failed to delete index", e);
        }
    }
}
