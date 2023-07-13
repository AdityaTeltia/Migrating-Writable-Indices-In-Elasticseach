package org.example;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class SnapshotUtils {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotUtils.class);
    private static final String SNAPSHOT_NAME_FORMAT = "%s-%s";

    public static String createSnapshot(RestHighLevelClient client, String repository, String sourceIndex) throws IOException {
        Objects.requireNonNull(client, "RestHighLevelClient must not be null");
        Objects.requireNonNull(repository, "Repository name must not be null");
        Objects.requireNonNull(sourceIndex, "Source index name must not be null");

        LocalDateTime now = LocalDateTime.now();
        String timestamp = now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        String snapshotName = String.format(SNAPSHOT_NAME_FORMAT, sourceIndex, timestamp);

        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest(repository, snapshotName)
                .indices(sourceIndex)
                .includeGlobalState(false)
                .waitForCompletion(true);

        CreateSnapshotResponse response = client.snapshot().create(createSnapshotRequest, RequestOptions.DEFAULT);
        logger.info("Snapshot {} created successfully", snapshotName);
        return snapshotName;
    }

    public static void restoreSnapshot(RestHighLevelClient client, String repository, String snapshotName, String sourceIndex, String destIndex) throws IOException {
        Objects.requireNonNull(client, "RestHighLevelClient must not be null");
        Objects.requireNonNull(repository, "Repository name must not be null");
        Objects.requireNonNull(snapshotName, "Snapshot name must not be null");
        Objects.requireNonNull(sourceIndex, "Source index name must not be null");
        Objects.requireNonNull(destIndex, "Destination index name must not be null");

        RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest(repository, snapshotName)
                .indices(sourceIndex)
                .renamePattern(sourceIndex)
                .renameReplacement(destIndex)
                .waitForCompletion(true);

        RestoreSnapshotResponse response = client.snapshot().restore(restoreSnapshotRequest, RequestOptions.DEFAULT);
        logger.info("Snapshot {} restored successfully to index {}", snapshotName, destIndex);
    }

    public static void snaps(RestHighLevelClient sourceClient, String sourceRepository, String sourceIndex,
                             RestHighLevelClient destClient, String destIndex) throws IOException {
        String snapshotName = createSnapshot(sourceClient, sourceRepository, sourceIndex);
        restoreSnapshot(destClient, sourceRepository, snapshotName, sourceIndex, destIndex);
    }
}
