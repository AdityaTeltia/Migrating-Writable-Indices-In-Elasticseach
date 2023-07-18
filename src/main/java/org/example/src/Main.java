package org.example.src;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    // S3_repository
    private static final String sourceRepository = "s3testing";
    // Hosts
    private static final String sourceHost = "http://localhost:9200";
    private static final String destHost = "http://localhost:9201";
    private static final String prefixIndex = "test";

    public static void main(String[] args) {
        try (RestHighLevelClient sourceClient = new RestHighLevelClient(
                RestClient.builder(HttpHost.create(sourceHost)));
             RestHighLevelClient destClient = new RestHighLevelClient(
                     RestClient.builder(HttpHost.create(destHost)))) {

            // Indexes
            GetIndexResponse sourceIndicesResponse = sourceClient.indices()
                    .get(new GetIndexRequest(prefixIndex + "*"), RequestOptions.DEFAULT);
            String[] sourceIndices = sourceIndicesResponse.getIndices();
            List<String> sourceIndexList = Arrays.asList(sourceIndices);
            List<String> destIndices = new ArrayList<>();
            for (String sourceIndex : sourceIndices) {
                destIndices.add("restored_" + sourceIndex);
            }

            // Migration starts
            Instant startTime = Instant.now();
            Migration.migrateIndices(sourceClient, destClient, sourceHost, sourceIndexList, destIndices,
                    sourceRepository);
            Instant endTime = Instant.now();

            // Calculate durations
            Duration duration = Duration.between(startTime, endTime);
            long totalDuration = duration.getSeconds();

            // Print duration
            logger.info("Total duration: {} seconds", totalDuration);
        } catch (IOException e) {
            logger.error("An error occurred during execution", e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
