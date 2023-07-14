package org.example.Utils;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.example.Migration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class PipelineUtils {
    private static final Logger logger = LoggerFactory.getLogger(PipelineUtils.class);
    private static String getPipelineSource(int count) {
        return "{\n" +
                "  \"description\" : \"Adding under migration field\",\n" +
                "  \"processors\" : [\n" +
                "    {\n" +
                "      \"set\": {\n" +
                "        \"field\": \"_is_under_migration\",\n" +
                "        \"value\": " + count + "\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
    }

    public static void createWriteRedirectPipeline(RestHighLevelClient sourceClient, String pipelineName, int count) throws IOException {
        String source = getPipelineSource(count);
        PutPipelineRequest request = new PutPipelineRequest(
                pipelineName,
                new BytesArray(source.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON
        );
        try {
            sourceClient.ingest().putPipeline(request, RequestOptions.DEFAULT);
            logger.info("Pipeline " + pipelineName + " created successfully!");
        } catch (IOException e) {
            logger.error("Failed to create pipeline: " + e.getMessage());
            throw e;
        }
    }

    public static void associatePipelineWithIndex(RestHighLevelClient sourceClient, String sourceIndex, String pipelineName) throws IOException {
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(sourceIndex);
        String settingKey = "index.default_pipeline";
        Settings settings = Settings.builder().put(settingKey, pipelineName).build();
        updateSettingsRequest.settings(settings);
        try {
            sourceClient.indices().putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
            logger.info("Settings updated");
        } catch (IOException e) {
            logger.error("Failed to update settings: " + e.getMessage());
            throw e;
        }
    }

    public static void makeNull(RestHighLevelClient destClient, String destIndex) throws IOException {
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(destIndex);
        String settingKey = "index.default_pipeline";
        Settings settings = Settings.builder().putNull(settingKey).build();
        updateSettingsRequest.settings(settings);
        try {
            destClient.indices().putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
            logger.info("Pipeline set to null!");
        } catch (IOException e) {
            logger.error("Failed to set pipeline to null: " + e.getMessage());
            throw e;
        }
    }

    public static void changePipeline(RestHighLevelClient sourceClient, String pipelineName, int count, String sourceIndex) throws IOException {
        createWriteRedirectPipeline(sourceClient, pipelineName, count);
        associatePipelineWithIndex(sourceClient, sourceIndex, pipelineName);
    }
}
