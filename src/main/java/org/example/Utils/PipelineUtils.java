package org.example.Utils;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class PipelineUtils {

    public static void createWriteRedirectPipeline(RestHighLevelClient sourceClient, String pipelineName, int count) throws IOException {
        String source = "{\n" +
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

        PutPipelineRequest request = new PutPipelineRequest(
                pipelineName,
                new BytesArray(source.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON
        );

        try {
            sourceClient.ingest().putPipeline(request, RequestOptions.DEFAULT);
            System.out.println("Pipeline " + pipelineName + " created successfully!");
        } catch (IOException e) {
            System.err.println("Failed to create pipeline: " + e.getMessage());
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
            System.out.println("Settings updated");
        } catch (IOException e) {
            System.err.println("Failed to update settings: " + e.getMessage());
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
            System.out.println("Pipeline set to null!");
        } catch (IOException e) {
            System.err.println("Failed to set pipeline to null: " + e.getMessage());
            throw e;
        }
    }

    public static void changePipeline(RestHighLevelClient sourceClient, String pipelineName, int count, String sourceIndex) throws IOException {
        createWriteRedirectPipeline(sourceClient, pipelineName, count);
        associatePipelineWithIndex(sourceClient, sourceIndex, pipelineName);
    }
}
