package org.example;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;

public class Checks {
    private static final Logger logger = LoggerFactory.getLogger(Checks.class);
    private static final String LOG_ERROR_MESSAGE_FORMAT = "Index '%s' already exists.";
    private static final String LOG_WARNING_MESSAGE_DISK_SPACE_INSUFFICIENT = "Destination cluster does not have enough disk space to accommodate the source index.";
    private static final String LOG_VERIFICATION_ERROR_MESSAGE = "Document count mismatch after snapshot and restore!";

    private static long extractSizeInBytes(String responseBody) {
        JsonObject jsonObject = JsonParser.parseString(responseBody).getAsJsonObject();
        JsonObject indices = jsonObject.getAsJsonObject("_all");
        JsonObject primaries = indices.getAsJsonObject("primaries");
        JsonObject store = primaries.getAsJsonObject("store");
        long sizeInBytes = store.get("size_in_bytes").getAsLong();
        return sizeInBytes;
    }

    private static long calculateSizeInBytesSum(String json) {
        long sizeInBytesSum = 0;

        JsonElement jsonElement = JsonParser.parseString(json);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        JsonObject nodes = jsonObject.getAsJsonObject("nodes");

        for (Map.Entry<String, JsonElement> entry : nodes.entrySet()) {
            JsonObject nodeObject = entry.getValue().getAsJsonObject();
            JsonObject indices = nodeObject.getAsJsonObject("indices");
            JsonObject store = indices.getAsJsonObject("store");
            long sizeInBytes = store.get("size_in_bytes").getAsLong();
            sizeInBytesSum += sizeInBytes;
        }

        return sizeInBytesSum;
    }


    public static void preMigrationCheck(RestHighLevelClient destClient, RestHighLevelClient sourceClient, String sourceIndex, String destIndex) throws IOException {
        try {
            // Check if destination index already exists
            GetIndexRequest indexRequest = new GetIndexRequest(destIndex);
            boolean destIndexAlreadyExists = destClient.indices().exists(indexRequest, RequestOptions.DEFAULT);
            if (destIndexAlreadyExists) {
                logger.error(String.format(LOG_ERROR_MESSAGE_FORMAT, destIndex));
                return;
            }

            RestClient lowLevelDestClient = destClient.getLowLevelClient();
            Request request = new Request(
                    "GET",
                    "/_nodes/stats");
            Response response = lowLevelDestClient.performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            long availableDiskSpace = calculateSizeInBytesSum(responseBody);

            RestClient lowLevelSourceClient = sourceClient.getLowLevelClient();
            request = new Request(
                    "GET",
                    "/"+sourceIndex+"/_stats"
            );
            response = lowLevelSourceClient.performRequest(request);
            // Convert the response entity to a string
            responseBody = EntityUtils.toString(response.getEntity());
            // Parse the JSON response to extract the size_in_bytes value
            long sourceIndexSize = extractSizeInBytes(responseBody);

            // Compare available disk space with source index size
             if (availableDiskSpace < sourceIndexSize) {
                 logger.warn(LOG_WARNING_MESSAGE_DISK_SPACE_INSUFFICIENT);
                 return;
             }
        } catch (IOException e) {
            logger.error("Error during pre-migration check: " + e.getMessage(), e);
            throw e;
        }
    }

    public static long checkDocumentCount(RestHighLevelClient client, String index) throws IOException {
        try {
            RefreshRequest refreshRequest = new RefreshRequest(index);
            client.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
            CountRequest countRequest = new CountRequest(index);
            long count = client.count(countRequest, RequestOptions.DEFAULT).getCount();
            return count;
        } catch (IOException e) {
            logger.error("Error while checking document count: " + e.getMessage(), e);
            throw e;
        }
    }

    public static void verifyDocumentsCount(RestHighLevelClient sourceClient, String sourceIndex, RestHighLevelClient destClient, String destIndex) throws IOException {
        try {
            long sourceCount = checkDocumentCount(sourceClient, sourceIndex);
            long destCount = checkDocumentCount(destClient, destIndex);

            if (sourceCount != destCount) {
                logger.error(LOG_VERIFICATION_ERROR_MESSAGE);
            }
        } catch (IOException e) {
            logger.error("Error while verifying document count: " + e.getMessage(), e);
            throw e;
        }
    }
}
