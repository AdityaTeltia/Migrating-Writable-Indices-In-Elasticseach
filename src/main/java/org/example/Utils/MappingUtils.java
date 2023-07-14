package org.example.Utils;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MappingUtils {

    private static final Logger logger = LoggerFactory.getLogger(MappingUtils.class);

    public static void setMapping(RestHighLevelClient client, String index) throws IOException {
        try {
            PutMappingRequest putMappingRequest = new PutMappingRequest(index);
            putMappingRequest.source(getMappingSource(), XContentType.JSON);
            client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
            logger.info("Mapping updated for index: {}", index);
        } catch (IOException e) {
            logger.error("Failed to update mapping for index {}: {}", index, e.getMessage());
            throw e;
        } catch (ElasticsearchException e) {
            logger.error("Failed to update mapping for index {}: {}", index, e.getMessage());
            throw new IOException("Failed to update mapping", e);
        }
    }

    private static String getMappingSource() {
        return "{\n" +
                "    \"properties\": {\n" +
                "      \"_is_under_migration\": { \n" +
                "        \"type\": \"keyword\",\n" +
                "        \"doc_values\": false\n" +
                "      }\n" +
                "    }\n" +
                "}";
    }
}
