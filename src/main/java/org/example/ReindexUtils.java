package org.example;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class ReindexUtils {

    private static final Logger logger = LoggerFactory.getLogger(ReindexUtils.class);

    public static void reindexData(RestHighLevelClient destClient, String sourceIndex, String destIndex) throws IOException {
        try {
            ReindexRequest reindexRequest = new ReindexRequest();
            reindexRequest.setRemoteInfo(createRemoteInfo());
            reindexRequest.setSourceIndices(sourceIndex);
            reindexRequest.setDestIndex(destIndex);
            reindexRequest.setScript(createScript());

            destClient.reindex(reindexRequest, RequestOptions.DEFAULT);
            logger.info("Reindexing data from index {} to {} completed successfully", sourceIndex, destIndex);
        } catch (IOException e) {
            logger.error("Failed to reindex data from index {} to {}: {}", sourceIndex, destIndex, e.getMessage());
            throw e;
        } catch (ElasticsearchException e) {
            logger.error("Failed to reindex data from index {} to {}: {}", sourceIndex, destIndex, e.getMessage());
            throw new IOException("Failed to reindex data", e);
        }
    }

    private static RemoteInfo createRemoteInfo() {
        return new RemoteInfo(
                "http", "localhost", 9200, null,
                new BytesArray(QueryBuilders.existsQuery("_is_under_migration").toString()),
                "", "", Collections.emptyMap(),
                new TimeValue(100, TimeUnit.MILLISECONDS),
                new TimeValue(100, TimeUnit.SECONDS)
        );
    }

    private static Script createScript() {
        return new Script(
                ScriptType.INLINE,
                "painless",
                "ctx._source.remove('_is_under_migration')",
                Collections.emptyMap()
        );
    }
}
