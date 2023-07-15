package org.example.src.Utils;

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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class ReindexUtils {
    private static final Logger logger = LoggerFactory.getLogger(ReindexUtils.class);

    public static void reindexData(RestHighLevelClient destClient, String sourceHost, String sourceIndex, String destIndex, double maxSeqNoValue) throws IOException {
        try {
            ReindexRequest reindexRequest = new ReindexRequest();
            reindexRequest.setRemoteInfo(createRemoteInfo(sourceHost, maxSeqNoValue));
            reindexRequest.setSourceIndices(sourceIndex);
            reindexRequest.setDestIndex(destIndex);
//            reindexRequest.setScript(createScript());

            destClient.reindex(reindexRequest, RequestOptions.DEFAULT);
            logger.info("Reindexing data from index {} to {} completed successfully", sourceIndex, destIndex);
        } catch (IOException e) {
            logger.error("Failed to reindex data from index {} to {}: {}", sourceIndex, destIndex, e.getMessage());
            throw e;
        } catch (ElasticsearchException | URISyntaxException e) {
            logger.error("Failed to reindex data from index {} to {}: {}", sourceIndex, destIndex, e.getMessage());
            throw new IOException("Failed to reindex data", e);
        }
    }

    private static RemoteInfo createRemoteInfo(String sourceHost, double maxSeqNoValue) throws URISyntaxException {
        URI uri = new URI(sourceHost);
        String scheme = uri.getScheme();
        String host = uri.getHost();
        int port = uri.getPort();
        String pathPrefix = uri.getPath();

        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"range\": {\n" +
                "      \"_seq_no\": {\n" +
                "        \"gt\":"+ maxSeqNoValue+"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        return new RemoteInfo(
                scheme, host, port, pathPrefix,
                new BytesArray(QueryBuilders.rangeQuery("_seq_no").gt(maxSeqNoValue).toString()),
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
