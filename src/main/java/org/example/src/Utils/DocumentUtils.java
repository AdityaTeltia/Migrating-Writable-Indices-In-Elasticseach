package org.example.src.Utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class DocumentUtils {

    private static final int RANDOM_STRING_LENGTH = 10;
    private static final Random RANDOM = new Random();


    public static double getLastSequenceNumber(RestHighLevelClient client, String index) throws IOException {
        MaxAggregationBuilder maxSeqNoAggregation = AggregationBuilders.max("max_seq_no").field("_seq_no");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .size(0)
                .aggregation(maxSeqNoAggregation);

        SearchRequest searchRequest = new SearchRequest(index)
                .source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String responseBody = searchResponse.toString();
        JsonElement jsonElement = JsonParser.parseString(responseBody);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        JsonElement maxSeqNoElement = jsonObject.getAsJsonObject("aggregations")
                .getAsJsonObject("max#max_seq_no")
                .get("value");

        double maxSeqNoValue = maxSeqNoElement.getAsDouble();
        return maxSeqNoValue;
    }


    public static void addDocuments(RestHighLevelClient client, String index, String id) throws IOException, InterruptedException {
            // Define the document data
            String name = generateRandomString(RANDOM_STRING_LENGTH);
            int age = 30;
            String email = "johndoe@example.com";

            // Create the JSON document using XContentBuilder
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("name", name);
            builder.field("age", age);
            builder.field("email", email);
            builder.endObject();

            // Create the IndexRequest and add the document to the index
            if(id == null){
                IndexRequest indexRequest = new IndexRequest(index).source(builder);
                client.index(indexRequest, RequestOptions.DEFAULT);
            }else{
                IndexRequest indexRequest = new IndexRequest(index).id(id).source(builder);
                client.index(indexRequest, RequestOptions.DEFAULT);
            }
    }

    private static String generateRandomString(int length) {
        StringBuilder stringBuilder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            stringBuilder.append(randomChar());
        }
        return stringBuilder.toString();
    }

    public static char randomChar() {
        String characters = "abcdefghijklmnopqrstuvwxyz0123456789";
        int index = RANDOM.nextInt(characters.length());
        return characters.charAt(index);
    }

}
