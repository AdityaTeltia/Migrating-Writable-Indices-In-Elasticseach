package org.example.src.Utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.example.src.Migration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class DocumentUtils {

    private static final int RANDOM_STRING_LENGTH = 10;
    private static final Random RANDOM = new Random();

    private static final Logger logger = LoggerFactory.getLogger(DocumentUtils.class);

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

    public static void updateDocuments(RestHighLevelClient client, String index , String id) throws IOException, InterruptedException {
        // Fetch a random document ID from the source index
        String randomDocumentId = getRandomDocumentId(client, index);

        if(id != null){
            randomDocumentId = id;
        }

        // If a document ID is found
        if (randomDocumentId != null) {
            // Generate new data for the document
            String newName = generateRandomString(10);
            int newAge = 35;
            String newEmail = "updated@example.com";

            // Create the JSON document using XContentBuilder
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("name", newName);
            builder.field("age", newAge);
            builder.field("email", newEmail);
            builder.endObject();

            // Create the UpdateRequest and update the document in the index
            UpdateRequest updateRequest = new UpdateRequest(index, randomDocumentId)
                    .doc(builder);
            UpdateResponse updateResponse = client.update(updateRequest , RequestOptions.DEFAULT);
            logger.info("ID: {} document of index {} updated successfully!", randomDocumentId, index);
            Thread.sleep(200);
        }
    }

    // Helper function to fetch a random document ID from the Elasticsearch index
    public static String getRandomDocumentId(RestHighLevelClient client, String index) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(1);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();

        // Check if there are any hits
        if (hits.getTotalHits().value > 0) {
            SearchHit randomHit = hits.getAt(new Random().nextInt(hits.getHits().length));
            return randomHit.getId();
        }
        return null; // No documents found
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
