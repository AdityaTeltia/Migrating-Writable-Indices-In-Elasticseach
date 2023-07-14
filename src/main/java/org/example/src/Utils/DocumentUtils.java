package org.example.src.Utils;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Random;

public class DocumentUtils {

    private static final int RANDOM_STRING_LENGTH = 10;
    private static final Random RANDOM = new Random();

    public static void addDocuments(RestHighLevelClient client, String index) throws IOException, InterruptedException {
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
            IndexRequest indexRequest = new IndexRequest(index).source(builder);
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
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
