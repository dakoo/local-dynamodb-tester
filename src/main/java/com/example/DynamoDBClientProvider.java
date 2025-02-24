package com.example;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

public class DynamoDBClientProvider {
    private static final SdkAsyncHttpClient asyncHttpClient = NettyNioAsyncHttpClient.builder()
            .maxConcurrency(200)  // Allow more concurrent requests
            .build();

    private static final DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
            .httpClient(asyncHttpClient)
            .build();

    public static DynamoDbAsyncClient getClient() {
        return dynamoDbAsyncClient;  // Return the singleton client
    }
}
