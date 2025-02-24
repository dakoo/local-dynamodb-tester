package com.example;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.regions.Region;

public class DynamoDBClientProvider {
    public static DynamoDbAsyncClient createClient(String awsProfile) {
        AwsCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create(awsProfile);

        SdkAsyncHttpClient asyncHttpClient = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(200)  // ✅ Allows higher parallel writes
                .build();

        return DynamoDbAsyncClient.builder()
                .credentialsProvider(credentialsProvider) // ✅ Uses AWS profile passed as argument
                .httpClient(asyncHttpClient)
                .region(Region.AP_NORTHEAST_2)  // ✅ Default region is ap-northeast-2
                .build();
    }
}
