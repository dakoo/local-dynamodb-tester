package com.example;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncDynamoDbWriter {
    private static final Logger log = LoggerFactory.getLogger(AsyncDynamoDbWriter.class);
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final EnvironmentConfig environmentConfig;

    private static final ExecutorService executorService = Executors.newFixedThreadPool(50); // ✅ Optimize concurrency

    public AsyncDynamoDbWriter(DynamoDbAsyncClient client, EnvironmentConfig environmentConfig) {
        this.dynamoDbAsyncClient = client;
        this.environmentConfig = environmentConfig;
    }

    public void executeAsyncWrites(List<SampleModel> models) {
        Collections.shuffle(models);  // ✅ Helps distribute load across DynamoDB partitions

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (SampleModel model : models) {
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                        log.info("SEND {} to table {}", model.getId(), environmentConfig.getTableName());
                        return doConditionalUpdateAsync(model).join();  // ✅ Ensures async execution
                    }, executorService)
                    .thenAccept(response -> {
                        log.info("COMPLETED {} successfully.", model.getId());
                    })
                    .exceptionally(ex -> {
                        log.error("Error updating model: {} - {}", model.getId(), ex.getMessage(), ex);
                        return null;
                    });

            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .exceptionally(ex -> {
                    log.error("Batch update failed: {}", ex.getMessage(), ex);
                    return null;
                })
                .join(); // ✅ Ensures all writes complete
    }

    private CompletableFuture<UpdateItemResponse> doConditionalUpdateAsync(SampleModel model) {
        Map<String, AttributeValue> key = Map.of(
                "id", AttributeValue.builder().s(model.getId()).build()
        );

        Map<String, AttributeValue> expressionAttributeValues = Map.of(
                ":newName", AttributeValue.builder().s(model.getName()).build(),
                ":newVersion", AttributeValue.builder().n(String.valueOf(model.getVersion())).build()
        );

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(environmentConfig.getTableName())
                .key(key)
                .updateExpression("SET name = :newName, version = :newVersion")
                .expressionAttributeValues(expressionAttributeValues)
                .conditionExpression("attribute_not_exists(version) OR version < :newVersion") // ✅ Only update if newer version
                .build();

        return dynamoDbAsyncClient.updateItem(request)
                .exceptionally(ex -> {
                    if (ex.getCause() instanceof ConditionalCheckFailedException) {
                        log.warn("Skipping update for {} due to version conflict.", model.getId());
                    } else {
                        log.error("DynamoDB update failed for {}: {}", model.getId(), ex.getMessage(), ex);
                    }
                    return null;
                });
    }
}
