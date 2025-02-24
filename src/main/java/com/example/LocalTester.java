package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LocalTester {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java -jar local-dynamodb-tester.jar <file-path> <table-name>");
            System.exit(1);
        }

        String filePath = args[0];
        String tableName = args[1];

        try {
            System.out.println("Reading file: " + filePath);
            byte[] jsonData = Files.readAllBytes(Paths.get(filePath));
            ObjectMapper objectMapper = new ObjectMapper();
            List<SampleModel> models = objectMapper.readValue(jsonData,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, SampleModel.class));

            System.out.println("Loaded " + models.size() + " sample items.");

            AsyncDynamoDbWriter writer = new AsyncDynamoDbWriter(DynamoDBClientProvider.getClient(), tableName);

            CompletableFuture<Void> writeFuture = CompletableFuture.runAsync(() -> writer.executeAsyncWrites(models));
            writeFuture.join();  // Wait for execution to finish

            System.out.println("All writes completed.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
