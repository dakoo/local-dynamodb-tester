package com.example;

public class EnvironmentConfig {
    private final String tableName;

    public EnvironmentConfig(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
