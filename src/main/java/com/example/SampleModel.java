package com.example;

import com.example.annotations.PartitionKey;
import com.example.annotations.VersionKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SampleModel {
    @PartitionKey
    private String id;
    private String name;
    private Map<String, String> attributes;
    @VersionKey
    private int version;
}
