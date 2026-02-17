package com.example.pyflink.sink.ddb;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.dynamodb.sink.DynamoDbSink;

import java.util.Properties;
public class DdbSink {
public static DynamoDbSink<String> getJsonSink(Properties sinkProperties,
                                                  DdbExecutionProperties executionProperties) {
    return DynamoDbSink.<String>builder()
            .setDynamoDbProperties(sinkProperties)
            .setTableName(sinkProperties.getProperty("table.name"))
            .setElementConverter(JsonDdbElementConverter.builder().build())
            .setFailOnError(executionProperties.isFailOnError())
            .setMaxBatchSize(executionProperties.getMaxBatchSize())
            .setMaxInFlightRequests(executionProperties.getMaxInFlightRequests())
            .setMaxBufferedRequests(executionProperties.getMaxBufferedRequests())
            .setMaxTimeInBufferMS(executionProperties.getMaxTimeInBufferMS())
            .build();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class DdbExecutionProperties {
        @Builder.Default
        private boolean failOnError = false;
        @Builder.Default
        private int maxBatchSize = 25;
        @Builder.Default
        private int maxInFlightRequests = 50;
        @Builder.Default
        private int maxBufferedRequests = 10_000;
        @Builder.Default
        private long maxTimeInBufferMS = 500;
    }
}