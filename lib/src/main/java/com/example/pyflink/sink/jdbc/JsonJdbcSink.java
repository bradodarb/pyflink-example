package com.example.pyflink.sink.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Properties;

public class JsonJdbcSink {

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Creates a JDBC SinkFunction that parses incoming JSON strings and maps
     * named fields to positional PreparedStatement parameters.
     *
     * @param sql         INSERT/UPSERT statement with ? placeholders
     * @param fieldNames  JSON field names in the order matching the ? placeholders
     * @param connProps   Connection properties: url, driver, username, password
     * @param execProps   Execution properties: batch.interval.ms, batch.size, max.retries
     */
    public static SinkFunction<String> getSink(
            String sql,
            String[] fieldNames,
            Properties connProps,
            Properties execProps
    ) {
        JdbcConnectionOptions connOpts = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(connProps.getProperty("url"))
                .withDriverName(connProps.getProperty("driver", "org.postgresql.Driver"))
                .withUsername(connProps.getProperty("username"))
                .withPassword(connProps.getProperty("password"))
                .build();

        JdbcExecutionOptions execOpts = JdbcExecutionOptions.builder()
                .withBatchIntervalMs(Long.parseLong(execProps.getProperty("batch.interval.ms", "200")))
                .withBatchSize(Integer.parseInt(execProps.getProperty("batch.size", "5")))
                .withMaxRetries(Integer.parseInt(execProps.getProperty("max.retries", "5")))
                .build();

        return JdbcSink.sink(
                sql,
                (ps, jsonStr) -> {
                    try {
                        JsonNode node = mapper.readTree(jsonStr);
                        for (int i = 0; i < fieldNames.length; i++) {
                            JsonNode field = node.get(fieldNames[i]);
                            if (field == null || field.isNull()) {
                                ps.setNull(i + 1, java.sql.Types.VARCHAR);
                            } else if (field.isNumber()) {
                                if (field.isIntegralNumber()) {
                                    ps.setLong(i + 1, field.asLong());
                                } else {
                                    ps.setDouble(i + 1, field.asDouble());
                                }
                            } else if (field.isBoolean()) {
                                ps.setBoolean(i + 1, field.asBoolean());
                            } else {
                                ps.setString(i + 1, field.asText());
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse JSON: " + jsonStr, e);
                    }
                },
                execOpts,
                connOpts
        );
    }
}
