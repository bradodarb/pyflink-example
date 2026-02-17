package com.example.pyflink.sink.ddb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.SneakyThrows;
import lombok.experimental.SuperBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.*;

@SuperBuilder
public class JsonDdbElementConverter implements ElementConverter<String, DynamoDbWriteRequest> {

    final ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows
    @Override
    public DynamoDbWriteRequest apply(String jsonBlob, org.apache.flink.api.connector.sink2.SinkWriter.Context context) {
        ObjectNode jsonNodes = objectMapper.readValue(jsonBlob, ObjectNode.class);
        final Map<String, AttributeValue> item = convertMap(jsonNodes, 0);
        return DynamoDbWriteRequest.builder().setType(DynamoDbWriteRequestType.PUT).setItem(item).build();
    }

    protected Map<String, AttributeValue> convertMap(ObjectNode node, int depth) {
        Map<String, AttributeValue> item = new HashMap<>();

        int currentDepth = depth + 1;
        node.fieldNames().forEachRemaining(f -> {
            item.put(f, convertNode(node.get(f), currentDepth));
        });
        return item;
    }

    protected Collection<AttributeValue> convertList(JsonNode node, int depth) {
        List<AttributeValue> items = new ArrayList<>();

        int currentDepth = depth + 1;
        node.elements().forEachRemaining(element -> {
            items.add(convertNode(element, currentDepth));
        });
        return items;
    }

    protected AttributeValue convertNode(JsonNode node, int depth) {

        int currentDepth = depth + 1;
        AttributeValue.Builder builder = AttributeValue.builder();
        switch (node.getNodeType()) {
            case NULL:
                builder.nul(true);
                break;
            case BOOLEAN:
                builder.bool(node.asBoolean());
                break;
            case NUMBER:
                builder.n(node.asText());
                break;
            case ARRAY:
                builder.l(convertList(node, currentDepth));
                break;
            case OBJECT:
                builder.m(convertMap((ObjectNode) node, currentDepth));
                break;
            case STRING:
                builder.s(node.asText());
                break;
            case BINARY:
                builder.b(SdkBytes.fromUtf8String(node.asText()));
                break;
            default:
                throw new RuntimeException("Invalid JSON type provided for DDB conversion");
        }
        return builder.build();
    }


}
