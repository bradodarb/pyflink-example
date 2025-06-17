package com.example.pyflink.sink.ddb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Builder;
import lombok.experimental.SuperBuilder;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.*;

@SuperBuilder
public class JsonDdbElementConverter implements ElementConverter<ObjectNode, DynamoDbWriteRequest> {

    @Override
    public DynamoDbWriteRequest apply(ObjectNode jsonNodes, org.apache.flink.api.connector.sink2.SinkWriter.Context context) {
        final Map<String, AttributeValue> item = convertMap(jsonNodes, 0);
        return DynamoDbWriteRequest.builder().setType(DynamoDbWriteRequestType.PUT).setItem(item).build();
    }

    protected Map<String, AttributeValue> convertMap(ObjectNode node, int depth) {
        Map<String, AttributeValue> item = new HashMap<>();

        int currentDepth = depth + 1;
        node.fieldNames().forEachRemaining(f -> {
            item.put(f, convertNode((ObjectNode) node.get(f), currentDepth));
        });
        return item;
    }

    protected Collection<AttributeValue> convertList(ObjectNode node, int depth) {
        List<AttributeValue> items = new ArrayList<>();

        int currentDepth = depth + 1;
        node.elements().forEachRemaining(element -> {
            items.add(convertNode((ObjectNode) element, currentDepth));
        });
        return items;
    }

    protected AttributeValue convertNode(ObjectNode node, int depth) {

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
                builder.m(convertMap(node, currentDepth));
                //recurse
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
