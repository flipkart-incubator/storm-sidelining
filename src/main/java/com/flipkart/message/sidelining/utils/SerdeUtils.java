package com.flipkart.message.sidelining.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;

import java.io.IOException;

/**
 * Created by gupta.rajat on 06/06/17.
 */
public class SerdeUtils {
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static ObjectMapper yamlMapper = new ObjectMapper();

    static {
        initObjecMapper(objectMapper);
        yamlMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public static void initObjecMapper(ObjectMapper mapper) {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        mapper.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING);
        mapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        mapper.setSubtypeResolver(new StdSubtypeResolver());
    }

    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public static <T> byte[] serialize(T object) throws JsonProcessingException {
        if (object == null)
            return null;

        return objectMapper.writeValueAsString(object).getBytes();
    }

    public static <T> String serializeToString(T object) throws JsonProcessingException {
        if (object == null)
            return null;

        return objectMapper.writeValueAsString(object);
    }

    public static <T> T deserialize(byte[] bytes, com.fasterxml.jackson.core.type.TypeReference<T> typeReference) throws IOException {
        if (bytes == null) return null;
        return objectMapper.readValue(bytes, typeReference);
    }

    public static <T> void deserializeToInstance(byte[] bytes, T object) throws IOException {
        if (bytes == null) throw new IOException("bytes are null");
        objectMapper.readerForUpdating(object).readValue(bytes);
    }

    public static <T> T deserialize(String s, com.fasterxml.jackson.core.type.TypeReference<T> typeReference) throws IOException {
        if (s == null) return null;
        return objectMapper.readValue(s, typeReference);
    }

    public static JsonNode deserialize(String jsonString) throws IOException {
        return objectMapper.readTree(jsonString);
    }

    public static <T> T deserializeYaml(byte[] bytes, TypeReference<T> typeReference) throws IOException {
        if (bytes == null) return null;
        return yamlMapper.readValue(bytes, typeReference);
    }
}
