package com.flipkart.message.sidelining.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by saurabh.jha on 16/09/16.
 */
public class JsonUtils {
    private static ObjectMapper objectMapper;

    public static ObjectMapper getObjectMapper()
    {
        return getObjectMapper(false);
    }

    public static ObjectMapper getObjectMapper(boolean createNewInstance)
    {

        if (objectMapper != null && !createNewInstance)
            return objectMapper;

        ObjectMapper objectMapperInstance = new ObjectMapper();
        objectMapperInstance.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // newInstance request should not override global ObjectMapper instance
        if (!createNewInstance)
            objectMapper = objectMapperInstance;

        return objectMapperInstance;
    }
}
