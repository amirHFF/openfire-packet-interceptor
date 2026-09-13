package io.projectZ.eventListener.mapper;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public final class ObjectMapperFactory {

    private ObjectMapperFactory() {
    }

    public static ObjectMapper create() {

        ObjectMapper mapper = new ObjectMapper();

        // Don't serialize null fields
        mapper.setSerializationInclusion(
                JsonInclude.Include.NON_NULL
        );

        // Better throughput: don't flush after every value
        mapper.disable(
                JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM
        );

        // We don't need pretty JSON
        mapper.disable(
                SerializationFeature.INDENT_OUTPUT
        );

        // Avoid unnecessary timestamp handling
        mapper.disable(
                SerializationFeature.WRITE_DATES_AS_TIMESTAMPS
        );

        // More tolerant consumer-side behavior
        mapper.disable(
                DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
        );

        return mapper;
    }
}