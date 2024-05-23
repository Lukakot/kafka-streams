package com.example.kafkastreams.serdes;

import com.example.kafkastreams.domain.Greeting;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Slf4j
public class GreetingSerializer implements Serializer<Greeting> {


    private final ObjectMapper objectMapper;

    public GreetingSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Greeting data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public byte[] serialize(String s, Greeting greeting) {
        try {
            return objectMapper.writeValueAsBytes(greeting);
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (Exception e){
            log.error("Exception: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }



}
