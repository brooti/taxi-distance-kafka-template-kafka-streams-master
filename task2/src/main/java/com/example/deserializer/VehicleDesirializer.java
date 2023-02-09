package com.example.deserializer;

import com.example.model.VehicleSignal;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class VehicleDesirializer implements Deserializer<VehicleSignal> {

    private final ObjectMapper objectMapper;

    public VehicleDesirializer() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public VehicleSignal deserialize(String s, byte[] bytes) {
        try{
            return this.objectMapper.readValue(bytes, VehicleSignal.class);
        } catch (Exception e){
            throw new SerializationException(e);
        }
    }
}
