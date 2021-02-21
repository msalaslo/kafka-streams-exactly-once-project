package com.github.msalaslo.kafka.streams.serdes.deserializer;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BaseDeserializer<T> implements Deserializer<T> {
	
    private Class<T> typeOfT;

    @SuppressWarnings("unchecked")
    private void inicializeTypeOfT() {
        this.typeOfT = (Class<T>)
                ((ParameterizedType)getClass()
                .getGenericSuperclass())
                .getActualTypeArguments()[0];
    }
    
    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    public T deserialize(String topic, byte[] data) {
    	this.inicializeTypeOfT();
        ObjectMapper objectMapper = new ObjectMapper();
        T target = null;
        try {
            target = objectMapper.readValue(data, typeOfT);
        } catch (IOException e) {
            log.error(e.getMessage());
        }

        return target;
    }
	
}
