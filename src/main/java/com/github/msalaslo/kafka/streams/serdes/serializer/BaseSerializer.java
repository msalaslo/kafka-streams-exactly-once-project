package com.github.msalaslo.kafka.streams.serdes.serializer;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BaseSerializer<K> implements Serializer<K> {

	/**
	 * Convert {@code data} into a byte array.
	 *
	 * @param topic topic associated with data
	 * @param data  typed data
	 * @return serialized bytes
	 */
	@Override
	public byte[] serialize(String topic, K data) {
		byte[] result = null;
		ObjectMapper objectMapper = new ObjectMapper();

		try {
			result = objectMapper.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			log.error(e.getMessage());
		}

		return result;
	}

}
