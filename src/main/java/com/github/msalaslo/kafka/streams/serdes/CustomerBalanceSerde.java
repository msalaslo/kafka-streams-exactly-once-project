package com.github.msalaslo.kafka.streams.serdes;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.github.msalaslo.kafka.streams.model.CustomerBalance;
import com.github.msalaslo.kafka.streams.serdes.deserializer.CustomerBalanceDeserializer;
import com.github.msalaslo.kafka.streams.serdes.serializer.CustomerBalanceSerializer;

public class CustomerBalanceSerde implements Serde<CustomerBalance> {

	private final Serializer<CustomerBalance> serializer = new CustomerBalanceSerializer();
	private final Deserializer<CustomerBalance> deserializer = new CustomerBalanceDeserializer();

	/**
	 * Configure this class, which will configure the underlying serializer and
	 * deserializer.
	 *
	 * @param configs configs in key/value pairs
	 * @param isKey   whether is for key or value
	 */
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.serializer.configure(configs, isKey);
		this.deserializer.configure(configs, isKey);
	}

	/**
	 * Close this serde class, which will close the underlying serializer and
	 * deserializer.
	 * <p>
	 * This method has to be idempotent because it might be called multiple times.
	 */
	@Override
	public void close() {
		this.serializer.close();
		this.deserializer.close();
	}

	@Override
	public Serializer<CustomerBalance> serializer() {
		return this.serializer;
	}

	@Override
	public Deserializer<CustomerBalance> deserializer() {
		return this.deserializer;
	}

}