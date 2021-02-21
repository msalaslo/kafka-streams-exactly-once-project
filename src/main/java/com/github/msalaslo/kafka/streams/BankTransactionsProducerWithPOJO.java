package com.github.msalaslo.kafka.streams;

import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.github.msalaslo.kafka.streams.model.CustomerTransaction;
import com.github.msalaslo.kafka.streams.serdes.serializer.CustomerTransactionSerializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BankTransactionsProducerWithPOJO {

	// Name of the topic
	private final static String TOPIC = "bank-transactions";
	// Kafka brokers
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";

	private static Producer<String, CustomerTransaction> createProducer() {
		Properties config = new Properties();
		config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomerTransactionSerializer.class.getName());
		// Strongest producing guarantee
		config.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		// Retries
		config.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
		config.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
		// Ensure we don't push duplicates
		config.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

		return new KafkaProducer<>(config);
	}

	public static void main(String[] args) {
		Producer<String, CustomerTransaction> producer = createProducer();

		 int i = 0;
		while (true) {
			try {
				log.info("producing batch " + i);
				producer.send(produceRecord(TOPIC, "miguel"));
				Thread.sleep(100);
				producer.send(produceRecord(TOPIC, "angel"));
				Thread.sleep(100);
				producer.send(produceRecord(TOPIC, "jose"));
				Thread.sleep(100);
				i++;
			} catch (InterruptedException e) {
				log.error("Error in threads", e);
				break;
			}
		}
		producer.flush();
		producer.close();
	}

	public static ProducerRecord<String, CustomerTransaction> produceRecord(String topic, String customerId) {
		CustomerTransaction transaction = new CustomerTransaction();
		transaction.setName(customerId);
		transaction.setAmount(getAmount());
		transaction.setTime(Instant.now().toEpochMilli());
		return new ProducerRecord<>(topic, transaction.getName(), transaction);
	}

	private static long getAmount() {
		long leftLimit = 1L;
		long rightLimit = 100L;
		return Long.valueOf(leftLimit + (long) (Math.random() * (rightLimit - leftLimit)));
	}

}
