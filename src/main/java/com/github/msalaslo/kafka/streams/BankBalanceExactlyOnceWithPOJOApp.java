package com.github.msalaslo.kafka.streams;

import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import com.github.msalaslo.kafka.streams.model.CustomerBalance;
import com.github.msalaslo.kafka.streams.model.CustomerTransaction;
import com.github.msalaslo.kafka.streams.serdes.CustomerBalanceSerde;
import com.github.msalaslo.kafka.streams.serdes.CustomerTransactionSerde;

public class BankBalanceExactlyOnceWithPOJOApp {

	private final static String INPUT_TOPIC = "bank-transactions";
	private final static String OUTPUT_TOPIC = "balance-output";

	public static void main(String[] args) {
		System.out.println("Hello world Employees Balance Stream APP");
		Properties config = new Properties();
		config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "employees-balance-app");
		config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomerTransactionSerde.class.getName());

		// Exactly Once property
		config.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

		// Disable cache just to test, no recommended in production
		config.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

		final CustomerBalanceSerde customerBalanceSerde = new CustomerBalanceSerde();
		
		CustomerBalance initialCustomerBalance = new CustomerBalance();
		initialCustomerBalance.setAmount(0L);
		initialCustomerBalance.setCount(0);
		initialCustomerBalance.setTime(Instant.now().toEpochMilli());

		StreamsBuilder builder = new StreamsBuilder();
		// 1 - stream from Kafka
		KStream<String, CustomerTransaction> bankTransactions = builder.stream(INPUT_TOPIC);

		final KTable<String, CustomerBalance> bankBalance = bankTransactions.groupByKey().aggregate(
				() -> initialCustomerBalance, /* initialize to 0 */
				(key, transaction, balance) -> newBalance(transaction, balance),
				Materialized.
				<String, CustomerBalance, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
				.withKeySerde(Serdes.String())
				.withValueSerde(customerBalanceSerde));
				//with(Serdes.String(), customerBalanceSerde));

		// 7 - Write the result to the topic
		bankBalance.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), customerBalanceSerde));

		KafkaStreams streams = new KafkaStreams(builder.build(), config);
		streams.start();

		// Print the topology
		System.out.println(streams.toString());

		// Shutdown hook to correctly close the streams application
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

		// Shutdown hook to correctly close the serde
		Runtime.getRuntime().addShutdownHook(new Thread(customerBalanceSerde::close));
	}

	public static CustomerBalance newBalance(CustomerTransaction transaction, CustomerBalance customerBalance) {
		CustomerBalance newCustomerBalance = new CustomerBalance();
		newCustomerBalance.setCount(customerBalance.getCount() + 1);
		newCustomerBalance.setAmount(customerBalance.getAmount() + transaction.getAmount());

		long balanceTime = customerBalance.getTime();
		long transactionTime = transaction.getTime();
		newCustomerBalance.setTime(Math.max(transactionTime, balanceTime));
		return newCustomerBalance;
	}

}
