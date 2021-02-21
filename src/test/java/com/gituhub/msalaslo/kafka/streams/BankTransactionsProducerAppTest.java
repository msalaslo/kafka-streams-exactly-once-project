package com.gituhub.msalaslo.kafka.streams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import com.github.msalaslo.kafka.streams.BankTransactionsProducerWithPOJO;
import com.github.msalaslo.kafka.streams.model.CustomerTransaction;

public class BankTransactionsProducerAppTest {
	
	// Name of the topic
	private final static String TOPIC = "bank-transactions";
	
	@Test
    public void newRandomTransactionsTest() {
        ProducerRecord<String, CustomerTransaction> record = BankTransactionsProducerWithPOJO.produceRecord(TOPIC, "test");
        String key = record.key();
        CustomerTransaction customer = record.value();

        // assert statements
        assertEquals(key, "test");
        assertEquals(customer.getName(), "test");
        assertTrue("Amount must be positive.", customer.getAmount() > 0);
        System.out.println("Customer:" + customer);
    }

}
