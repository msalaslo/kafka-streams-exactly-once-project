package com.github.msalaslo.kafka.streams.model;

public class CustomerBalance {
	
	private int count;
	private Long amount;
	private Long time;

	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}

	public Long getAmount() {
		return amount;
	}
	public void setAmount(Long amount) {
		this.amount = amount;
	}
	
	public Long getTime() {
		return time;
	}
	public void setTime(Long time) {
		this.time = time;
	}


}
