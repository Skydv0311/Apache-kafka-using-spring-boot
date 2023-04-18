package com.learnkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaEventsConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaEventsConsumerApplication.class, args);
	}

}
